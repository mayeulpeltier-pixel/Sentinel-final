#!/usr/bin/env python3
# mailer.py — SENTINEL v3.51 — Envoi email via SMTP (Gmail ou autre)
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   B10          MIME text/html + application/pdf (non octet-stream)
#   F-4          SMTP_USER / GMAIL_USER / SMTP_HOST / SMTP_PORT configurables
#   FIX-OBS1     Logging structuré via logger nommé
#   K-6          3 tentatives SMTP, backoff configurable via ENV
#   NEW-M1       Destinataires multiples (REPORT_EMAIL=a@b.com,c@d.com)
#   NEW-M2       Mode dry-run (SENTINEL_MAILER_DRYRUN=1)
#   NEW-M3       Validation adresses email avant envoi
#   NEW-M4       Intégration SentinelDB — log envoi dans metrics
#   NEW-M5       Support TLS direct (port 465) + STARTTLS (port 587)
#   NEW-M6       Objet email personnalisable via ENV SENTINEL_EMAIL_SUBJECT
#
# Corrections v3.40-POST :
#   POST-M1      BUG-1 : NameError dans _smtp_send() — report_path explicite
#   POST-M2      BUG-2 : send_report() compatible sentinel_main.py v3.44
#   POST-M3      BUG-3 : Regex email corrigée
#   POST-M4      BUG-4 : load_dotenv() avant constantes module-level
#   POST-M5      INC-1 : _resend_pending() en début de send_report()
#   POST-M6      INC-2 : _log_send_status() (stub supprimé)
#   POST-M7      INC-3 : re.search() direct (re importé en tête)
#   POST-M8      INC-4 : html_body="" documenté
#   POST-M9      PERF-1 : Backoff configurable SMTP_RETRY_WAIT_1/2
#   POST-M10     PERF-2 : check_config() pour health_check.py
#   POST-M11     TAILLE : Warning si > SMTP_ATTACH_WARN_MB / blocage si > SMTP_ATTACH_LIMIT_MB
#   POST-M12     LIGHT MODE : Dégradation gracieuse — PDF retiré si trop lourd
#
# MODIFICATIONS v3.51 — Intégration pièces jointes CSV :
#   CSV-M1  send_report() : paramètre csv_attachments: dict | None = None
#           Reçoit le dict retourné par SentinelDB.export_csv() dans sentinel_main.py.
#           Zéro impact sur l'API existante — paramètre optionnel, défaut None.
#           Usage sentinel_main.py :
#               csv_exports = SentinelDB.export_csv()
#               send_report(html_path, csv_attachments=csv_exports)
#   CSV-M2  _attach_csv_files() : attache les Path CSV au message MIME existant.
#           MIME type : text/csv — détection automatique par les clients mail.
#           Content-Disposition : attachment — force le téléchargement.
#           Encode base64 via email.encoders — compatible tous serveurs SMTP.
#           Fallback par fichier : une erreur sur un CSV n'annule pas les autres.
#   CSV-M3  Filtrage cohérent SENTINEL_CSV_MODE (garde supplémentaire) :
#           Le dict est déjà filtré par export_csv() — le mailer applique un
#           second filtre de sécurité sur les clés.
#           mode=standard → clés "_excel" ignorées
#           mode=excel    → clés sans "_excel" ignorées
#           mode=both     → tout attaché (comportement par défaut)
#           Guard : ENV absent ou valeur inconnue → tout attaché (pas de perte silencieuse)
#   CSV-M4  Dégradation light mode étendue — ordre priorité décroissante :
#           1. Retrait CSV d'abord (_strip_csv_attachments()) — impact minimal analyste
#           2. Retrait PDF ensuite (_strip_pdf_attachments()) — HTML conservé
#           3. Blocage total si HTML seul encore > limite
#           _strip_csv_attachments() ajouté — symétrique à _strip_pdf_attachments().
#           _check_message_size() mis à jour avec la nouvelle séquence de dégradation.
#   CSV-M5  Imports MIMEBase / encode_base64 déplacés en tête de fichier —
#           utilisés par _attach_csv_files() et _build_minimal_mime().
# ─────────────────────────────────────────────────────────────────────────────
# Variables d'environnement requises (dans .env) :
#   SMTP_USER      ou  GMAIL_USER         adresse expéditeur
#   SMTP_PASS      ou  SMTP_PASSWORD
#               ou  GMAIL_APP_PASSWORD   mot de passe applicatif Gmail
#   REPORT_EMAIL                          destinataire(s), virgule-séparés
#
# Variables optionnelles :
#   SMTP_HOST                    défaut : smtp.gmail.com
#   SMTP_PORT                    défaut : 587 (STARTTLS) | 465 (TLS direct)
#   SMTP_RETRY_WAIT_1            défaut : 30s
#   SMTP_RETRY_WAIT_2            défaut : 90s
#   SMTP_ATTACH_WARN_MB          défaut : 10
#   SMTP_ATTACH_LIMIT_MB         défaut : 24
#   SENTINEL_MAILER_DRYRUN       1 = log seulement
#   SENTINEL_EMAIL_SUBJECT       préfixe du sujet (défaut : SENTINEL)
#   SENTINEL_CSV_MODE            both | standard | excel (défaut : both)
# ─────────────────────────────────────────────────────────────────────────────
# Compatibilité sentinel_main.py v3.44 :
#   Pipeline normal  : send_report(html_report)
#   Crash handler    : send_report(None, subject="[SENTINEL CRASH] ...")
#   Rapport mensuel  : send_report(html_report)
#   Avec CSV v3.51   : send_report(html_report, csv_attachments=csv_exports)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import datetime
import logging
import os
import re
import shutil
import smtplib
import time
from email.encoders import encode_base64          # CSV-M5 : déplacé en tête
from email.mime.base import MIMEBase              # CSV-M5 : déplacé en tête
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# ── POST-M4 : load_dotenv() AVANT les constantes module-level ────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("sentinel.mailer")

# ── Configuration SMTP (F-4) ──────────────────────────────────────────────────
SMTP_USER = (
    os.environ.get("SMTP_USER")
    or os.environ.get("GMAIL_USER")
    or ""
)
SMTP_PASSWORD = (
    os.environ.get("SMTP_PASS")
    or os.environ.get("SMTP_PASSWORD")
    or os.environ.get("GMAIL_APP_PASSWORD")
    or ""
)
SMTP_TO   = os.environ.get("REPORT_EMAIL", SMTP_USER)
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))

# ── Mode dry-run (NEW-M2) ─────────────────────────────────────────────────────
DRY_RUN = os.environ.get("SENTINEL_MAILER_DRYRUN", "0").strip() == "1"

# ── Sujet personnalisable (NEW-M6) ────────────────────────────────────────────
EMAIL_SUBJECT_PREFIX = os.environ.get("SENTINEL_EMAIL_SUBJECT", "SENTINEL")

# ── Paramètres retry — POST-M9 ────────────────────────────────────────────────
_SMTP_MAX_ATTEMPTS = 3
_SMTP_BACKOFF: list[int] = [
    int(os.environ.get("SMTP_RETRY_WAIT_1", "30")),
    int(os.environ.get("SMTP_RETRY_WAIT_2", "90")),
]

# ── Seuils taille message — POST-M11 ─────────────────────────────────────────
_ATTACH_WARN_MB  = int(os.environ.get("SMTP_ATTACH_WARN_MB",  "10"))
_ATTACH_LIMIT_MB = int(os.environ.get("SMTP_ATTACH_LIMIT_MB", "24"))

# ── Mode CSV — CSV-M3 ─────────────────────────────────────────────────────────
_CSV_MODE = os.environ.get("SENTINEL_CSV_MODE", "both")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION (NEW-M3 + POST-M3)
# ─────────────────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"^[^@s]+@[^@s]+.[^@s]+$")


def _is_valid_email(addr: str) -> bool:
    return bool(_EMAIL_RE.match(addr.strip()))


def _parse_recipients(raw: str) -> list[str]:
    """Parse virgule/point-virgule, filtre les invalides (NEW-M1 + NEW-M3)."""
    candidates = [a.strip() for a in re.split(r"[,;]", raw) if a.strip()]
    valid   = [a for a in candidates if _is_valid_email(a)]
    invalid = [a for a in candidates if not _is_valid_email(a)]
    if invalid:
        log.warning(f"MAILER Adresses ignorées (format invalide) : {invalid}")
    return valid


# ─────────────────────────────────────────────────────────────────────────────
# PIÈCES JOINTES CSV — CSV-M2 / CSV-M3
# ─────────────────────────────────────────────────────────────────────────────

def _attach_csv_files(msg: MIMEMultipart, csv_attachments: dict) -> int:
    """
    [CSV-M2] Attache les fichiers CSV au message MIME existant.

    Chaque entrée du dict est une clé (ex : "metrics", "metrics_excel")
    et une Path vers le fichier généré par SentinelDB.export_csv().

    [CSV-M3] Guard filtrage SENTINEL_CSV_MODE :
        mode=standard → clés "_excel" ignorées (excel_fr non joint)
        mode=excel    → clés sans "_excel" ignorées (standard non joint)
        mode=both     → tout attaché
        valeur inconnue → tout attaché (pas de perte silencieuse)

    Fallback par fichier (CSV-M2) : une erreur sur un CSV n'annule pas
    les autres — log.warning et on continue.

    Retourne le nombre de fichiers effectivement attachés.
    """
    attached = 0

    for key, path in csv_attachments.items():
        # CSV-M3 : garde supplémentaire cohérente avec SENTINEL_CSV_MODE
        if _CSV_MODE == "standard" and key.endswith("_excel"):
            log.debug(f"MAILER CSV {key} ignoré (mode=standard)")
            continue
        if _CSV_MODE == "excel" and not key.endswith("_excel"):
            log.debug(f"MAILER CSV {key} ignoré (mode=excel)")
            continue

        path = Path(path)
        if not path.exists():
            log.warning(f"MAILER CSV '{key}' introuvable : {path} — ignoré")
            continue

        try:
            part = MIMEBase("text", "csv")
            with open(path, "rb") as f:
                part.set_payload(f.read())
            encode_base64(part)
            part.add_header(
                "Content-Disposition", "attachment",
                filename=path.name,
            )
            msg.attach(part)
            size_kb = path.stat().st_size // 1024
            attached += 1
            log.info(f"MAILER CSV joint : {path.name} ({size_kb} Ko) [clé={key}]")
        except Exception as e:
            log.warning(f"MAILER CSV '{key}' attachement échoué : {e}")

    return attached


# ─────────────────────────────────────────────────────────────────────────────
# LIGHT MODE — POST-M12 + CSV-M4
# ─────────────────────────────────────────────────────────────────────────────

def _strip_csv_attachments(msg: MIMEMultipart) -> tuple[MIMEMultipart, int]:
    """
    [CSV-M4] Retire les pièces jointes CSV du message MIME.

    Appelée EN PRIORITÉ sur _strip_pdf_attachments() lors du light mode :
    les CSV sont des données analytiques accessibles autrement (output/),
    le PDF est le livrable principal — on sacrifie les CSV en premier.

    Retourne (msg_allégé, nb_csv_retirés).
    Retourne (msg_original, 0) si aucun CSV présent.
    """
    light_msg = MIMEMultipart("mixed")
    for header in ("From", "To", "Subject", "Date", "Message-ID"):
        if msg[header]:
            light_msg[header] = msg[header]

    removed = 0
    for part in msg.get_payload():
        content_type = part.get_content_type()
        if content_type == "text/csv":
            removed += 1
            log.info(
                f"MAILER Light mode — CSV retiré : "
                f"{part.get_filename() or 'sans nom'}"
            )
        else:
            light_msg.attach(part)

    if removed:
        log.warning(
            f"MAILER Light mode CSV : {removed} fichier(s) CSV retiré(s). "
            f"Les CSV restent disponibles dans output/ pour récupération manuelle."
        )
    return light_msg, removed


def _strip_pdf_attachments(msg: MIMEMultipart) -> MIMEMultipart:
    """
    POST-M12 : Retire les pièces jointes PDF du message MIME existant.

    Construit un nouveau MIMEMultipart en copiant tous les payloads sauf
    les parts application/pdf. Headers (From, To, Subject) préservés.

    Appelée après _strip_csv_attachments() dans la séquence de dégradation
    light mode (CSV-M4).
    """
    light_msg = MIMEMultipart("mixed")
    for header in ("From", "To", "Subject", "Date", "Message-ID"):
        if msg[header]:
            light_msg[header] = msg[header]

    removed = 0
    for part in msg.get_payload():
        content_type = part.get_content_type()
        if content_type == "application/pdf":
            removed += 1
            log.info(
                f"MAILER Light mode — PDF retiré : "
                f"{part.get_filename() or 'sans nom'}"
            )
        else:
            light_msg.attach(part)

    if removed:
        log.warning(
            f"MAILER Light mode PDF : {removed} PDF retiré(s). "
            f"Le rapport HTML reste joint. "
            f"Augmenter SMTP_ATTACH_LIMIT_MB dans .env pour envoyer le PDF complet."
        )
    return light_msg


def _check_message_size(
    msg_bytes:   bytes,
    msg:         MIMEMultipart,
    report_path: str | None = None,
) -> tuple[bool, MIMEMultipart]:
    """
    POST-M11 + POST-M12 + CSV-M4 : Vérifie la taille du message et applique
    le light mode si nécessaire.

    Retourne (peut_envoyer, msg_à_envoyer).

    Séquence de dégradation v3.51 (CSV-M4) :
      1. Taille ≤ SMTP_ATTACH_WARN_MB    → envoi normal
      2. Taille ≤ SMTP_ATTACH_LIMIT_MB   → warning non bloquant, envoi normal
      3. Taille > SMTP_ATTACH_LIMIT_MB   →
         3a. Retrait CSV d'abord          → si ça passe : envoi sans CSV
         3b. Retrait CSV + PDF            → si ça passe : envoi HTML seul
         3c. Encore trop lourd            → blocage total + archivage output/pending/

    Justification ordre CSV avant PDF (CSV-M4) :
        Les CSV sont disponibles dans output/ — perte d'accès tolérable.
        Le PDF est le livrable signé du cycle — priorité de conservation maximale.
    """
    size_mb = len(msg_bytes) / (1024 * 1024)

    if size_mb <= _ATTACH_WARN_MB:
        log.info(f"MAILER Taille message : {size_mb:.1f} Mo ✓")
        return True, msg

    if size_mb <= _ATTACH_LIMIT_MB:
        log.warning(
            f"MAILER Message lourd : {size_mb:.1f} Mo > seuil {_ATTACH_WARN_MB} Mo. "
            f"Envoi tenté — certains serveurs SMTP d'entreprise rejettent au-delà "
            f"de {_ATTACH_WARN_MB} Mo. Ajuster SMTP_ATTACH_WARN_MB dans .env."
        )
        return True, msg

    # ── Étape 3a : retrait CSV d'abord (CSV-M4) ──────────────────────────────
    log.warning(
        f"MAILER Message trop lourd : {size_mb:.1f} Mo > limite {_ATTACH_LIMIT_MB} Mo. "
        f"Tentative light mode — retrait CSV d'abord…"
    )
    msg_no_csv, nb_csv_removed = _strip_csv_attachments(msg)
    if nb_csv_removed:
        no_csv_mb = len(msg_no_csv.as_bytes()) / (1024 * 1024)
        if no_csv_mb <= _ATTACH_LIMIT_MB:
            log.warning(
                f"MAILER Light mode CSV OK : {no_csv_mb:.1f} Mo "
                f"({nb_csv_removed} CSV retiré(s), PDF+HTML conservés). "
                f"CSV disponibles dans output/ pour récupération manuelle."
            )
            return True, msg_no_csv
    else:
        msg_no_csv = msg  # pas de CSV à retirer — continuer avec msg original

    # ── Étape 3b : retrait PDF (POST-M12) ─────────────────────────────────────
    log.warning(
        f"MAILER Light mode CSV insuffisant — tentative retrait PDF…"
    )
    light_msg   = _strip_pdf_attachments(msg_no_csv)
    light_bytes = light_msg.as_bytes()
    light_mb    = len(light_bytes) / (1024 * 1024)

    if light_mb <= _ATTACH_LIMIT_MB:
        log.warning(
            f"MAILER Light mode PDF OK : {light_mb:.1f} Mo "
            f"(CSV + PDF retirés, HTML conservé). "
            f"Pour envoyer le PDF : réduire DPI charts (charts.py) "
            f"ou augmenter SMTP_ATTACH_LIMIT_MB={int(size_mb) + 1} dans .env."
        )
        return True, light_msg

    # ── Étape 3c : blocage total ──────────────────────────────────────────────
    log.error(
        f"MAILER Blocage total : message trop lourd même en mode light "
        f"({light_mb:.1f} Mo > {_ATTACH_LIMIT_MB} Mo). "
        f"Rapport archivé dans output/pending/ pour envoi manuel."
    )
    if report_path and Path(report_path).exists():
        try:
            pending_dir = Path("output/pending")
            pending_dir.mkdir(parents=True, exist_ok=True)
            dest = pending_dir / Path(report_path).name
            shutil.copy2(report_path, dest)
            log.warning(f"MAILER rapport archivé → {dest}")
        except Exception as arch_err:
            log.warning(f"MAILER archivage impossible : {arch_err}")

    return False, msg


# ─────────────────────────────────────────────────────────────────────────────
# ENVOI SMTP — POST-M1
# ─────────────────────────────────────────────────────────────────────────────

def _smtp_send(
    msg_string:  str,
    from_addr:   str,
    to_list:     list[str],
    report_path: str | None = None,
) -> bool:
    """
    Envoie le message MIME via SMTP avec 3 tentatives et backoff.
    Supporte STARTTLS (port 587) et TLS direct (port 465).
    POST-M1 : report_path paramètre explicite (plus de NameError).
    K-6 / R3A3-NEW-5 / NEW-M5
    """
    for attempt in range(_SMTP_MAX_ATTEMPTS):
        try:
            if SMTP_PORT == 465:
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.sendmail(from_addr, to_list, msg_string)
            else:
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                    server.starttls()
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.sendmail(from_addr, to_list, msg_string)

            log.info(
                f"MAILER Email envoyé → {to_list} "
                f"(tentative {attempt + 1}/{_SMTP_MAX_ATTEMPTS})"
            )
            return True

        except (smtplib.SMTPException, OSError) as e:
            log.warning(
                f"MAILER Tentative {attempt + 1}/{_SMTP_MAX_ATTEMPTS} échouée : {e}"
            )
            if attempt < _SMTP_MAX_ATTEMPTS - 1:
                wait = _SMTP_BACKOFF[attempt]
                log.info(f"MAILER Retry dans {wait}s…")
                time.sleep(wait)

    log.error(f"MAILER Échec définitif après {_SMTP_MAX_ATTEMPTS} tentatives")

    if report_path and Path(report_path).exists():
        try:
            pending_dir = Path("output/pending")
            pending_dir.mkdir(parents=True, exist_ok=True)
            dest = pending_dir / Path(report_path).name
            shutil.copy2(report_path, dest)
            log.warning(
                f"MAILER rapport archivé → {dest} "
                "(sera renvoyé au prochain succès SMTP via _resend_pending)"
            )
        except Exception as arch_err:
            log.warning(f"MAILER archivage impossible : {arch_err}")

    return False


# ─────────────────────────────────────────────────────────────────────────────
# LOG ENVOI (POST-M6)
# ─────────────────────────────────────────────────────────────────────────────

def _log_send_status(date_str: str, success: bool) -> None:
    """POST-M6 : log structuré du statut d'envoi (stub trompeur supprimé)."""
    status = "OK" if success else "ECHEC"
    log.info(f"MAILER Statut envoi [{date_str}] : {status}")


# ─────────────────────────────────────────────────────────────────────────────
# RENVOI DES RAPPORTS EN ATTENTE — POST-M5
# ─────────────────────────────────────────────────────────────────────────────

def _resend_pending() -> None:
    """
    Renvoie les rapports archivés dans output/pending/.
    POST-M5 : appelée en début de send_report().
    POST-M7 : re.search() direct (re importé en tête de fichier).
    """
    pending_dir = Path("output/pending")
    if not pending_dir.exists():
        return

    for html_file in sorted(pending_dir.glob("*.html")):
        try:
            m = re.search(r"(d{4}-d{2}-d{2})", html_file.name)
            date_obj = (
                datetime.date.fromisoformat(m.group(1))
                if m else datetime.date.today()
            )
            log.info(f"MAILER Tentative renvoi rapport en attente : {html_file.name}")
            # Pas de csv_attachments pour les rapports en attente (déjà archivés seuls)
            ok = send_report(str(html_file), date_obj=date_obj)
            if ok:
                html_file.unlink()
                log.info(f"MAILER rapport renvoyé et supprimé : {html_file.name}")
        except Exception as e:
            log.warning(f"MAILER resend pending {html_file.name} : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK MIME MINIMAL (si report_builder absent)
# ─────────────────────────────────────────────────────────────────────────────

def _build_minimal_mime(
    date_str:    str,
    report_path: str,
    to_list:     list[str],
    subject:     str | None = None,
) -> MIMEMultipart:
    """
    Construction MIME minimale de secours.
    B10-FIX : MIME text/html et application/pdf corrects.
    CSV-M5 : MIMEBase / encode_base64 désormais importés en tête de fichier.
    Utilisé uniquement si report_builder.build_email_message() est absent.
    """
    from email.mime.text import MIMEText

    msg = MIMEMultipart("mixed")
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject or f"SENTINEL — Rapport robotique défense du {date_str}"

    body = (
        f"Bonjour,

"
        f"Veuillez trouver ci-joint le rapport SENTINEL du {date_str}.
"
        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la défense.
"
    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    base = Path(report_path)
    for suffix, m_type, m_sub in [
        (".html", "text",        "html"),
        (".pdf",  "application", "pdf"),
    ]:
        att = base.with_suffix(suffix)
        if not att.exists() and suffix == ".html":
            att = Path(report_path)
        if att.exists():
            try:
                part = MIMEBase(m_type, m_sub)
                with open(att, "rb") as f:
                    part.set_payload(f.read())
                encode_base64(part)
                part.add_header(
                    "Content-Disposition", "attachment",
                    filename=att.name,
                )
                msg.attach(part)
            except OSError as e:
                log.warning(f"MAILER Pièce jointe {att.name} ignorée : {e}")

    return msg


# ─────────────────────────────────────────────────────────────────────────────
# FONCTION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

def send_report(
    report_path:      str | None,
    date_obj:         datetime.date | None = None,
    extra_recipients: list[str] | None = None,
    subject:          str | None = None,
    csv_attachments:  dict | None = None,
) -> bool:
    """
    Envoie le rapport HTML (+ PDF si disponible, + CSV si fournis) par email.

    Compatibilité sentinel_main.py v3.44 (POST-M2) — API inchangée v3.51 :
        Pipeline normal  : send_report(html_report)
        Crash handler    : send_report(None, subject="[SENTINEL CRASH] ...")
        Rapport mensuel  : send_report(html_report)
        Avec CSV v3.51   : send_report(html_report, csv_attachments=csv_exports)

    Paramètres
    ----------
    report_path       : str | None  Chemin du fichier HTML. None = texte seul.
    date_obj          : date        Date du rapport. Défaut : aujourd'hui.
    extra_recipients  : list        Destinataires supplémentaires (optionnel).
    subject           : str         Sujet complet (crash handler).
    csv_attachments   : dict        dict[str, Path] retourné par
                                    SentinelDB.export_csv(). Optionnel.
                                    Filtré selon SENTINEL_CSV_MODE (CSV-M3).

    Logique taille v3.51 (POST-M11 + POST-M12 + CSV-M4) :
        ≤ SMTP_ATTACH_WARN_MB  → envoi normal
        ≤ SMTP_ATTACH_LIMIT_MB → warning non bloquant + envoi normal
        > SMTP_ATTACH_LIMIT_MB → 1. retrait CSV (léger, récupérable dans output/)
                                  2. retrait PDF (HTML conservé)
                                  3. blocage + archivage output/pending/
    """
    # POST-M5 : renvoyer les rapports archivés avant le nouvel envoi
    _resend_pending()

    if date_obj is None:
        date_obj = datetime.date.today()

    # ── Cas crash handler : report_path=None (POST-M2) ───────────────────────
    if report_path is None:
        if subject:
            log.warning(f"MAILER Crash handler — {subject}")
        else:
            log.warning("MAILER send_report(None) appelé sans sujet")
        if not SMTP_USER or not SMTP_PASSWORD:
            log.error("MAILER SMTP non configuré — alerte crash non envoyée")
            return False
        recipients = _parse_recipients(SMTP_TO)
        if not recipients:
            log.error("MAILER Aucun destinataire — alerte crash non envoyée")
            return False
        from email.mime.text import MIMEText as _MIMEText
        crash_msg = MIMEMultipart()
        crash_msg["From"]    = SMTP_USER
        crash_msg["To"]      = ", ".join(recipients)
        crash_msg["Subject"] = subject or f"{EMAIL_SUBJECT_PREFIX} — Erreur pipeline"
        crash_msg.attach(_MIMEText(subject or "Erreur pipeline SENTINEL.", "plain", "utf-8"))
        if DRY_RUN:
            log.info(
                f"MAILER [DRY-RUN] Alerte crash non envoyée — "
                f"sujet={crash_msg['Subject']!r}"
            )
            return True
        return _smtp_send(crash_msg.as_string(), SMTP_USER, recipients, report_path=None)

    # ── Pré-vérifications ─────────────────────────────────────────────────────
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("MAILER SMTP_USER ou SMTP_PASS manquants — configurer .env")
        return False

    if not _is_valid_email(SMTP_USER):
        log.error(f"MAILER Adresse expéditeur invalide : {SMTP_USER!r}")
        return False

    # Destinataires (NEW-M1)
    to_list = _parse_recipients(SMTP_TO)
    if extra_recipients:
        for addr in extra_recipients:
            if _is_valid_email(addr) and addr not in to_list:
                to_list.append(addr)

    if not to_list:
        log.error("MAILER Aucun destinataire valide — email annulé")
        return False

    # ── Construction MIME via report_builder (B10-FIX / POST-M8) ─────────────
    date_str = date_obj.strftime("%d/%m/%Y")
    msg: MIMEMultipart | None = None
    try:
        from report_builder import build_email_message
        msg = build_email_message(
            date_str   = date_str,
            html_body  = "",
            from_addr  = SMTP_USER,
            to_addr    = ", ".join(to_list),
            output_dir = Path(report_path).parent,
        )
    except ImportError:
        log.warning("MAILER report_builder absent — construction MIME minimale (fallback)")
    except Exception as e:
        log.warning(f"MAILER build_email_message() échoué : {e} — fallback MIME minimal")

    if msg is None:
        msg = _build_minimal_mime(date_str, report_path, to_list, subject=subject)

    # ── Sujet & destinataires finaux ──────────────────────────────────────────
    final_subject = subject or (
        f"{EMAIL_SUBJECT_PREFIX} — Rapport robotique défense du {date_str}"
    )
    if "Subject" in msg:
        del msg["Subject"]
    msg["Subject"] = final_subject

    if "To" in msg:
        del msg["To"]
    msg["To"] = ", ".join(to_list)

    # ── CSV-M1/M2 : attacher les fichiers CSV si fournis ──────────────────────
    if csv_attachments:
        nb_csv = _attach_csv_files(msg, csv_attachments)
        if nb_csv:
            log.info(f"MAILER {nb_csv} fichier(s) CSV joint(s) au message")
        elif csv_attachments:
            log.warning(
                f"MAILER csv_attachments fourni ({len(csv_attachments)} entrée(s)) "
                f"mais aucun fichier attaché (mode={_CSV_MODE} ? fichiers absents ?)"
            )

    # ── Mode dry-run (NEW-M2) ─────────────────────────────────────────────────
    if DRY_RUN:
        nb_parts = len(msg.get_payload()) if isinstance(msg.get_payload(), list) else 1
        log.info(
            f"MAILER [DRY-RUN] Email non envoyé — destinataires={to_list} "
            f"sujet={msg['Subject']!r} parts={nb_parts}"
        )
        return True

    # ── POST-M11 + POST-M12 + CSV-M4 : vérification taille + light mode ──────
    msg_bytes = msg.as_bytes()
    can_send, msg = _check_message_size(msg_bytes, msg, report_path=report_path)
    if not can_send:
        return False

    # ── Envoi SMTP (POST-M1) ──────────────────────────────────────────────────
    success = _smtp_send(msg.as_string(), SMTP_USER, to_list, report_path=report_path)
    _log_send_status(str(date_obj), success)
    return success


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC — POST-M10
# ─────────────────────────────────────────────────────────────────────────────

def check_config() -> dict:
    """
    Vérifie la configuration SMTP et retourne un dictionnaire de statut.
    POST-M10 : health_check.py doit appeler cette fonction (pas smtplib direct).
    """
    recipients = _parse_recipients(SMTP_TO)
    return {
        "smtp_user_set":    bool(SMTP_USER),
        "smtp_pass_set":    bool(SMTP_PASSWORD),
        "smtp_host":        SMTP_HOST,
        "smtp_port":        SMTP_PORT,
        "smtp_tls":         "TLS direct" if SMTP_PORT == 465 else "STARTTLS",
        "recipients":       recipients,
        "recipients_ok":    len(recipients) > 0,
        "dry_run":          DRY_RUN,
        "subject_prefix":   EMAIL_SUBJECT_PREFIX,
        "retry_wait":       _SMTP_BACKOFF,
        "attach_warn_mb":   _ATTACH_WARN_MB,
        "attach_limit_mb":  _ATTACH_LIMIT_MB,
        "csv_mode":         _CSV_MODE,
        "ready":            bool(SMTP_USER and SMTP_PASSWORD and recipients),
    }


def send_test_email() -> bool:
    """Envoie un email de test pour valider la configuration SMTP."""
    from email.mime.text import MIMEText

    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("MAILER Config incomplète — test annulé")
        return False

    recipients = _parse_recipients(SMTP_TO)
    if not recipients:
        log.error("MAILER Aucun destinataire valide — test annulé")
        return False

    msg = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = f"{EMAIL_SUBJECT_PREFIX} — Test configuration SMTP"
    msg.attach(MIMEText(
        "Ceci est un email de test SENTINEL v3.51.
"
        "Si vous recevez ce message, la configuration SMTP est correcte.
",
        "plain", "utf-8",
    ))

    success = _smtp_send(msg.as_string(), SMTP_USER, recipients, report_path=None)
    if success:
        log.info("MAILER Email de test envoyé avec succès ✓")
    else:
        log.error("MAILER Échec de l'email de test ✗")
    return success


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    if "--check" in sys.argv:
        cfg = check_config()
        print("
── Configuration SMTP SENTINEL v3.51 ────────────────────")
        for k, v in cfg.items():
            status = "✓" if v else "✗" if v is False else " "
            print(f"  {status} {k:<22} {v}")
        print("─────────────────────────────────────────────────────────
")
        sys.exit(0 if cfg["ready"] else 1)

    if "--test" in sys.argv:
        ok = send_test_email()
        sys.exit(0 if ok else 1)

    print("Usage :")
    print("  python mailer.py --check    Vérifie la configuration SMTP")
    print("  python mailer.py --test     Envoie un email de test")
