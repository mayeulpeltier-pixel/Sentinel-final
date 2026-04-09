#!/usr/bin/env python3
# mailer.py — SENTINEL v3.40 — Envoi email via SMTP (Gmail ou autre)
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   B10          MIME text/html + application/pdf (non octet-stream)
#                → délégué à report_builder.build_email_message()
#   F-4          SMTP_USER / GMAIL_USER / SMTP_HOST / SMTP_PORT tous
#                configurables via .env
#   FIX-OBS1     Logging structuré via logger nommé, plus de print()
#   K-6 / R3A3-NEW-5  3 tentatives SMTP, backoff configurable via ENV
#   NEW-M1       Destinataires multiples (REPORT_EMAIL=a@b.com,c@d.com)
#   NEW-M2       Mode dry-run (SENTINEL_MAILER_DRYRUN=1) pour tests CI
#   NEW-M3       Validation adresses email avant envoi
#   NEW-M4       Intégration SentinelDB — log de l'envoi dans metrics
#   NEW-M5       Support TLS direct (port 465) en plus STARTTLS (port 587)
#   NEW-M6       Objet email personnalisable via ENV SENTINEL_EMAIL_SUBJECT
#
# Corrections v3.40-POST (post-audit) :
#   POST-M1      BUG-1 : NameError dans _smtp_send() — report_path passé
#                en paramètre explicite au lieu d'être référencé hors scope
#   POST-M2      BUG-2 : Signature send_report() rendue compatible avec
#                sentinel_main.py v3.44 — date_obj optionnel, kwarg subject
#                ajouté pour le crash handler (send_report(None, subject=...))
#   POST-M3      BUG-3 : Regex email corrigée — s échappé, . échappé
#                r"^[^@s]+@[^@s]+.[^@s]+$" (était r"^[^@s]+@[^@s]+.[^@s]+$")
#   POST-M4      BUG-4 : load_dotenv() ajouté AVANT les constantes module-level
#                — évite les variables vides si import précoce
#   POST-M5      INC-1 : _resend_pending() appelée en début de send_report()
#   POST-M6      INC-2 : _log_send_to_db() → _log_send_status() (stub supprimé)
#   POST-M7      INC-3 : __import__("re") remplacé par re (déjà importé)
#   POST-M8      INC-4 : html_body="" documenté — dépendance explicite sur
#                report_builder.build_email_message()
#   POST-M9      PERF-1 : Backoff configurable via SMTP_RETRY_WAIT_1/2
#   POST-M10     PERF-2 : check_config() point d'entrée pour health_check.py
#   POST-M11     TAILLE : Vérification taille avant envoi SMTP.
#                Warning si > SMTP_ATTACH_WARN_MB (défaut 10 Mo).
#                Blocage si > SMTP_ATTACH_LIMIT_MB (défaut 24 Mo).
#   POST-M12     LIGHT MODE : Dégradation gracieuse si message trop lourd.
#                Si taille > SMTP_ATTACH_LIMIT_MB, _check_message_size() tente
#                de retirer les pièces jointes PDF du message MIME existant.
#                Si le HTML seul passe sous la limite → envoi en mode light
#                avec log.warning. Si même le HTML seul est trop lourd →
#                blocage total + archivage dans output/pending/.
#                Implémenté entièrement dans mailer.py, sans relancer
#                report_builder (qui génère les fichiers, pas les envoie).
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
#   SMTP_RETRY_WAIT_1            défaut : 30s  (attente entre tentative 1→2)
#   SMTP_RETRY_WAIT_2            défaut : 90s  (attente entre tentative 2→3)
#   SMTP_ATTACH_WARN_MB          défaut : 10   (warning si message > N Mo)
#   SMTP_ATTACH_LIMIT_MB         défaut : 24   (blocage / light mode si > N Mo)
#   SENTINEL_MAILER_DRYRUN       1 = log seulement, pas d'envoi réel
#   SENTINEL_EMAIL_SUBJECT       préfixe du sujet (défaut : SENTINEL)
# ─────────────────────────────────────────────────────────────────────────────
# Compatibilité sentinel_main.py v3.44 :
#   Pipeline normal  : send_report(html_report)
#   Crash handler    : send_report(None, subject="[SENTINEL CRASH] ...")
#   Rapport mensuel  : send_report(html_report)
# ─────────────────────────────────────────────────────────────────────────────
# Intégration health_check.py (POST-M10) :
#   Appeler mailer.check_config() — ne pas recréer de connexion smtplib directe.
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import datetime
import logging
import os
import re
import shutil
import smtplib
import time
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

# ── Paramètres retry — POST-M9 (K-6 / R3A3-NEW-5) ───────────────────────────
_SMTP_MAX_ATTEMPTS = 3
_SMTP_BACKOFF: list[int] = [
    int(os.environ.get("SMTP_RETRY_WAIT_1", "30")),
    int(os.environ.get("SMTP_RETRY_WAIT_2", "90")),
]

# ── Seuils taille message — POST-M11 ─────────────────────────────────────────
_ATTACH_WARN_MB  = int(os.environ.get("SMTP_ATTACH_WARN_MB",  "10"))
_ATTACH_LIMIT_MB = int(os.environ.get("SMTP_ATTACH_LIMIT_MB", "24"))


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
# LIGHT MODE — POST-M12
# ─────────────────────────────────────────────────────────────────────────────

def _strip_pdf_attachments(msg: MIMEMultipart) -> MIMEMultipart:
    """
    POST-M12 : Retire les pièces jointes PDF du message MIME existant.

    Construit un nouveau MIMEMultipart en copiant tous les payloads sauf
    les parts de type application/pdf. Les headers du message original
    (From, To, Subject) sont préservés.

    Ne touche pas report_builder — la dégradation est entièrement gérée
    côté mailer.py sur le message déjà construit.
    """
    light_msg = MIMEMultipart("mixed")

    # Copier les headers principaux
    for header in ("From", "To", "Subject", "Date", "Message-ID"):
        if msg[header]:
            light_msg[header] = msg[header]

    removed = 0
    for part in msg.get_payload():
        content_type = part.get_content_type()
        if content_type == "application/pdf":
            removed += 1
            log.info(
                f"MAILER Light mode — pièce jointe PDF retirée : "
                f"{part.get_filename() or 'sans nom'}"
            )
        else:
            light_msg.attach(part)

    if removed:
        log.warning(
            f"MAILER Light mode — {removed} PDF retiré(s) du message. "
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
    POST-M11 + POST-M12 : Vérifie la taille du message et applique le
    mode light si nécessaire.

    Retourne (peut_envoyer, msg_à_envoyer).

    Logique de dégradation (POST-M12) :
      1. Taille OK (≤ SMTP_ATTACH_WARN_MB)    → envoi normal
      2. Taille warning (≤ SMTP_ATTACH_LIMIT_MB) → warning non bloquant, envoi normal
      3. Taille > SMTP_ATTACH_LIMIT_MB         → tenter light mode (retirer PDF)
         3a. Light mode passe                  → warning + envoi sans PDF
         3b. Light mode trop lourd aussi       → blocage total + archivage

    Contexte : rapport SENTINEL avec PDF WeasyPrint + charts PNG Plotly peut
    facilement dépasser 10 Mo. Sans cette vérification, le serveur SMTP
    rejette silencieusement après la phase DATA (SMTPDataError sans contexte).
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

    # Taille > SMTP_ATTACH_LIMIT_MB → tenter light mode (POST-M12)
    log.warning(
        f"MAILER Message trop lourd : {size_mb:.1f} Mo > limite {_ATTACH_LIMIT_MB} Mo. "
        f"Tentative mode light (retrait PDF)…"
    )
    light_msg  = _strip_pdf_attachments(msg)
    light_bytes = light_msg.as_bytes()
    light_mb    = len(light_bytes) / (1024 * 1024)

    if light_mb <= _ATTACH_LIMIT_MB:
        log.warning(
            f"MAILER Mode light OK : {light_mb:.1f} Mo (PDF retiré, HTML conservé). "
            f"Pour envoyer le PDF, réduire son poids : "
            f"(1) DPI charts 300 → 150 dans charts.py, "
            f"(2) Augmenter SMTP_ATTACH_LIMIT_MB={int(size_mb) + 1} dans .env."
        )
        return True, light_msg

    # Même sans PDF c'est trop lourd → blocage total
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
    C2-FIX : renvoie les rapports archivés dans output/pending/.
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
    Utilisé uniquement si report_builder.build_email_message() est absent.
    """
    from email.encoders import encode_base64
    from email.mime.base import MIMEBase
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
                with open(att, "rb") as f:
                    part = MIMEBase(m_type, m_sub)
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
) -> bool:
    """
    Envoie le rapport HTML (+ PDF si disponible) par email.

    Compatibilité sentinel_main.py v3.44 (POST-M2) :
        Pipeline normal  : send_report(html_report)
        Crash handler    : send_report(None, subject="[SENTINEL CRASH] ...")
        Rapport mensuel  : send_report(html_report)

    Paramètres
    ----------
    report_path       : str | None  Chemin du fichier HTML. None = texte seul.
    date_obj          : date        Date du rapport. Défaut : aujourd'hui.
    extra_recipients  : list        Destinataires supplémentaires (optionnel).
    subject           : str         Sujet complet (crash handler).

    Logique taille (POST-M11 + POST-M12) :
        ≤ SMTP_ATTACH_WARN_MB  → envoi normal
        ≤ SMTP_ATTACH_LIMIT_MB → warning non bloquant + envoi normal
        > SMTP_ATTACH_LIMIT_MB → light mode : PDF retiré, HTML conservé
        > limite même sans PDF → blocage + archivage dans output/pending/
    """
    # POST-M5 : renvoyer les rapports archivés avant le nouvel envoi
    _resend_pending()

    # date_obj optionnel (POST-M2)
    if date_obj is None:
        date_obj = datetime.date.today()

    # Cas crash handler : report_path=None (POST-M2)
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
        # Message texte seul → vérif taille non nécessaire
        return _smtp_send(crash_msg.as_string(), SMTP_USER, recipients, report_path=None)

    # Pré-vérifications
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

    # Construire le message MIME via report_builder (B10-FIX / POST-M8)
    date_str = date_obj.strftime("%d/%m/%Y")
    msg: MIMEMultipart | None = None
    try:
        from report_builder import build_email_message
        # POST-M8 : html_body="" intentionnel — build_email_message() lit
        # le fichier HTML depuis output_dir de manière autonome.
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

    # Sujet final
    final_subject = subject or (
        f"{EMAIL_SUBJECT_PREFIX} — Rapport robotique défense du {date_str}"
    )
    if "Subject" in msg:
        del msg["Subject"]
    msg["Subject"] = final_subject

    if "To" in msg:
        del msg["To"]
    msg["To"] = ", ".join(to_list)

    # Mode dry-run (NEW-M2)
    if DRY_RUN:
        log.info(
            f"MAILER [DRY-RUN] Email non envoyé — destinataires={to_list} "
            f"sujet={msg['Subject']!r}"
        )
        return True

    # ── POST-M11 + POST-M12 : vérification taille + light mode ───────────────
    # _check_message_size retourne (peut_envoyer, msg_final).
    # msg_final peut être allégé (PDF retiré) si taille > SMTP_ATTACH_LIMIT_MB.
    msg_bytes = msg.as_bytes()
    can_send, msg = _check_message_size(msg_bytes, msg, report_path=report_path)
    if not can_send:
        return False

    # Envoi SMTP (POST-M1 : report_path passé explicitement)
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
        "Ceci est un email de test SENTINEL v3.40.
"
        "Si vous recevez ce message, la configuration SMTP est correcte.
",
        "plain", "utf-8",
    ))

    # Message texte seul → vérif taille non nécessaire
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
── Configuration SMTP SENTINEL v3.40 ────────────────────")
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
