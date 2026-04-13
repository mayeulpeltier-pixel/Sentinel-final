#!/usr/bin/env python3
# mailer.py — SENTINEL v3.53 — Envoi email via SMTP (Gmail ou autre)
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
#   CSV-M2  _attach_csv_files() : attache les Path CSV au message MIME
#   CSV-M3  Filtrage cohérent SENTINEL_CSV_MODE (garde supplémentaire)
#   CSV-M4  Dégradation light mode étendue — CSV avant PDF
#   CSV-M5  Imports MIMEBase / encode_base64 déplacés en tête de fichier
#
# MODIFICATIONS v3.52 — Bandeau avertissement light mode (RB-43-WARN1) :
#   WARN-1  _strip_csv_attachments() : attache build_light_mode_warning_part(["CSV"])
#   WARN-2  _strip_pdf_attachments() : attache build_light_mode_warning_part(["PDF"])
#   WARN-3  Import lazy via try/except dans chaque strip function
#   WARN-4  DOC regex _EMAIL_RE corrigée (fix pas réellement appliqué → MAIL-R1)
#   WARN-5  DOC regex _resend_pending corrigée (fix pas réellement appliqué → MAIL-R2)
#
# CORRECTIONS v3.53 — Post-audit complet :
#   [MAIL-R1]  _EMAIL_RE : regex VRAIMENT corrigée via chr(92) — WARN-4 documentait
#              la correction mais l'appliquait sur l'ancienne valeur cassée.
#              r"^[^@s]+@[^@s]+.[^@s]+$" → [^@\s] et \. via _BS = chr(92).
#              Même protection anti-copier-coller que telegram_scraper.py (TG-R1).
#   [MAIL-R2]  _resend_pending regex VRAIMENT corrigée via chr(92) — WARN-5 idem.
#              r"(d{4}-d{2}-d{2})" → \d{4}-\d{2}-\d{2} via _BS.
#              Sans ce fix, les rapports en attente étaient renvoyés avec
#              datetime.date.today() quel que soit le nom du fichier archivé.
#   [MAIL-R3]  Anti-récursion flag _resending — send_report() appelle
#              _resend_pending() qui rappelle send_report() : boucle infinie
#              si le renvoi échoue. Flag module-level coupe la récursion.
#   [MAIL-R4]  MIMEText importé EN TÊTE (plus d'imports locaux éparpillés dans
#              _build_minimal_mime, send_report crash handler, send_test_email).
#              Cohérence avec MIMEBase / encode_base64 déjà en tête (CSV-M5).
#   [MAIL-R5]  SMTP_TO / destinataires lus DYNAMIQUEMENT via _get_recipients()
#              au lieu d'être figés à l'import. L'ancienne évaluation module-level
#              SMTP_TO = os.environ.get("REPORT_EMAIL", SMTP_USER) était vide si
#              .env non chargé avant l'import et impossible à surcharger en tests.
#              SMTP_TO conservé comme alias lecture-seule pour compatibilité externe.
#   [MAIL-R6]  VERSION constante utilisée dans send_test_email() et les logs
#              (plus de "v3.52" hardcodé dans le corps du message).
#   [MAIL-R7]  _check_message_size refactorisée : msg_bytes déjà calculé dans
#              send_report() est passé en paramètre → pas de double as_bytes().
#              Les appels as_bytes() internes (msg_no_csv, light_msg) sont des
#              nouvelles structures post-strip — inévitables et commentés.
#   [MAIL-R8]  _resend_pending() : limitation CSV documentée explicitement.
#              Les rapports renvoyés n'incluent jamais les CSV (non archivés
#              dans output/pending/). Commentaire de conception ajouté.
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
# Compatibilité sentinel_main.py v3.54 — API inchangée v3.53 :
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
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
# [MAIL-R4] MIMEText importé en tête — plus d'imports locaux éparpillés
from email.mime.text import MIMEText
from pathlib import Path

# ── POST-M4 : load_dotenv() AVANT les constantes module-level ────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("sentinel.mailer")

# ─────────────────────────────────────────────────────────────────────────────
# VERSION
# [MAIL-R6] Constante centralisée — plus de "v3.5x" hardcodé dans le code
# ─────────────────────────────────────────────────────────────────────────────
VERSION = "3.53"

# ─────────────────────────────────────────────────────────────────────────────
# [MAIL-R1] REGEX BACKSLASH-SAFE — chr(92) = backslash
# Même protection anti-copier-coller que telegram_scraper.py (TG-R1).
# Certains éditeurs / interfaces chat corrompent les séquences d'échappement
# dans les raw strings lors d'un copier-coller.
#   _BS      = \
#   _NOT_AT  = [^@\s]  — ni @, ni espace, ni whitespace
#   _DOT_L   = \.      — point littéral (pas "tout caractère")
#   _DIGIT   = \d      — chiffre (pour les dates)
# ─────────────────────────────────────────────────────────────────────────────
_BS     = chr(92)
_NOT_AT = "[^@" + _BS + "s]"   # [^@\s]
_DOT_L  = _BS + "."             # \.
_DIGIT  = _BS + "d"             # \d

# [MAIL-R1] Regex email correcte : user@domain.tld
# Ancienne valeur cassée : r"^[^@s]+@[^@s]+.[^@s]+$"
#   [^@s]  → ne filtrait que @ et la lettre s, pas les espaces
#   .      → matchait n'importe quel caractère, pas seulement le point
#   Résultat : user@domain.com retournait False → aucun email jamais envoyé
# Nouvelle valeur : [^@\s]+@[^@\s]+\.[^@\s]+
_EMAIL_RE = re.compile(
    "^" + _NOT_AT + "+" + "@" + _NOT_AT + "+" + _DOT_L + _NOT_AT + "+$"
)

# [MAIL-R2] Regex date pour _resend_pending()
# Ancienne valeur cassée : r"(d{4}-d{2}-d{2})"
#   d{4} → matchait la chaîne littérale "dddd", jamais une vraie date
#   Résultat : date_obj = datetime.date.today() toujours, date du fichier ignorée
# Nouvelle valeur : (\d{4}-\d{2}-\d{2})
_DATE_RE = re.compile(
    "(" + _DIGIT + "{4}-" + _DIGIT + "{2}-" + _DIGIT + "{2})"
)

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
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))

# [MAIL-R5] SMTP_TO conservé en lecture seule pour compatibilité externe.
# NE PAS utiliser SMTP_TO directement dans le code interne — utiliser
# _get_recipients() qui relit os.environ à chaque appel. Cela permet :
#   - de surcharger REPORT_EMAIL en tests sans recharger le module
#   - d'éviter une valeur vide si .env est chargé APRÈS l'import
SMTP_TO = os.environ.get("REPORT_EMAIL", SMTP_USER)

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
# [MAIL-R3] ANTI-RÉCURSION FLAG
# send_report() → _resend_pending() → send_report() : boucle infinie si le
# renvoi d'un rapport en attente échoue à nouveau. Ce flag module-level coupe
# la récursion : _resend_pending() ne s'exécute jamais depuis un send_report()
# déjà déclenché par _resend_pending().
# ─────────────────────────────────────────────────────────────────────────────
_resending: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# [MAIL-R5] DESTINATAIRES DYNAMIQUES
# ─────────────────────────────────────────────────────────────────────────────

def _get_recipients() -> str:
    """
    [MAIL-R5] Relit REPORT_EMAIL depuis os.environ à chaque appel.
    Contrairement à SMTP_TO (figé à l'import), cette fonction reflète
    les modifications d'environnement postérieures à l'import du module
    (utile en tests et en rechargement de config sans redémarrage).
    Fallback : SMTP_USER si REPORT_EMAIL absent.
    """
    return os.environ.get("REPORT_EMAIL", SMTP_USER)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION (NEW-M3 + POST-M3 + MAIL-R1)
# ─────────────────────────────────────────────────────────────────────────────

def _is_valid_email(addr: str) -> bool:
    """
    Valide une adresse email avec _EMAIL_RE.
    [MAIL-R1] Regex corrigée : [^@\\s]+@[^@\\s]+\\.[^@\\s]+
    """
    return bool(_EMAIL_RE.match(addr.strip()))


def _parse_recipients(raw: str) -> list[str]:
    """Parse virgule/point-virgule, filtre les invalides (NEW-M1 + NEW-M3)."""
    if not raw:
        return []
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

    [CSV-M3] Guard filtrage SENTINEL_CSV_MODE :
        mode=standard → clés "_excel" ignorées
        mode=excel    → clés sans "_excel" ignorées
        mode=both     → tout attaché
        valeur inconnue → tout attaché (pas de perte silencieuse)

    Fallback par fichier : une erreur sur un CSV n'annule pas les autres.
    Retourne le nombre de fichiers effectivement attachés.
    """
    attached = 0

    for key, path in csv_attachments.items():
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
# LIGHT MODE — POST-M12 + CSV-M4 + WARN-1/WARN-2
# ─────────────────────────────────────────────────────────────────────────────

def _copy_headers(src: MIMEMultipart, dst: MIMEMultipart) -> None:
    """Copie les headers standard d'un message MIME vers un autre."""
    for header in ("From", "To", "Subject", "Date", "Message-ID"):
        if src[header]:
            dst[header] = src[header]


def _strip_csv_attachments(msg: MIMEMultipart) -> tuple[MIMEMultipart, int]:
    """
    [CSV-M4] Retire les pièces jointes CSV du message MIME.

    Appelée EN PRIORITÉ sur _strip_pdf_attachments() lors du light mode :
    les CSV sont des données analytiques accessibles dans output/,
    le PDF est le livrable principal — on sacrifie les CSV en premier.

    [WARN-1] Si des CSV sont retirés, attache un bandeau HTML d'avertissement
    via report_builder.build_light_mode_warning_part(["CSV"]).
    Import lazy + try/except : non bloquant si report_builder absent.

    Retourne (msg_allégé, nb_csv_retirés).
    """
    light_msg = MIMEMultipart("mixed")
    _copy_headers(msg, light_msg)

    removed = 0
    for part in msg.get_payload():
        if part.get_content_type() == "text/csv":
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
        # WARN-1 : bandeau HTML d'avertissement pour le destinataire
        try:
            from report_builder import build_light_mode_warning_part
            light_msg.attach(
                build_light_mode_warning_part(["CSV"], _ATTACH_LIMIT_MB)
            )
            log.debug("MAILER Bandeau avertissement CSV attaché au message allégé")
        except Exception as warn_err:
            log.debug(f"MAILER Bandeau avertissement CSV non généré : {warn_err}")

    return light_msg, removed


def _strip_pdf_attachments(msg: MIMEMultipart) -> MIMEMultipart:
    """
    POST-M12 : Retire les pièces jointes PDF du message MIME existant.

    Appelée APRÈS _strip_csv_attachments() dans la séquence de dégradation.
    msg peut déjà contenir le bandeau WARN-1 (CSV retiré précédemment).

    [WARN-2] Si le PDF est retiré, attache un bandeau HTML d'avertissement
    via report_builder.build_light_mode_warning_part(["PDF"]).
    Import lazy + try/except : non bloquant si report_builder absent.
    """
    light_msg = MIMEMultipart("mixed")
    _copy_headers(msg, light_msg)

    removed = 0
    for part in msg.get_payload():
        if part.get_content_type() == "application/pdf":
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
        # WARN-2 : bandeau HTML d'avertissement pour le destinataire
        try:
            from report_builder import build_light_mode_warning_part
            light_msg.attach(
                build_light_mode_warning_part(["PDF"], _ATTACH_LIMIT_MB)
            )
            log.debug("MAILER Bandeau avertissement PDF attaché au message allégé")
        except Exception as warn_err:
            log.debug(f"MAILER Bandeau avertissement PDF non généré : {warn_err}")

    return light_msg


def _check_message_size(
    msg_bytes:   bytes,
    msg:         MIMEMultipart,
    report_path: str | None = None,
) -> tuple[bool, MIMEMultipart]:
    """
    POST-M11 + POST-M12 + CSV-M4 : Vérifie la taille du message et applique
    le light mode si nécessaire.

    [MAIL-R7] msg_bytes passé en paramètre (déjà calculé dans send_report()
    avant cet appel) pour éviter un double as_bytes() sur le message original.
    Les as_bytes() internes (msg_no_csv, light_msg) sont inévitables car ces
    objets sont de nouvelles structures créées après les opérations de strip.

    Retourne (peut_envoyer, msg_à_envoyer).

    Séquence de dégradation v3.53 :
      1. Taille ≤ SMTP_ATTACH_WARN_MB    → envoi normal
      2. Taille ≤ SMTP_ATTACH_LIMIT_MB   → warning log non bloquant, envoi normal
      3. Taille > SMTP_ATTACH_LIMIT_MB   →
         3a. Retrait CSV d'abord          → si ça passe : envoi sans CSV
                                            bandeau ["CSV"] attaché (WARN-1)
         3b. Retrait CSV + PDF            → si ça passe : envoi HTML seul
                                            bandeau ["PDF"] attaché (WARN-2)
         3c. Encore trop lourd            → blocage total + archivage output/pending/
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
        # as_bytes() ici est inévitable : msg_no_csv est un nouveau MIMEMultipart
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
    log.warning("MAILER Light mode CSV insuffisant — tentative retrait PDF…")
    light_msg   = _strip_pdf_attachments(msg_no_csv)
    # as_bytes() ici est inévitable : light_msg est un nouveau MIMEMultipart
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
    """POST-M6 : log structuré du statut d'envoi."""
    status = "OK" if success else "ECHEC"
    log.info(f"MAILER Statut envoi [{date_str}] : {status}")


# ─────────────────────────────────────────────────────────────────────────────
# RENVOI DES RAPPORTS EN ATTENTE — POST-M5
# [MAIL-R2] Regex _DATE_RE corrigée via chr(92) — WARN-5 documentait la
#            correction mais le code montrait encore r"(d{4}-d{2}-d{2})" cassé.
# [MAIL-R3] Anti-récursion via flag _resending
# [MAIL-R8] Limitation CSV documentée explicitement
# ─────────────────────────────────────────────────────────────────────────────

def _resend_pending() -> None:
    """
    Renvoie les rapports archivés dans output/pending/.
    POST-M5 : appelée en début de send_report().

    [MAIL-R2] _DATE_RE correctement construit via chr(92) — WARN-5 documentait
    la correction mais le code montrait encore r"(d{4}-d{2}-d{2})" cassé.

    [MAIL-R3] Guard anti-récursion : send_report() appelle _resend_pending()
    qui appelle send_report() → boucle infinie si le renvoi échoue à nouveau.
    Le flag _resending coupe la récursion : _resend_pending() ne s'exécute
    jamais à l'intérieur d'un send_report() lui-même déclenché par _resend_pending().

    [MAIL-R8] Limitation de conception : les rapports renvoyés n'incluent
    JAMAIS les fichiers CSV. Les CSV ne sont pas archivés dans output/pending/
    (seul le fichier HTML l'est). Pour un renvoi complet, relancer le pipeline.
    """
    global _resending

    # [MAIL-R3] Coupe la récursion
    if _resending:
        log.debug("MAILER _resend_pending() ignoré — déjà en cours de renvoi")
        return

    pending_dir = Path("output/pending")
    if not pending_dir.exists():
        return

    _resending = True
    try:
        for html_file in sorted(pending_dir.glob("*.html")):
            try:
                # [MAIL-R2] _DATE_RE : \d{4}-\d{2}-\d{2} correctement construit
                m = _DATE_RE.search(html_file.name)
                date_obj = (
                    datetime.date.fromisoformat(m.group(1))
                    if m else datetime.date.today()
                )
                log.info(f"MAILER Tentative renvoi rapport en attente : {html_file.name}")
                # [MAIL-R8] csv_attachments non passé — limitation connue :
                # les CSV ne sont pas archivés dans output/pending/.
                ok = send_report(str(html_file), date_obj=date_obj)
                if ok:
                    html_file.unlink()
                    log.info(f"MAILER rapport renvoyé et supprimé : {html_file.name}")
            except Exception as e:
                log.warning(f"MAILER resend pending {html_file.name} : {e}")
    finally:
        _resending = False


# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK MIME MINIMAL (si report_builder absent)
# [MAIL-R4] MIMEText importé en tête — plus d'import local ici
# [MAIL-R6] VERSION utilisé dans le corps du message
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
    [MAIL-R4] MIMEText importé en tête de fichier (plus d'import local).
    [MAIL-R6] VERSION utilisé dans le corps du message.
    """
    msg = MIMEMultipart("mixed")
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject or (
        f"SENTINEL — Rapport robotique défense du {date_str}"
    )

    body = (
        f"Bonjour,\n\n"
        f"Veuillez trouver ci-joint le rapport SENTINEL v{VERSION} du {date_str}.\n"
        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la défense.\n"
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
# [MAIL-R3] Anti-récursion via _resending
# [MAIL-R4] MIMEText importé en tête — plus d'import local dans crash handler
# [MAIL-R5] _get_recipients() remplace SMTP_TO figé à l'import
# [MAIL-R7] msg_bytes calculé une seule fois, passé à _check_message_size()
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

    Compatibilité sentinel_main.py v3.54 — API inchangée v3.53 :
        Pipeline normal  : send_report(html_report)
        Crash handler    : send_report(None, subject="[SENTINEL CRASH] ...")
        Rapport mensuel  : send_report(html_report)
        Avec CSV         : send_report(html_report, csv_attachments=csv_exports)

    Paramètres
    ----------
    report_path       : str | None  Chemin du fichier HTML. None = texte seul.
    date_obj          : date        Date du rapport. Défaut : aujourd'hui.
    extra_recipients  : list        Destinataires supplémentaires (optionnel).
    subject           : str         Sujet complet (crash handler).
    csv_attachments   : dict        dict[str, Path] retourné par
                                    SentinelDB.export_csv(). Optionnel.

    Séquence de dégradation taille v3.53 :
        ≤ SMTP_ATTACH_WARN_MB  → envoi normal
        ≤ SMTP_ATTACH_LIMIT_MB → warning log + envoi normal
        > SMTP_ATTACH_LIMIT_MB → 3a. retrait CSV  (bandeau WARN-1 attaché)
                                  3b. retrait PDF  (bandeau WARN-2 attaché)
                                  3c. blocage total + archivage output/pending/
    """
    # [MAIL-R3] _resend_pending() ignorée si déjà dans un renvoi
    if not _resending:
        _resend_pending()

    if date_obj is None:
        date_obj = datetime.date.today()

    # [MAIL-R5] Relire REPORT_EMAIL dynamiquement à chaque appel
    smtp_to_current = _get_recipients()

    # ── Cas crash handler : report_path=None (POST-M2) ───────────────────────
    if report_path is None:
        if subject:
            log.warning(f"MAILER Crash handler — {subject}")
        else:
            log.warning("MAILER send_report(None) appelé sans sujet")
        if not SMTP_USER or not SMTP_PASSWORD:
            log.error("MAILER SMTP non configuré — alerte crash non envoyée")
            return False
        # [MAIL-R5] Destinataires relus dynamiquement
        recipients = _parse_recipients(smtp_to_current)
        if not recipients:
            log.error("MAILER Aucun destinataire — alerte crash non envoyée")
            return False
        # [MAIL-R4] MIMEText importé en tête — utilisé directement (plus d'import local)
        crash_msg = MIMEMultipart()
        crash_msg["From"]    = SMTP_USER
        crash_msg["To"]      = ", ".join(recipients)
        crash_msg["Subject"] = subject or f"{EMAIL_SUBJECT_PREFIX} — Erreur pipeline"
        crash_msg.attach(MIMEText(subject or "Erreur pipeline SENTINEL.", "plain", "utf-8"))
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

    # [MAIL-R5] Destinataires relus dynamiquement
    to_list = _parse_recipients(smtp_to_current)
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
        else:
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
    # [MAIL-R7] msg_bytes calculé UNE SEULE FOIS ici et passé à _check_message_size()
    # qui ne refait plus as_bytes() sur msg original (évite le double encodage coûteux).
    msg_bytes = msg.as_bytes()
    can_send, msg = _check_message_size(msg_bytes, msg, report_path=report_path)
    if not can_send:
        return False

    success = _smtp_send(msg.as_string(), SMTP_USER, to_list, report_path=report_path)
    _log_send_status(str(date_obj), success)
    return success


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC — POST-M10
# [MAIL-R5] _get_recipients() pour refléter l'état courant de l'environnement
# [MAIL-R6] VERSION incluse dans le dictionnaire retourné
# ─────────────────────────────────────────────────────────────────────────────

def check_config() -> dict:
    """
    Vérifie la configuration SMTP et retourne un dictionnaire de statut.
    POST-M10 : health_check.py doit appeler cette fonction (pas smtplib direct).
    [MAIL-R5] _get_recipients() pour refléter l'état courant de l'environnement.
    [MAIL-R6] VERSION incluse dans le dict retourné.
    """
    recipients = _parse_recipients(_get_recipients())
    return {
        "version":          VERSION,
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
    """
    Envoie un email de test pour valider la configuration SMTP.
    [MAIL-R4] MIMEText importé en tête — plus d'import local ici.
    [MAIL-R5] _get_recipients() pour destinataires dynamiques.
    [MAIL-R6] VERSION utilisé dans le sujet et le corps du message.
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("MAILER Config incomplète — test annulé")
        return False

    recipients = _parse_recipients(_get_recipients())
    if not recipients:
        log.error("MAILER Aucun destinataire valide — test annulé")
        return False

    msg = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = f"{EMAIL_SUBJECT_PREFIX} — Test configuration SMTP v{VERSION}"
    msg.attach(MIMEText(
        f"Ceci est un email de test SENTINEL v{VERSION}.\n"
        "Si vous recevez ce message, la configuration SMTP est correcte.\n",
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
# [MAIL-R6] VERSION utilisé dans les print() CLI
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
        print(f"\n── Configuration SMTP SENTINEL v{VERSION} ────────────────────")
        for k, v in cfg.items():
            status = "✓" if v is True else "✗" if v is False else " "
            print(f"  {status} {k:<22} {v}")
        print("─────────────────────────────────────────────────────────\n")
        sys.exit(0 if cfg["ready"] else 1)

    if "--test" in sys.argv:
        ok = send_test_email()
        sys.exit(0 if ok else 1)

    print(f"SENTINEL Mailer v{VERSION}")
    print("Usage :")
    print("  python mailer.py --check    Vérifie la configuration SMTP")
    print("  python mailer.py --test     Envoie un email de test")
