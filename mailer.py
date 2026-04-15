#!/usr/bin/env python3
# mailer.py — SENTINEL v3.60 — Envoi email via SMTP (Gmail ou autre)
# =============================================================================
# Corrections héritées v3.53 (toutes conservées — voir historique complet
# dans l'ancien mailer.py) :
#   MAIL-R1  Regex email _EMAIL_RE corrigée via chr(92)
#   MAIL-R2  Regex date _DATE_RE corrigée via chr(92)
#   MAIL-R3  Anti-récursion flag _resending
#   MAIL-R4  MIMEText importé en tête
#   MAIL-R5  _get_recipients() dynamique
#   MAIL-R6  VERSION centralisée
#   MAIL-R7  _check_message_size() sans double as_bytes()
#   MAIL-R8  Limitation CSV _resend_pending() documentée
#   CSV-M1/2/3/4/5 — Pièces jointes CSV
#   WARN-1/2 — Bandeaux light mode
#   NEW-M1..6, POST-M1..12 — Tous les correctifs antérieurs
#
# Corrections v3.60 — Audit inter-scripts 2026-04 :
#   MAIL-60-FIX1  BUG CRITIQUE : _SMTP_BACKOFF n'avait que 2 valeurs pour
#                 _SMTP_MAX_ATTEMPTS=3 → IndexError garanti à la 3ème tentative.
#                 Ajout SMTP_RETRY_WAIT_3 (180s par défaut). Accès protégé
#                 par min(attempt, len(_SMTP_BACKOFF)-1) en dernier recours.
#   MAIL-60-FIX2  Import config.py centralisé — SMTP_PASS lit maintenant
#                 SMTP_PASS / SMTP_PASSWORD / GMAIL_APP_PASSWORD (3 noms).
#                 Cohérence avec health_check.py et ops_patents.py.
#   MAIL-60-FIX3  DRY_RUN depuis config.py — cohérence watchdog/health_check.
#                 SENTINEL_DRY_RUN OU SENTINEL_MAILER_DRYRUN = source unique.
#   MAIL-60-FIX4  VERSION synchronisée sur 3.60 (source : config.py).
#   MAIL-60-FIX5  Chemins output/pending absolus via config.PROJECT_ROOT.
#   MAIL-60-FIX6  ssl.create_default_context() sur starttls() — anti-MITM.
# =============================================================================

from __future__ import annotations

import datetime
import logging
import os
import re
import shutil
import smtplib
import ssl
import time
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── POST-M4 : load_dotenv() AVANT les constantes module-level ────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── MAIL-60-FIX2 : config centralisée ────────────────────────────────────────
try:
    import config as _cfg
    _CONFIG_AVAILABLE = True
except ImportError:
    _cfg              = None   # type: ignore[assignment]
    _CONFIG_AVAILABLE = False

log = logging.getLogger("sentinel.mailer")

# ── MAIL-60-FIX4 : VERSION depuis config.py ──────────────────────────────────
VERSION = _cfg.VERSION if _CONFIG_AVAILABLE else "3.60"

# ─────────────────────────────────────────────────────────────────────────────
# MAIL-R1/R2 — REGEX BACKSLASH-SAFE via chr(92)
# ─────────────────────────────────────────────────────────────────────────────
_BS     = chr(92)
_NOT_AT = "[^@" + _BS + "s]"
_DOT_L  = _BS + "."
_DIGIT  = _BS + "d"

_EMAIL_RE = re.compile(
    "^" + _NOT_AT + "+" + "@" + _NOT_AT + "+" + _DOT_L + _NOT_AT + "+$"
)
_DATE_RE = re.compile(
    "(" + _DIGIT + "{4}-" + _DIGIT + "{2}-" + _DIGIT + "{2})"
)

# ── Configuration SMTP — MAIL-60-FIX2 (config centralisée) ───────────────────
if _CONFIG_AVAILABLE:
    SMTP_USER     = _cfg.SMTP_USER
    SMTP_PASSWORD = _cfg.SMTP_PASS   # gère les 3 noms de variable
    SMTP_HOST     = _cfg.SMTP_HOST
    SMTP_PORT     = _cfg.SMTP_PORT
else:
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

# MAIL-R5 : alias lecture-seule pour compatibilité externe
SMTP_TO = os.environ.get("REPORT_EMAIL", SMTP_USER)

# ── MAIL-60-FIX3 : DRY_RUN depuis config.py ──────────────────────────────────
if _CONFIG_AVAILABLE:
    DRY_RUN = _cfg.DRY_RUN
else:
    DRY_RUN = (
        os.environ.get("SENTINEL_DRY_RUN", "").lower() in ("1", "true", "yes")
        or os.environ.get("SENTINEL_MAILER_DRYRUN", "0").strip() == "1"
    )

EMAIL_SUBJECT_PREFIX = os.environ.get("SENTINEL_EMAIL_SUBJECT", "SENTINEL")

# ── MAIL-60-FIX1 : SMTP_BACKOFF — 3 valeurs pour 3 tentatives ────────────────
# BUG CORRIGÉ : l'ancienne liste [30, 90] provoquait un IndexError à la 3ème
# tentative (_SMTP_BACKOFF[2] → hors limites).
# Correction : ajout de SMTP_RETRY_WAIT_3 (180s par défaut).
# Sécurité supplémentaire : accès via min(attempt, len-1) dans _smtp_send().
_SMTP_MAX_ATTEMPTS = 3
_SMTP_BACKOFF: list[int] = [
    int(os.environ.get("SMTP_RETRY_WAIT_1", "30")),
    int(os.environ.get("SMTP_RETRY_WAIT_2", "90")),
    int(os.environ.get("SMTP_RETRY_WAIT_3", "180")),  # MAIL-60-FIX1 : valeur ajoutée
]

_ATTACH_WARN_MB  = int(os.environ.get("SMTP_ATTACH_WARN_MB",  "10"))
_ATTACH_LIMIT_MB = int(os.environ.get("SMTP_ATTACH_LIMIT_MB", "24"))
_CSV_MODE        = os.environ.get("SENTINEL_CSV_MODE", "both")

# MAIL-60-FIX5 : chemin absolu pour output/pending
if _CONFIG_AVAILABLE:
    _PENDING_DIR = _cfg.PROJECT_ROOT / "output" / "pending"
else:
    _PENDING_DIR = Path(__file__).resolve().parent / "output" / "pending"

# MAIL-R3 : anti-récursion
_resending: bool = False

# ─────────────────────────────────────────────────────────────────────────────
# MAIL-R5 : Destinataires dynamiques
# ─────────────────────────────────────────────────────────────────────────────

def _get_recipients() -> str:
    return os.environ.get("REPORT_EMAIL", SMTP_USER)

# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def _is_valid_email(addr: str) -> bool:
    return bool(_EMAIL_RE.match(addr.strip()))

def _parse_recipients(raw: str) -> list[str]:
    if not raw:
        return []
    candidates = [a.strip() for a in re.split(r"[,;]", raw) if a.strip()]
    valid      = [a for a in candidates if _is_valid_email(a)]
    invalid    = [a for a in candidates if not _is_valid_email(a)]
    if invalid:
        log.warning(f"MAILER Adresses ignorées (format invalide) : {invalid}")
    return valid

# ─────────────────────────────────────────────────────────────────────────────
# Pièces jointes CSV
# ─────────────────────────────────────────────────────────────────────────────

def _attach_csv_files(msg: MIMEMultipart, csv_attachments: dict) -> int:
    attached = 0
    for key, path in csv_attachments.items():
        if _CSV_MODE == "standard" and key.endswith("_excel"):
            continue
        if _CSV_MODE == "excel" and not key.endswith("_excel"):
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
            part.add_header("Content-Disposition", "attachment", filename=path.name)
            msg.attach(part)
            attached += 1
            log.info(f"MAILER CSV joint : {path.name} ({path.stat().st_size // 1024} Ko)")
        except Exception as e:
            log.warning(f"MAILER CSV '{key}' attachement échoué : {e}")
    return attached

# ─────────────────────────────────────────────────────────────────────────────
# Light mode — dégradation gracieuse
# ─────────────────────────────────────────────────────────────────────────────

def _copy_headers(src: MIMEMultipart, dst: MIMEMultipart) -> None:
    for header in ("From", "To", "Subject", "Date", "Message-ID"):
        if src[header]:
            dst[header] = src[header]

def _strip_csv_attachments(msg: MIMEMultipart) -> tuple[MIMEMultipart, int]:
    light_msg = MIMEMultipart("mixed")
    _copy_headers(msg, light_msg)
    removed = 0
    for part in msg.get_payload():
        if part.get_content_type() == "text/csv":
            removed += 1
        else:
            light_msg.attach(part)
    if removed:
        log.warning(f"MAILER Light mode : {removed} CSV retiré(s)")
        try:
            from report_builder import build_light_mode_warning_part  # type: ignore
            light_msg.attach(build_light_mode_warning_part(["CSV"], _ATTACH_LIMIT_MB))
        except Exception:
            pass
    return light_msg, removed

def _strip_pdf_attachments(msg: MIMEMultipart) -> MIMEMultipart:
    light_msg = MIMEMultipart("mixed")
    _copy_headers(msg, light_msg)
    removed = 0
    for part in msg.get_payload():
        if part.get_content_type() == "application/pdf":
            removed += 1
        else:
            light_msg.attach(part)
    if removed:
        log.warning(f"MAILER Light mode : {removed} PDF retiré(s)")
        try:
            from report_builder import build_light_mode_warning_part  # type: ignore
            light_msg.attach(build_light_mode_warning_part(["PDF"], _ATTACH_LIMIT_MB))
        except Exception:
            pass
    return light_msg

def _check_message_size(
    msg_bytes:   bytes,
    msg:         MIMEMultipart,
    report_path: str | None = None,
) -> tuple[bool, MIMEMultipart]:
    size_mb = len(msg_bytes) / (1024 * 1024)
    if size_mb <= _ATTACH_WARN_MB:
        log.info(f"MAILER Taille : {size_mb:.1f} Mo ✓")
        return True, msg
    if size_mb <= _ATTACH_LIMIT_MB:
        log.warning(f"MAILER Message lourd : {size_mb:.1f} Mo — envoi tenté")
        return True, msg

    log.warning(f"MAILER Message trop lourd : {size_mb:.1f} Mo — light mode CSV…")
    msg_no_csv, nb_csv = _strip_csv_attachments(msg)
    if nb_csv:
        no_csv_mb = len(msg_no_csv.as_bytes()) / (1024 * 1024)
        if no_csv_mb <= _ATTACH_LIMIT_MB:
            return True, msg_no_csv
    else:
        msg_no_csv = msg

    log.warning("MAILER Light mode CSV insuffisant — retrait PDF…")
    light_msg  = _strip_pdf_attachments(msg_no_csv)
    light_mb   = len(light_msg.as_bytes()) / (1024 * 1024)
    if light_mb <= _ATTACH_LIMIT_MB:
        return True, light_msg

    log.error(f"MAILER Blocage total : {light_mb:.1f} Mo > {_ATTACH_LIMIT_MB} Mo")
    if report_path and Path(report_path).exists():
        try:
            _PENDING_DIR.mkdir(parents=True, exist_ok=True)
            dest = _PENDING_DIR / Path(report_path).name
            shutil.copy2(report_path, dest)
            log.warning(f"MAILER Rapport archivé → {dest}")
        except Exception as e:
            log.warning(f"MAILER Archivage impossible : {e}")
    return False, msg

# ─────────────────────────────────────────────────────────────────────────────
# Envoi SMTP — MAIL-60-FIX1/FIX6
# ─────────────────────────────────────────────────────────────────────────────

def _smtp_send(
    msg_string:  str,
    from_addr:   str,
    to_list:     list[str],
    report_path: str | None = None,
) -> bool:
    """
    MAIL-60-FIX1 : accès _SMTP_BACKOFF via min(attempt, len-1) — sécurité
                   supplémentaire même si la liste a maintenant 3 valeurs.
    MAIL-60-FIX6 : ssl.create_default_context() pour STARTTLS — anti-MITM.
    """
    for attempt in range(_SMTP_MAX_ATTEMPTS):
        try:
            ctx = ssl.create_default_context()  # MAIL-60-FIX6
            if SMTP_PORT == 465:
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as s:
                    s.login(SMTP_USER, SMTP_PASSWORD)
                    s.sendmail(from_addr, to_list, msg_string)
            else:
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                    s.ehlo()
                    s.starttls(context=ctx)  # MAIL-60-FIX6 : contexte SSL explicite
                    s.login(SMTP_USER, SMTP_PASSWORD)
                    s.sendmail(from_addr, to_list, msg_string)
            log.info(f"MAILER Email envoyé → {to_list} (tentative {attempt + 1})")
            return True
        except smtplib.SMTPAuthenticationError as e:
            log.error(f"MAILER Auth SMTP échouée : {e}")
            break
        except (smtplib.SMTPException, OSError) as e:
            log.warning(f"MAILER Tentative {attempt + 1}/{_SMTP_MAX_ATTEMPTS} : {e}")
            if attempt < _SMTP_MAX_ATTEMPTS - 1:
                # MAIL-60-FIX1 : min() en garde-fou même avec 3 valeurs
                wait = _SMTP_BACKOFF[min(attempt, len(_SMTP_BACKOFF) - 1)]
                log.info(f"MAILER Retry dans {wait}s…")
                time.sleep(wait)

    log.error(f"MAILER Échec définitif après {_SMTP_MAX_ATTEMPTS} tentatives")
    if report_path and Path(report_path).exists():
        try:
            _PENDING_DIR.mkdir(parents=True, exist_ok=True)
            dest = _PENDING_DIR / Path(report_path).name
            shutil.copy2(report_path, dest)
            log.warning(f"MAILER Rapport archivé → {dest}")
        except Exception as e:
            log.warning(f"MAILER Archivage impossible : {e}")
    return False

# ─────────────────────────────────────────────────────────────────────────────
# Log statut
# ─────────────────────────────────────────────────────────────────────────────

def _log_send_status(date_str: str, success: bool) -> None:
    log.info(f"MAILER Statut envoi [{date_str}] : {'OK' if success else 'ECHEC'}")

# ─────────────────────────────────────────────────────────────────────────────
# Renvoi rapports en attente
# ─────────────────────────────────────────────────────────────────────────────

def _resend_pending() -> None:
    global _resending
    if _resending:
        return
    if not _PENDING_DIR.exists():
        return
    _resending = True
    try:
        for html_file in sorted(_PENDING_DIR.glob("*.html")):
            try:
                m        = _DATE_RE.search(html_file.name)
                date_obj = (
                    datetime.date.fromisoformat(m.group(1))
                    if m else datetime.date.today()
                )
                log.info(f"MAILER Renvoi en attente : {html_file.name}")
                ok = send_report(str(html_file), date_obj=date_obj)
                if ok:
                    html_file.unlink()
                    log.info(f"MAILER Rapport renvoyé et supprimé : {html_file.name}")
            except Exception as e:
                log.warning(f"MAILER resend {html_file.name} : {e}")
    finally:
        _resending = False

# ─────────────────────────────────────────────────────────────────────────────
# Fallback MIME minimal
# ─────────────────────────────────────────────────────────────────────────────

def _build_minimal_mime(
    date_str:    str,
    report_path: str,
    to_list:     list[str],
    subject:     str | None = None,
) -> MIMEMultipart:
    msg            = MIMEMultipart("mixed")
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject or f"SENTINEL — Rapport robotique défense du {date_str}"
    msg.attach(MIMEText(
        f"Rapport SENTINEL v{VERSION} du {date_str}.
"
        "Robotique marine et terrestre — défense.",
        "plain", "utf-8",
    ))
    base = Path(report_path)
    for suffix, m_type, m_sub in [(".html", "text", "html"), (".pdf", "application", "pdf")]:
        att = base.with_suffix(suffix)
        if not att.exists() and suffix == ".html":
            att = Path(report_path)
        if att.exists():
            try:
                part = MIMEBase(m_type, m_sub)
                with open(att, "rb") as f:
                    part.set_payload(f.read())
                encode_base64(part)
                part.add_header("Content-Disposition", "attachment", filename=att.name)
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
    if not _resending:
        _resend_pending()

    if date_obj is None:
        date_obj = datetime.date.today()

    smtp_to_current = _get_recipients()

    # Cas crash handler
    if report_path is None:
        if not SMTP_USER or not SMTP_PASSWORD:
            log.error("MAILER SMTP non configuré — alerte crash non envoyée")
            return False
        recipients = _parse_recipients(smtp_to_current)
        if not recipients:
            log.error("MAILER Aucun destinataire — alerte crash non envoyée")
            return False
        crash_msg            = MIMEMultipart()
        crash_msg["From"]    = SMTP_USER
        crash_msg["To"]      = ", ".join(recipients)
        crash_msg["Subject"] = subject or f"{EMAIL_SUBJECT_PREFIX} — Erreur pipeline"
        crash_msg.attach(MIMEText(subject or "Erreur pipeline SENTINEL.", "plain", "utf-8"))
        if DRY_RUN:
            log.info(f"MAILER [DRY-RUN] Alerte crash — sujet={crash_msg['Subject']!r}")
            return True
        return _smtp_send(crash_msg.as_string(), SMTP_USER, recipients)

    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("MAILER SMTP_USER ou SMTP_PASS manquants — configurer .env")
        return False
    if not _is_valid_email(SMTP_USER):
        log.error(f"MAILER Adresse expéditeur invalide : {SMTP_USER!r}")
        return False

    to_list = _parse_recipients(smtp_to_current)
    if extra_recipients:
        for addr in extra_recipients:
            if _is_valid_email(addr) and addr not in to_list:
                to_list.append(addr)
    if not to_list:
        log.error("MAILER Aucun destinataire valide — email annulé")
        return False

    date_str = date_obj.strftime("%d/%m/%Y")
    msg: MIMEMultipart | None = None
    try:
        from report_builder import build_email_message  # type: ignore
        msg = build_email_message(
            date_str   = date_str,
            html_body  = "",
            from_addr  = SMTP_USER,
            to_addr    = ", ".join(to_list),
            output_dir = Path(report_path).parent,
        )
    except ImportError:
        log.warning("MAILER report_builder absent — fallback MIME minimal")
    except Exception as e:
        log.warning(f"MAILER build_email_message() : {e} — fallback")

    if msg is None:
        msg = _build_minimal_mime(date_str, report_path, to_list, subject=subject)

    final_subject = subject or f"{EMAIL_SUBJECT_PREFIX} — Rapport défense du {date_str}"
    if "Subject" in msg:
        del msg["Subject"]
    msg["Subject"] = final_subject
    if "To" in msg:
        del msg["To"]
    msg["To"] = ", ".join(to_list)

    if csv_attachments:
        nb_csv = _attach_csv_files(msg, csv_attachments)
        log.info(f"MAILER {nb_csv} CSV joint(s)")

    if DRY_RUN:
        nb_parts = len(msg.get_payload()) if isinstance(msg.get_payload(), list) else 1
        log.info(f"MAILER [DRY-RUN] → {to_list} | {msg['Subject']!r} | {nb_parts} part(s)")
        return True

    msg_bytes        = msg.as_bytes()
    can_send, msg    = _check_message_size(msg_bytes, msg, report_path=report_path)
    if not can_send:
        return False

    success = _smtp_send(msg.as_string(), SMTP_USER, to_list, report_path=report_path)
    _log_send_status(str(date_obj), success)
    return success

# ─────────────────────────────────────────────────────────────────────────────
# check_config() + send_test_email()
# ─────────────────────────────────────────────────────────────────────────────

def check_config() -> dict:
    recipients = _parse_recipients(_get_recipients())
    return {
        "version":         VERSION,
        "smtp_user_set":   bool(SMTP_USER),
        "smtp_pass_set":   bool(SMTP_PASSWORD),
        "smtp_host":       SMTP_HOST,
        "smtp_port":       SMTP_PORT,
        "smtp_tls":        "TLS direct" if SMTP_PORT == 465 else "STARTTLS",
        "recipients":      recipients,
        "recipients_ok":   len(recipients) > 0,
        "dry_run":         DRY_RUN,
        "subject_prefix":  EMAIL_SUBJECT_PREFIX,
        "retry_waits":     _SMTP_BACKOFF,
        "attach_warn_mb":  _ATTACH_WARN_MB,
        "attach_limit_mb": _ATTACH_LIMIT_MB,
        "csv_mode":        _CSV_MODE,
        "ready":           bool(SMTP_USER and SMTP_PASSWORD and recipients),
    }

def send_test_email() -> bool:
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("MAILER Config incomplète — test annulé")
        return False
    recipients = _parse_recipients(_get_recipients())
    if not recipients:
        log.error("MAILER Aucun destinataire — test annulé")
        return False
    msg            = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = f"{EMAIL_SUBJECT_PREFIX} — Test SMTP v{VERSION}"
    msg.attach(MIMEText(
        f"Test SENTINEL v{VERSION}.
"
        "Si vous recevez ce message, la configuration SMTP est correcte.
",
        "plain", "utf-8",
    ))
    success = _smtp_send(msg.as_string(), SMTP_USER, recipients)
    log.info(f"MAILER Test email : {'OK ✓' if success else 'ECHEC ✗'}")
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
        print(f"
── MAILER SENTINEL v{VERSION} ─────────────────────────")
        for k, v in cfg.items():
            icon = "✓" if v is True else "✗" if v is False else " "
            print(f"  {icon} {k:<22} {v}")
        sys.exit(0 if cfg["ready"] else 1)
    if "--test" in sys.argv:
        sys.exit(0 if send_test_email() else 1)
    print(f"SENTINEL Mailer v{VERSION}")
    print("  --check   Vérifie la configuration SMTP")
    print("  --test    Envoie un email de test")
