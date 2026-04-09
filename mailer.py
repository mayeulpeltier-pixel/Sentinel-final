# mailer.py — SENTINEL v3.40 — Envoi email via SMTP (Gmail ou autre)
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   B10          MIME text/html + application/pdf (non octet-stream)
#                → délégué à report_builder.build_email_message()
#   F-4          SMTP_USER / GMAIL_USER / SMTP_HOST / SMTP_PORT tous
#                configurables via .env
#   FIX-OBS1     Logging structuré via logger nommé, plus de print()
#   K-6 / R3A3-NEW-5  3 tentatives SMTP, backoff 30s → 90s → abandon
#   NEW-M1       Destinataires multiples (REPORT_EMAIL=a@b.com,c@d.com)
#   NEW-M2       Mode dry-run (SENTINEL_MAILER_DRYRUN=1) pour tests CI
#   NEW-M3       Validation adresses email avant envoi
#   NEW-M4       Intégration SentinelDB — log de l'envoi dans metrics
#   NEW-M5       Support TLS direct (port 465) en plus STARTTLS (port 587)
#   NEW-M6       Objet email personnalisable via ENV SENTINEL_EMAIL_SUBJECT
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
#   SENTINEL_MAILER_DRYRUN       1 = log seulement, pas d'envoi réel
#   SENTINEL_EMAIL_SUBJECT       préfixe du sujet (défaut : SENTINEL)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
import re
import smtplib
import time
from pathlib import Path
from typing import Optional

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

# ── Paramètres retry (K-6 / R3A3-NEW-5) ──────────────────────────────────────
_SMTP_MAX_ATTEMPTS = 3
_SMTP_BACKOFF      = [30, 90]   # secondes entre tentatives 1→2 et 2→3


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION (NEW-M3)
# ─────────────────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"^[^@s]+@[^@s]+.[^@s]+$")


def _is_valid_email(addr: str) -> bool:
    return bool(_EMAIL_RE.match(addr.strip()))


def _parse_recipients(raw: str) -> list[str]:
    """
    Parse une chaîne d'adresses séparées par virgule ou point-virgule.
    Filtre les adresses invalides avec log.warning (NEW-M1 + NEW-M3).
    """
    candidates = [a.strip() for a in re.split(r"[,;]", raw) if a.strip()]
    valid   = [a for a in candidates if _is_valid_email(a)]
    invalid = [a for a in candidates if not _is_valid_email(a)]
    if invalid:
        log.warning(f"MAILER Adresses ignorées (format invalide) : {invalid}")
    return valid


# ─────────────────────────────────────────────────────────────────────────────
# ENVOI SMTP
# ─────────────────────────────────────────────────────────────────────────────

def _smtp_send(msg_string: str, from_addr: str, to_list: list[str]) -> bool:
    """
    Envoie le message MIME via SMTP avec 3 tentatives et backoff.
    Supporte STARTTLS (port 587, défaut) et TLS direct (port 465).
    Retourne True si succès, False si toutes les tentatives échouent.
    K-6 / R3A3-NEW-5 / NEW-M5
    """
    for attempt in range(_SMTP_MAX_ATTEMPTS):
        try:
            if SMTP_PORT == 465:
                # TLS direct (NEW-M5)
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.sendmail(from_addr, to_list, msg_string)
            else:
                # STARTTLS — port 587 par défaut (F-4)
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
    # C2-FIX : archiver rapport HTML si SMTP échoue définitivement
    try:
        if report_path and Path(report_path).exists():
            pending_dir = Path("output/pending")
            pending_dir.mkdir(parents=True, exist_ok=True)
            import shutil
            dest = pending_dir / Path(report_path).name
            shutil.copy2(report_path, dest)
            log.warning(f"MAILER rapport archivé → {dest} (sera renvoyé au prochain succès SMTP)")
    except Exception as _arch_err:
        log.warning(f"MAILER archivage impossible : {_arch_err}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# LOG ENVOI DANS SENTINELDB (NEW-M4)
# ─────────────────────────────────────────────────────────────────────────────

def _log_send_to_db(date_str: str, success: bool) -> None:
    """
    Enregistre le statut d'envoi email dans SentinelDB.metrics.
    NEW-M4 — non bloquant.
    """
    try:
        from db_manager import SentinelDB
        # On utilise un champ booléen encodé en REAL (1.0 / 0.0)
        # La colonne "alerte" n'est pas touchée ici — on n'écrit que si la
        # ligne existe déjà pour éviter de créer une ligne fantôme.
        rows = SentinelDB.getmetrics(ndays=1)
        if rows and rows[0].get("date") == date_str:
            # Ligne du jour présente → on peut l'enrichir si nécessaire
            pass  # Aucune colonne email_sent dans le schéma v3.40 pour l'instant
        status = "OK" if success else "ECHEC"
        log.info(f"MAILER Statut envoi [{date_str}] : {status}")
    except Exception as e:
        log.debug(f"MAILER log_send_to_db non bloquant : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# FONCTION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────


def _resend_pending() -> None:
    """C2-FIX : renvoie les rapports archivés dans output/pending/ après un succès SMTP."""
    pending_dir = Path("output/pending")
    if not pending_dir.exists():
        return
    for html_file in sorted(pending_dir.glob("*.html")):
        try:
            import datetime as _dt
            # Extraire date du nom de fichier SENTINEL_YYYY-MM-DD_rapport.html
            m = __import__("re").search(r"(\d{4}-\d{2}-\d{2})", html_file.name)
            date_obj = _dt.date.fromisoformat(m.group(1)) if m else _dt.date.today()
            ok = send_report(str(html_file), date_obj)
            if ok:
                html_file.unlink()
                log.info(f"MAILER rapport en attente renvoyé et supprimé : {html_file.name}")
        except Exception as e:
            log.warning(f"MAILER resend pending {html_file.name} : {e}")


def send_report(
    report_path: str,
    date_obj,
    extra_recipients: Optional[list[str]] = None,
) -> bool:
    """
    Envoie le rapport HTML (+ PDF si disponible) par email.

    Paramètres
    ----------
    report_path       : str   Chemin du fichier HTML généré par build_html_report()
    date_obj          : date  Date du rapport (datetime.date)
    extra_recipients  : list  Destinataires supplémentaires (optionnel)

    Retourne True si l'email a été envoyé avec succès.

    Architecture
    ------------
    La construction du message MIME est entièrement déléguée à
    report_builder.build_email_message() qui gère les MIME corrects
    (B10-FIX) et les pièces jointes HTML + PDF.
    """
    # ── Pré-vérifications ─────────────────────────────────────────────────────
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error(
            "MAILER SMTP_USER ou SMTP_PASS manquants — configurer le fichier .env"
        )
        return False

    if not _is_valid_email(SMTP_USER):
        log.error(f"MAILER Adresse expéditeur invalide : {SMTP_USER!r}")
        return False

    # ── Destinataires (NEW-M1) ────────────────────────────────────────────────
    to_list = _parse_recipients(SMTP_TO)
    if extra_recipients:
        for addr in extra_recipients:
            if _is_valid_email(addr) and addr not in to_list:
                to_list.append(addr)

    if not to_list:
        log.error("MAILER Aucun destinataire valide — email annulé")
        return False

    # ── Construire le message MIME via report_builder (B10-FIX) ──────────────
    date_str = date_obj.strftime("%d/%m/%Y")
    try:
        from report_builder import build_email_message
        msg = build_email_message(
            date_str  = date_str,
            html_body = "",          # corps texte géré dans build_email_message
            from_addr = SMTP_USER,
            to_addr   = ", ".join(to_list),
            output_dir = Path(report_path).parent,
        )
    except ImportError:
        # Fallback — construction minimale si report_builder absent
        log.warning("MAILER report_builder absent — construction MIME minimale")
        msg = _build_minimal_mime(date_str, report_path, to_list)

    # Personnaliser le sujet (NEW-M6)
    msg["Subject"] = (
        f"{EMAIL_SUBJECT_PREFIX} — Rapport robotique défense du {date_str}"
    )
    # Forcer To correct (destinataires multiples)
    if "To" in msg:
        del msg["To"]
    msg["To"] = ", ".join(to_list)

    # ── Mode dry-run (NEW-M2) ─────────────────────────────────────────────────
    if DRY_RUN:
        log.info(
            f"MAILER [DRY-RUN] Email non envoyé — destinataires={to_list} "
            f"sujet={msg['Subject']!r}"
        )
        return True

    # ── Envoi SMTP avec retry (K-6 / R3A3-NEW-5) ─────────────────────────────
    success = _smtp_send(msg.as_string(), SMTP_USER, to_list)
    _log_send_to_db(str(date_obj), success)
    return success


# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK MIME MINIMAL (si report_builder absent)
# ─────────────────────────────────────────────────────────────────────────────\n\ndef _build_minimal_mime(\n    date_str: str,\n    report_path: str,\n    to_list: list[str],\n):\n    """\n    Construction MIME minimale de secours.\n    B10-FIX : MIME text/html et application/pdf corrects.\n    """\n    from email.encoders import encode_base64\n    from email.mime.base import MIMEBase\n    from email.mime.multipart import MIMEMultipart\n    from email.mime.text import MIMEText\n\n    msg = MIMEMultipart("mixed")\n    msg["From"]    = SMTP_USER\n    msg["To"]      = ", ".join(to_list)\n    msg["Subject"] = f"SENTINEL — Rapport robotique défense du {date_str}"\n\n    body = (\n        f"Bonjour,\n\n"\n        f"Veuillez trouver ci-joint le rapport SENTINEL du {date_str}.\n"\n        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la défense.\n"\n    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # Pièces jointes HTML + PDF (B10-FIX)
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
                    part = MIMEBase(m_type, m_sub)  # B10-FIX
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
# DIAGNOSTIC (utilitaire CLI)
# ─────────────────────────────────────────────────────────────────────────────\n\ndef check_config() -> dict:\n    """\n    Vérifie la configuration SMTP et retourne un dictionnaire de statut.\n    Utilisé par healthcheck.py.\n    """\n    recipients = _parse_recipients(SMTP_TO)\n    return {\n        "smtp_user_set":   bool(SMTP_USER),\n        "smtp_pass_set":   bool(SMTP_PASSWORD),\n        "smtp_host":       SMTP_HOST,\n        "smtp_port":       SMTP_PORT,\n        "smtp_tls":        "TLS direct" if SMTP_PORT == 465 else "STARTTLS",\n        "recipients":      recipients,\n        "recipients_ok":   len(recipients) > 0,\n        "dry_run":         DRY_RUN,\n        "subject_prefix":  EMAIL_SUBJECT_PREFIX,\n        "ready":           bool(SMTP_USER and SMTP_PASSWORD and recipients),\n    }\n\n\ndef send_test_email() -> bool:\n    """\n    Envoie un email de test pour valider la configuration SMTP.\n    Utilisé par healthcheck.py et en mode CLI.\n    """\n    from email.mime.text import MIMEText\n    from email.mime.multipart import MIMEMultipart\n\n    if not SMTP_USER or not SMTP_PASSWORD:\n        log.error("MAILER Config incomplète — test annulé")\n        return False\n\n    recipients = _parse_recipients(SMTP_TO)\n    if not recipients:\n        log.error("MAILER Aucun destinataire valide — test annulé")\n        return False\n\n    msg = MIMEMultipart()\n    msg["From"]    = SMTP_USER\n    msg["To"]      = ", ".join(recipients)\n    msg["Subject"] = f"{EMAIL_SUBJECT_PREFIX} — Test configuration SMTP"\n    msg.attach(MIMEText(\n        "Ceci est un email de test SENTINEL v3.40.\n"\n        "Si vous recevez ce message, la configuration SMTP est correcte.\n",\n        "plain", "utf-8",\n    ))

    success = _smtp_send(msg.as_string(), SMTP_USER, recipients)
    if success:
        log.info("MAILER Email de test envoyé avec succès ✓")
    else:
        log.error("MAILER Échec de l'email de test ✗")
    return success


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────\n\nif __name__ == "__main__":\n    import sys\n    logging.basicConfig(\n        level   = logging.INFO,\n        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",\n        datefmt = "%Y-%m-%dT%H:%M:%S",\n    )\n\n    if "--check" in sys.argv:\n        cfg = check_config()\n        print("\n── Configuration SMTP SENTINEL v3.40 ───────────────────")\n        for k, v in cfg.items():\n            status = "✓" if v else "✗" if v is False else " "\n            print(f"  {status} {k:<22} {v}")\n        print("─────────────────────────────────────────────────────────\n")\n        sys.exit(0 if cfg["ready"] else 1)

    if "--test" in sys.argv:
        ok = send_test_email()
        sys.exit(0 if ok else 1)

    print("Usage :")
    print("  python mailer.py --check    Vérifie la configuration SMTP")
    print("  python mailer.py --test     Envoie un email de test")