#!/usr/bin/env python3
# watchdog.py — SENTINEL v3.40 — Surveillance cron & auto-recovery
# ─────────────────────────────────────────────────────────────────────────────
# Vérifie que sentinel_main.py a tourné, analyse la santé globale du pipeline,
# envoie des alertes email ciblées et peut relancer automatiquement le pipeline.
#
# Corrections v3.40 appliquées :
#   WD-FIX1  FIX-OBS1   — logging structuré (zéro print())
#   WD-FIX2  C-4        — vérification Python ≥ 3.10 en tête
#   WD-FIX3             — double source de vérité : log file + SQLite (db_manager)
#   WD-FIX4             — anti-spam alertes : 1 alerte max / 4h (state file JSON)
#   WD-FIX5  K-6        — SMTP retry exponentiel : 3 tentatives (30s / 90s)
#   WD-FIX6  CDC-4      — vérification espace disque + alerte seuil critique
#   WD-FIX7             — rotation automatique sentinel.log > 50 MB
#   WD-FIX8             — auto-restart sentinel_main.py si cron mort (opt-in)
#   WD-FIX9             — rapport JSON état watchdog dans logs/watchdog_state.json
#   WD-FIX10            — alerte email enrichie : DB stats, disk, last run detail
#   WD-FIX11            — email résumé quotidien "pipeline vivant" (8h30)
#   WD-FIX12 R6A3-NEW-1 — exception globale reraisée pour que cron détecte l'échec
#   WD-FIX13            — exit code 0=OK / 1=warning / 2=critique (CI/CD compliant)
#   WD-FIX14            — vérification output/ dernier HTML généré
#   WD-FIX15            — détection process zombie (sentinel_main bloqué)
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python watchdog.py                  Vérification complète
#   python watchdog.py --restart        Auto-restart si pipeline mort
#   python watchdog.py --summary        Force l'envoi du résumé quotidien
#   python watchdog.py --rotate-logs    Force la rotation des logs
# ─────────────────────────────────────────────────────────────────────────────
# Cron recommandé :
#   30 6  * * * cd /opt/sentinel && /usr/bin/python3 watchdog.py >> logs/watchdog.log 2>&1
#   30 8  * * * cd /opt/sentinel && /usr/bin/python3 watchdog.py --summary >> logs/watchdog.log 2>&1
# ─────────────────────────────────────────────────────────────────────────────
# Codes de sortie :
#   0 — Pipeline OK
#   1 — Warning (disque bas, log ancien, mais pas critique)
#   2 — Critique (pipeline mort, DB corrompue, espace disque insuffisant)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import os
import shutil
import smtplib
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Python ≥ 3.10 (C-4 / WD-FIX2) ───────────────────────────────────────────\nif sys.version_info < (3, 10):\n    sys.stderr.write(\n        f"WATCHDOG CRITIQUE : Python {sys.version_info.major}.{sys.version_info.minor} < 3.10\n"\n        "Requis : Python 3.10+  →  sudo apt install python3.10\n"\n    )
    sys.exit(2)

# ── Logging structuré (WD-FIX1 / FIX-OBS1) ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sentinel.watchdog")


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTES & CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

VERSION      = "3.38"

# Seuils temporels
SEUIL_H_ALERTE  = int(os.environ.get("SENTINEL_MAX_SILENCE", "26"))  # heures sans run → alerte
SEUIL_H_WARN    = 20   # heures sans run → warning seulement
ANTI_SPAM_H     = 4    # intervalle minimum entre deux alertes identiques
LOG_MAX_MB      = 50   # taille max sentinel.log avant rotation (CDC-4)
DISK_CRIT_MB    = 200  # espace disque critique (pipeline risque d'échouer)
DISK_WARN_MB    = 500  # espace disque warning

# Chemins
LOGFILE       = Path(os.environ.get("SENTINEL_LOG", "logs/sentinel.log"))
STATE_FILE    = Path("logs/watchdog_state.json")  # état anti-spam (WD-FIX9)
DB_PATH       = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
OUTPUT_DIR    = Path("output")

# SMTP (WD-FIX5)
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
ALERT_TO  = os.environ.get("REPORT_EMAIL", SMTP_USER)

# Flags CLI
AUTO_RESTART  = "--restart"    in sys.argv
FORCE_SUMMARY = "--summary"    in sys.argv
FORCE_ROTATE  = "--rotate-logs" in sys.argv

# Compteurs de sortie
_warnings  = 0
_critiques = 0


# ═════════════════════════════════════════════════════════════════════════════
# ÉTAT PERSISTANT (anti-spam / WD-FIX4 / WD-FIX9)
# ═════════════════════════════════════════════════════════════════════════════

def _load_state() -> dict[str, Any]:
    """Charge l'état persistant du watchdog depuis logs/watchdog_state.json."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"last_alert": {}, "last_summary": None, "runs": []}


def _save_state(state: dict[str, Any]) -> None:
    """Sauvegarde atomique de l'état watchdog."""
    Path("logs").mkdir(exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except OSError as e:
        log.warning(f"Impossible de sauvegarder l'état watchdog : {e}")


def _can_send_alert(state: dict, alert_key: str) -> bool:
    """WD-FIX4 — Vérifie le délai anti-spam pour un type d'alerte donné."""
    last = state.get("last_alert", {}).get(alert_key)
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now(timezone.utc) - last_dt).total_seconds() > ANTI_SPAM_H * 3600
    except ValueError:
        return True


def _mark_alert_sent(state: dict, alert_key: str) -> None:
    state.setdefault("last_alert", {})[alert_key] = datetime.now(timezone.utc).isoformat()


# ═════════════════════════════════════════════════════════════════════════════
# ENVOI EMAIL (WD-FIX5 — SMTP retry exponentiel, inspiré de mailer.py)
# ═════════════════════════════════════════════════════════════════════════════

def send_alert(subject: str, body: str, html_body: str = "") -> bool:
    """
    WD-FIX5 — Envoi email watchdog avec retry exponentiel (3 tentatives).
    Retourne True si envoi réussi.
    """
    if not SMTP_USER or not SMTP_PASS:
        log.warning(f"WATCHDOG Email non configuré — alerte : {subject}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_TO

    msg.attach(MIMEText(body, "plain", "utf-8"))
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))

    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(SMTP_USER, ALERT_TO.split(","), msg.as_string())
            log.info(f"Email envoyé → {ALERT_TO}  |  {subject}")
            return True
        except smtplib.SMTPAuthenticationError as e:
            log.error(f"SMTP auth échouée (tentative {attempt + 1}/3) : {e}")
            break  # Pas la peine de réessayer si les credentials sont mauvais
        except (smtplib.SMTPException, OSError) as e:
            delay = 30 * (3 ** attempt)  # 30s / 90s / 270s
            log.warning(f"SMTP tentative {attempt + 1}/3 échouée : {e} — retry dans {delay}s")
            if attempt < 2:
                time.sleep(delay)

    log.error(f"SMTP : 3 tentatives épuisées — alerte non envoyée : {subject}")
    return False


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 1 — Dernier run pipeline (log file + SQLite)
# ═════════════════════════════════════════════════════════════════════════════

def check_last_run() -> tuple[bool, float, str]:
    """
    WD-FIX3 — Double source de vérité : log file ET SQLite.
    Retourne (ok: bool, age_heures: float, message: str).
    """
    global _warnings, _critiques
    age_h_log  = float("inf")
    age_h_db   = float("inf")
    last_dt_log: datetime | None = None
    last_dt_db:  datetime | None = None

    # ── Source 1 : log file ──────────────────────────────────────────────────
    if LOGFILE.exists():
        lines = LOGFILE.read_text(encoding="utf-8", errors="ignore").splitlines()
        tokens_ok = ("terminé", "SENTINEL v", "pipeline terminé", "Rapport disponible")
        for line in reversed(lines[-1000:]):
            if any(tok in line for tok in tokens_ok):
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        last_dt_log = datetime.strptime(line[:19], fmt)
                        age_h_log   = (datetime.now() - last_dt_log).total_seconds() / 3600
                        break
                    except ValueError:
                        continue
                if last_dt_log:
                    break
    else:
        log.warning(f"Fichier log absent : {LOGFILE}")

    # ── Source 2 : SQLite (db_manager v3.40) ─────────────────────────────────
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=5)
            row  = conn.execute(
                "SELECT date FROM reports ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row:
                last_dt_db = datetime.fromisoformat(row[0])
                age_h_db   = (datetime.now() - last_dt_db).total_seconds() / 3600
        except sqlite3.Error as e:
            log.warning(f"SQLite lecture dernier rapport : {e}")

    # ── Décision finale : prendre la source la plus récente ──────────────────
    age_h  = min(age_h_log, age_h_db)
    source = "log" if age_h_log <= age_h_db else "SQLite"
    last_dt = last_dt_log if age_h_log <= age_h_db else last_dt_db

    if age_h == float("inf"):
        _critiques += 1
        return False, age_h, "Aucun run réussi trouvé (log absent + DB vide)"

    last_str = last_dt.strftime("%Y-%m-%d %H:%M") if last_dt else "inconnue"

    if age_h > SEUIL_H_ALERTE:
        _critiques += 1
        return (
            False, age_h,
            f"Pipeline mort depuis {age_h:.1f}h (seuil {SEUIL_H_ALERTE}h) "
            f"— dernier run : {last_str} [{source}]"
        )
    elif age_h > SEUIL_H_WARN:
        _warnings += 1
        return (
            False, age_h,
            f"Pipeline silencieux depuis {age_h:.1f}h (warning > {SEUIL_H_WARN}h) "
            f"— dernier run : {last_str} [{source}]"
        )
    else:
        return True, age_h, f"Dernier run OK il y a {age_h:.1f}h [{source}] ✓"


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 2 — Santé SQLite (WAL, intégrité, BUG-DB1)
# ═════════════════════════════════════════════════════════════════════════════

def check_sqlite_health() -> dict[str, Any]:
    """WD-FIX3 — Vérifie l'intégrité SQLite et collecte les stats pour l'email."""
    global _warnings, _critiques
    result: dict[str, Any] = {"ok": True, "detail": ""}

    if not DB_PATH.exists():
        result.update(ok=True, detail="DB absente (1er déploiement)")
        return result

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)

        # WAL mode
        jmode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if jmode.lower() != "wal":
            log.warning(f"SQLite journal_mode = '{jmode}' (WAL attendu)")
            _warnings += 1
            result["journal_mode_warn"] = jmode

        # Intégrité
        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if ic != "ok":
            _critiques += 1
            result.update(ok=False, detail=f"CORRUPTION DB : {ic}")
            log.error(f"SQLite CORRUPTION : {ic}")
            conn.close()
            return result

        # Comptages pour le rapport
        for table in ("reports", "seenhashes", "alertes", "tendances"):
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                result[f"n_{table}"] = n
            except sqlite3.Error:
                result[f"n_{table}"] = -1

        # BUG-DB1 : UNIQUE sur reports.date
        schema_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='reports' AND type='table'"
        ).fetchone()
        schema_sql = (schema_row[0] or "") if schema_row else ""
        if "UNIQUE" not in schema_sql.upper():
            _critiques += 1
            result["bug_db1"] = True
            log.error("SQLite BUG-DB1 : UNIQUE absent sur reports.date — UPSERT échouera")

        # Taille DB
        result["size_kb"] = DB_PATH.stat().st_size // 1024

        conn.close()
        result.update(ok=True, detail=f"{result.get('n_reports', 0)} rapports")

    except sqlite3.Error as e:
        _critiques += 1
        result.update(ok=False, detail=f"Erreur connexion SQLite : {e}")
        log.error(f"SQLite connexion : {e}")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 3 — Espace disque (WD-FIX6 / CDC-4)
# ═════════════════════════════════════════════════════════════════════════════

def check_disk_space() -> dict[str, Any]:
    """CDC-4 — Vérifie l'espace disque libre."""
    global _warnings, _critiques
    result: dict[str, Any] = {}
    try:
        usage   = shutil.disk_usage(".")
        free_mb = usage.free // (1024 * 1024)
        pct     = 100 * usage.used // usage.total
        result  = {"free_mb": free_mb, "used_pct": pct, "ok": True}

        if free_mb < DISK_CRIT_MB:
            _critiques += 1
            result["ok"] = False
            log.error(f"DISQUE CRITIQUE : {free_mb} MB libres ({pct}% utilisé)")
        elif free_mb < DISK_WARN_MB:
            _warnings += 1
            log.warning(f"Disque bas : {free_mb} MB libres ({pct}% utilisé)")
        else:
            log.info(f"Disque OK : {free_mb} MB libres ({pct}% utilisé) ✓")
    except OSError as e:
        log.warning(f"Disque : impossible de vérifier : {e}")
        result = {"free_mb": -1, "used_pct": -1, "ok": True}  # non bloquant
    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 4 — Output directory (WD-FIX14)
# ═════════════════════════════════════════════════════════════════════════════

def check_output_fresh() -> dict[str, Any]:
    """WD-FIX14 — Vérifie que le dernier rapport HTML a bien été généré."""
    global _warnings
    result: dict[str, Any] = {"last_html": None, "age_h": None, "ok": True}

    if not OUTPUT_DIR.is_dir():
        return result

    html_files = sorted(
        OUTPUT_DIR.glob("SENTINEL_*.html"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if not html_files:
        _warnings += 1
        result.update(ok=False, detail="Aucun rapport HTML trouvé dans output/")
        log.warning("output/ : aucun rapport HTML trouvé")
        return result

    latest   = html_files[0]
    age_h    = (time.time() - latest.stat().st_mtime) / 3600
    size_kb  = latest.stat().st_size // 1024
    result.update(last_html=latest.name, age_h=round(age_h, 1), size_kb=size_kb, ok=True)

    if age_h > SEUIL_H_ALERTE + 2:
        _warnings += 1
        log.warning(f"output/ : dernier HTML vieux de {age_h:.1f}h — {latest.name}")
    else:
        log.info(f"output/ : {latest.name} ({age_h:.1f}h, {size_kb} KB) ✓")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 5 — Process zombie (WD-FIX15)
# ═════════════════════════════════════════════════════════════════════════════

def check_zombie_process() -> bool:
    """
    WD-FIX15 — Détecte si sentinel_main.py tourne depuis trop longtemps
    (blocage / deadlock). Normal < 30 min, zombie si > 90 min.
    """
    global _warnings
    try:
        result = subprocess.run(
            ["pgrep", "-f", "sentinel_main.py"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return False  # Pas de process actif — normal

        pids = result.stdout.strip().split()
        for pid in pids:
            try:
                stat_file = Path(f"/proc/{pid}/stat")
                if stat_file.exists():
                    # Lire le temps de démarrage du process
                    start_info = stat_file.read_text().split()
                    # /proc/uptime pour calculer l'âge
                    uptime    = float(Path("/proc/uptime").read_text().split()[0])
                    hz        = os.sysconf("SC_CLK_TCK")
                    starttime = int(start_info[21]) / hz
                    age_s     = uptime - starttime
                    age_min   = age_s / 60

                    if age_min > 90:
                        _warnings += 1
                        log.warning(
                            f"Process sentinel_main.py (PID {pid}) actif depuis "
                            f"{age_min:.0f} min — possible blocage"
                        )
                        return True
            except (OSError, IndexError, ValueError):
                pass
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass  # pgrep non disponible (Windows / macOS) — non bloquant
    return False


# ═════════════════════════════════════════════════════════════════════════════
# ROTATION DES LOGS (WD-FIX7 / CDC-4)
# ═════════════════════════════════════════════════════════════════════════════

def rotate_logs_if_needed() -> bool:
    """
    WD-FIX7 — Rotation sentinel.log si > LOG_MAX_MB.
    Garde les 4 dernières archives (.1 → .4).
    Retourne True si rotation effectuée.
    """
    if not LOGFILE.exists():
        return False

    size_mb = LOGFILE.stat().st_size // (1024 * 1024)
    if size_mb < LOG_MAX_MB and not FORCE_ROTATE:
        return False

    log.info(f"Rotation logs : sentinel.log = {size_mb} MB → rotation")
    try:
        # Décaler les archives : .3→.4, .2→.3, .1→.2
        for i in range(3, 0, -1):
            src = LOGFILE.with_suffix(f".log.{i}")
            dst = LOGFILE.with_suffix(f".log.{i + 1}")
            if src.exists():
                src.replace(dst)
        # Renommer le log courant en .1
        LOGFILE.replace(LOGFILE.with_suffix(".log.1"))
        # Créer un nouveau log vide
        LOGFILE.touch()
        log.info("Rotation logs terminée ✓")
        return True
    except OSError as e:
        log.warning(f"Rotation logs échouée : {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# PURGE OUTPUT ANCIENS RAPPORTS (CDC-4)
# ═════════════════════════════════════════════════════════════════════════════

def purge_old_reports(days: int = 30) -> int:
    """CDC-4 — Supprime les rapports HTML/PDF/MD de plus de N jours."""
    if not OUTPUT_DIR.is_dir():
        return 0
    cutoff  = time.time() - days * 86400
    removed = 0
    for pattern in ("*.html", "*.pdf", "*.md"):
        for f in OUTPUT_DIR.glob(pattern):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                pass
    if removed:
        log.info(f"Purge output/ : {removed} fichiers > {days}j supprimés (CDC-4)")
    return removed


# ═════════════════════════════════════════════════════════════════════════════
# AUTO-RESTART (WD-FIX8 — opt-in via --restart)
# ═════════════════════════════════════════════════════════════════════════════

def auto_restart_pipeline() -> bool:
    """
    WD-FIX8 — Relance sentinel_main.py en arrière-plan si --restart est passé.
    Sécurité : ne relance pas si un process est déjà actif.
    Retourne True si relance effectuée.
    """
    # Vérifier qu'aucun process sentinel n'est déjà actif
    try:
        result = subprocess.run(
            ["pgrep", "-f", "sentinel_main.py"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            log.warning("Auto-restart annulé : sentinel_main.py déjà en cours")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass  # pgrep indisponible — on continue prudemment

    log.info("Auto-restart : lancement de sentinel_main.py...")
    try:
        proc = subprocess.Popen(
            [sys.executable, "sentinel_main.py"],
            stdout=open("logs/sentinel_restart.log", "a"),
            stderr=subprocess.STDOUT,
            start_new_session=True,  # Détacher du process parent
        )
        log.info(f"Auto-restart : sentinel_main.py lancé (PID {proc.pid})")
        return True
    except (OSError, subprocess.SubprocessError) as e:
        log.error(f"Auto-restart échoué : {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# CONSTRUCTION DU CORPS EMAIL ENRICHI (WD-FIX10)
# ═════════════════════════════════════════════════════════════════════════════

def _build_alert_email(
    run_ok: bool,
    run_age_h: float,
    run_msg: str,
    db: dict,
    disk: dict,
    output: dict,
    zombie: bool,
    restarted: bool,
) -> tuple[str, str]:
    """WD-FIX10 — Construit sujet + corps email d'alerte enrichi."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Sujet
    if _critiques > 0:
        subject = f"🔴 SENTINEL WATCHDOG — Pipeline MORT depuis {run_age_h:.0f}h — {now_str}"
    else:
        subject = f"⚠️ SENTINEL WATCHDOG — Warning pipeline — {now_str}"

    # Corps texte brut\n    lines = [\n        f"SENTINEL v{VERSION} — Rapport Watchdog — {now_str}",\n        "=" * 60,\n        "",\n        f"ÉTAT PIPELINE  : {'❌ MORT' if not run_ok else '⚠️ WARNING'}",\n        f"Dernier run    : {run_msg}",\n        "",\n        "BASE DE DONNÉES :",\n        f"  • Rapports    : {db.get('n_reports', '?')}",\n        f"  • Seenhashes  : {db.get('n_seenhashes', '?')}",\n        f"  • Alertes     : {db.get('n_alertes', '?')}",\n        f"  • Taille DB   : {db.get('size_kb', '?')} KB",\n        f"  • Intégrité   : {'✓ OK' if db.get('ok') else '❌ CORROMPUE'}",\n        "",\n        "STOCKAGE :",\n        f"  • Disque libre : {disk.get('free_mb', '?')} MB ({disk.get('used_pct', '?')}% utilisé)",\n        f"  • Dernier HTML : {output.get('last_html', 'aucun')} ({output.get('age_h', '?')}h)",\n        "",\n    ]\n\n    if zombie:\n        lines += [\n            "⚠️  PROCESS ZOMBIE détecté : sentinel_main.py bloqué depuis > 90 min",\n            "",\n        ]\n\n    if restarted:\n        lines += [\n            "✅ Auto-restart déclenché : sentinel_main.py relancé automatiquement",\n            "",\n        ]\n\n    lines += [\n        "ACTIONS RECOMMANDÉES :",\n        f"  1. Vérifier les logs : tail -100 {LOGFILE}",\n        "  2. Vérifier le cron  : crontab -l | grep sentinel",\n        "  3. Test manuel       : python sentinel_main.py",\n        "  4. HealthCheck       : python health_check.py --offline",\n        f"  5. Si process zombie : kill $(pgrep -f sentinel_main.py)",\n        "",\n        f"Log watchdog : logs/watchdog.log",\n        f"État watchdog : {STATE_FILE}",\n    ]\n\n    body = "\n".join(lines)

    # Corps HTML simple
    html = f"""<html><body style="font-family:monospace;font-size:13px">
<h2 style="color:{'#c0392b' if _critiques else '#e67e22'}">
  {'🔴' if _critiques else '⚠️'} SENTINEL Watchdog — {now_str}
</h2>
<table border="1" cellpadding="6" style="border-collapse:collapse">
  <tr><th>Vérification</th><th>Résultat</th></tr>
  <tr><td>Pipeline</td><td style="color:{'red' if not run_ok else 'orange'}">{run_msg}</td></tr>
  <tr><td>Base SQLite</td><td style="color:{'red' if not db.get('ok') else 'green'}">
    {'❌ CORROMPUE' if not db.get('ok') else f"✓ {db.get('n_reports','?')} rapports"}</td></tr>
  <tr><td>Disque</td><td style="color:{'red' if disk.get('free_mb',999)<DISK_CRIT_MB else 'green'}">
    {disk.get('free_mb','?')} MB libres</td></tr>
  <tr><td>Dernier HTML</td><td>{output.get('last_html','aucun')} ({output.get('age_h','?')}h)</td></tr>
  {'<tr><td>Process</td><td style="color:orange">⚠️ Process zombie détecté</td></tr>' if zombie else ''}
  {'<tr><td>Auto-restart</td><td style="color:green">✅ Relancé automatiquement</td></tr>' if restarted else ''}
</table>
<pre style="background:#f5f5f5;padding:10px;margin-top:16px">{chr(10).join(lines[-8:])}</pre>
</body></html>"""

    return subject, body, html


# ═════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ QUOTIDIEN "PIPELINE VIVANT" (WD-FIX11)
# ═════════════════════════════════════════════════════════════════════════════\n\ndef send_daily_summary(run_age_h: float, db: dict, disk: dict) -> None:\n    """\n    WD-FIX11 — Envoie un email de confirmation quotidienne si tout est OK.\n    Cron : 30 8 * * * python watchdog.py --summary\n    """\n    if not SMTP_USER or not SMTP_PASS:\n        return\n\n    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")\n    subject = f"✅ SENTINEL — Pipeline opérationnel — {now_str}"\n\n    body = (\n        f"SENTINEL v{VERSION} — Résumé quotidien — {now_str}\n"\n        f"{'=' * 50}\n\n"\n        f"Pipeline   : ✓ Dernier run il y a {run_age_h:.1f}h\n"\n        f"Rapports   : {db.get('n_reports', '?')} dans SQLite\n"\n        f"Seenhashes : {db.get('n_seenhashes', '?')} articles dédupliqués\n"\n        f"Alertes    : {db.get('n_alertes', '?')} actives\n"\n        f"Disque     : {disk.get('free_mb', '?')} MB libres ({disk.get('used_pct', '?')}% utilisé)\n"\n        f"DB intègre : {'Oui' if db.get('ok') else 'NON — vérifier !'}\n"\n    )

    send_alert(subject, body)
    log.info("Résumé quotidien envoyé ✓")


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def run_watchdog() -> int:
    """
    Lance la vérification complète du pipeline SENTINEL.
    Retourne le code de sortie : 0=OK, 1=warnings, 2=critiques.
    """
    global _warnings, _critiques

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    log.info("=" * 60)
    log.info(f"SENTINEL v{VERSION} — Watchdog démarré — {now_str}")
    log.info("=" * 60)

    state = _load_state()

    # ── Rotation logs proactive ───────────────────────────────────────────────
    rotate_logs_if_needed()

    # ── Purge output/ (CDC-4) ─────────────────────────────────────────────────
    purge_old_reports(days=30)

    # ── Vérifications ────────────────────────────────────────────────────────
    run_ok, run_age_h, run_msg  = check_last_run()
    db_result                   = check_sqlite_health()
    disk_result                 = check_disk_space()
    output_result               = check_output_fresh()
    zombie                      = check_zombie_process()

    # ── Décision alerte ──────────────────────────────────────────────────────
    log.info(f"Résumé : {_critiques} critique(s), {_warnings} warning(s)")

    restarted = False

    if _critiques > 0:
        # Alerte critique
        if _can_send_alert(state, "pipeline_mort"):
            subject, body, html = _build_alert_email(
                run_ok, run_age_h, run_msg,
                db_result, disk_result, output_result,
                zombie, restarted=False
            )
            # Auto-restart avant de signaler (WD-FIX8)
            if AUTO_RESTART and not run_ok:
                restarted = auto_restart_pipeline()
                if restarted:
                    subject, body, html = _build_alert_email(
                        run_ok, run_age_h, run_msg,
                        db_result, disk_result, output_result,
                        zombie, restarted=True
                    )
            send_alert(subject, body, html)
            _mark_alert_sent(state, "pipeline_mort")
        else:
            log.info("Anti-spam : alerte 'pipeline_mort' déjà envoyée dans les 4h — skip")

    elif _warnings > 0:
        # Warning seulement\n        if _can_send_alert(state, "pipeline_warn"):\n            subject = f"⚠️ SENTINEL — Warning pipeline — {now_str}"\n            body    = f"SENTINEL Watchdog\n\n{run_msg}\n\nDisque : {disk_result.get('free_mb','?')} MB libres"\n            send_alert(subject, body)\n            _mark_alert_sent(state, "pipeline_warn")\n        else:
            log.info("Anti-spam : alerte 'pipeline_warn' déjà envoyée dans les 4h — skip")

    else:
        log.info(f"Pipeline OK ✓  |  {run_msg}")

    # ── Résumé quotidien (WD-FIX11) ───────────────────────────────────────────
    if FORCE_SUMMARY or (
        run_ok
        and _critiques == 0
        and _warnings == 0
        and (
            not state.get("last_summary")
            or (datetime.now(timezone.utc) - datetime.fromisoformat(
                state["last_summary"]
            )).total_seconds() > 20 * 3600
        )
    ):
        send_daily_summary(run_age_h, db_result, disk_result)
        state["last_summary"] = datetime.now(timezone.utc).isoformat()

    # ── Sauvegarde état watchdog ─────────────────────────────────────────────
    state.setdefault("runs", []).append({
        "ts":       datetime.now(timezone.utc).isoformat(),
        "ok":       run_ok,
        "age_h":    round(run_age_h, 1) if run_age_h != float("inf") else None,
        "warnings": _warnings,
        "critiques": _critiques,
    })
    # Garder seulement les 30 derniers runs
    state["runs"] = state["runs"][-30:]
    _save_state(state)

    log.info("=" * 60)

    # ── Exit codes CI/CD (WD-FIX13) ──────────────────────────────────────────
    if _critiques > 0:
        return 2
    if _warnings > 0:
        return 1
    return 0


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        exit_code = run_watchdog()
        sys.exit(exit_code)
    except Exception as fatal:
        # WD-FIX12 / R6A3-NEW-1 — exception globale reraisée pour que
        # cron / supervisor détecte l'échec (jamais silencieux)
        log.critical(f"FATAL watchdog.py : {fatal}", exc_info=True)
        raise