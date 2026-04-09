#!/usr/bin/env python3
# watchdog.py — SENTINEL v3.43 — Surveillance cron & auto-recovery
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 :
# WD-FIX1  — logging structuré (zéro print())
# WD-FIX2  — vérification Python ≥ 3.10 en tête
# WD-FIX3  — double source de vérité : log file + SQLite
# WD-FIX4  — anti-spam alertes : 1 alerte max / 4h
# WD-FIX5  — SMTP retry exponentiel : 3 tentatives (30s/60s/120s)
# WD-FIX6  — vérification espace disque + alerte seuil critique
# WD-FIX7  — rotation automatique sentinel.log > 50 MB
# WD-FIX8  — auto-restart sentinel_main.py si cron mort (opt-in)
# WD-FIX9  — rapport JSON état watchdog dans logs/watchdog_state.json
# WD-FIX10 — alerte email enrichie : DB stats, disk, last run detail
# WD-FIX11 — email résumé quotidien "pipeline vivant" (8h30)
# WD-FIX12 — exception globale reraisée pour que cron détecte l'échec
# WD-FIX13 — exit code 0=OK / 1=warning / 2=critique
# WD-FIX14 — vérification output/ dernier HTML généré
# WD-FIX15 — détection process zombie (sentinel_main bloqué)
#
# Corrections v3.43 :
# WD-FIX16 — parsing log JSON structuré (format {"ts":...,"msg":...})
# WD-FIX17 — tokens succès : "CYCLE TERMINE EN" / "RAPPORT MENSUEL TERMINE EN"
# WD-FIX18 — datetime naive/aware unifiés → timezone.utc partout
# WD-FIX19 — open() sans close() dans auto_restart_pipeline() corrigé
# WD-FIX20 — glob pattern corrigé : "SENTINEL_*.html"
# WD-FIX21 — VERSION synchronisée à 3.43
# WD-FIX22 — annotation retour _build_alert_email : tuple[str,str,str]
# WD-FIX23 — seuil zombie étendu à 180 min le 1er du mois
# WD-FIX24 — compteurs _warnings/_critiques réinitialisés en début run
# WD-FIX25 — health endpoint HTTP vérifié (3e source de vérité)
# WD-FIX26 — log explicite après rotation (évite faux positif)
# WD-FIX27 — purge output/ renommée _purge_output_files()
# WD-FIX28 — exit_code sauvegardé dans state["runs"]
#
# Corrections v3.43-rev1 :
# WD-FIX29 — json.loads isolé : except JSONDecodeError/UnicodeDecodeError
# WD-FIX30 — _tail_lines() via collections.deque (sans charger le fichier en RAM)
#
# Corrections v3.43-rev2 :
# WD-FIX31 — SQLite en lecture seule (uri=True, mode=ro)
# WD-FIX32 — ssl.create_default_context() sur starttls() (anti-MITM)
# WD-FIX33 — SENTINEL_NO_ROTATE=1 désactive la rotation interne
#
# Corrections v3.43-win (compatibilité Windows/WSL2) :
# WD-FIX34 — psutil remplace pgrep + /proc : cross-platform (Linux/macOS/Windows)
# WD-FIX35 — DETACHED_PROCESS + CREATE_NO_WINDOW sur Windows
# WD-FIX36 — pythonw.exe sur Windows (pas de fenêtre console)
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python watchdog.py               Vérification complète
#   python watchdog.py --restart     Auto-restart si pipeline mort
#   python watchdog.py --summary     Force l'envoi du résumé quotidien
#   python watchdog.py --rotate-logs Force la rotation des logs
# ─────────────────────────────────────────────────────────────────────────────
# Planification recommandée :
#   Linux/macOS/WSL2 (cron) :
#     0  6  * * * cd ~/sentinel && python3 sentinel_main.py >> logs/sentinel.log 2>&1
#     30 6  * * * cd ~/sentinel && python3 watchdog.py >> logs/watchdog.log 2>&1
#     30 8  * * * cd ~/sentinel && python3 watchdog.py --summary >> logs/watchdog.log 2>&1
#
#   Windows natif (Task Scheduler) :
#     schtasks /create /tn "SentinelMain"     /tr "python C:sentinelsentinel_main.py" /sc daily /st 06:00
#     schtasks /create /tn "SentinelWatchdog" /tr "python C:sentinelwatchdog.py"      /sc daily /st 06:30
#     schtasks /create /tn "SentinelSummary"  /tr "python C:sentinelwatchdog.py --summary" /sc daily /st 08:30
# ─────────────────────────────────────────────────────────────────────────────
# Codes de sortie :
#   0 — Pipeline OK
#   1 — Warning (disque bas, log ancien, mais pas critique)
#   2 — Critique (pipeline mort, DB corrompue, espace disque insuffisant)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import collections
import json
import logging
import os
import platform
import re
import shutil
import smtplib
import sqlite3
import ssl
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# ── psutil (WD-FIX34 — cross-platform process detection) ─────────────────────
try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

# ── Charger .env si disponible ────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Python ≥ 3.10 (WD-FIX2) ──────────────────────────────────────────────────
if sys.version_info < (3, 10):
    sys.stderr.write(
        f"WATCHDOG CRITIQUE : Python {sys.version_info.major}.{sys.version_info.minor} < 3.10
"
        "Requis : Python 3.10+ → sudo apt install python3.10
"
    )
    sys.exit(2)

# ── Logging structuré (WD-FIX1) ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sentinel.watchdog")


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTES & CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

VERSION = "3.43"

# Seuils temporels
SEUIL_H_ALERTE = int(os.environ.get("SENTINEL_MAX_SILENCE", "26"))
SEUIL_H_WARN   = 20
ANTI_SPAM_H    = 4
LOG_MAX_MB     = 50
DISK_CRIT_MB   = 200
DISK_WARN_MB   = 500

# WD-FIX17 — tokens alignés sur sentinel_main.py v3.43
TOKENS_SUCCES = ("CYCLE TERMINE EN", "RAPPORT MENSUEL TERMINE EN")

# Chemins
LOGFILE    = Path(os.environ.get("SENTINEL_LOG", "logs/sentinel.log"))
STATE_FILE = Path("logs/watchdog_state.json")
DB_PATH    = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
OUTPUT_DIR = Path("output")

# SMTP
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
ALERT_TO  = os.environ.get("REPORT_EMAIL", SMTP_USER)

# Health endpoint (WD-FIX25)
HEALTH_PORT = int(os.environ.get("SENTINEL_HEALTH_PORT", "8765"))

# WD-FIX33 — désactive rotation interne si logrotate est actif
# Usage : SENTINEL_NO_ROTATE=1 python watchdog.py
DISABLE_INTERNAL_ROTATION = os.environ.get("SENTINEL_NO_ROTATE", "0") == "1"

# Flags CLI
AUTO_RESTART  = "--restart"      in sys.argv
FORCE_SUMMARY = "--summary"      in sys.argv
FORCE_ROTATE  = "--rotate-logs"  in sys.argv

# Compteurs — réinitialisés dans run_watchdog() (WD-FIX24)
_warnings  = 0
_critiques = 0


# ═════════════════════════════════════════════════════════════════════════════
# UTILITAIRE — Lecture des N dernières lignes sans charger le fichier en RAM
# ═════════════════════════════════════════════════════════════════════════════

def _tail_lines(path: Path, n: int = 2000) -> list[str]:
    """
    WD-FIX30 — collections.deque : évite d'allouer ~50 MB pour 2000 lignes.
    """
    try:
        with open(path, "rb") as f:
            dq = collections.deque(f, maxlen=n)
        return [line.decode("utf-8", errors="ignore").rstrip() for line in dq]
    except OSError as e:
        log.warning(f"_tail_lines : impossible de lire {path} : {e}")
        return []


# ═════════════════════════════════════════════════════════════════════════════
# ÉTAT PERSISTANT (WD-FIX4 / WD-FIX9)
# ═════════════════════════════════════════════════════════════════════════════

def _load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"last_alert": {}, "last_summary": None, "runs": []}


def _save_state(state: dict[str, Any]) -> None:
    """Sauvegarde atomique (write-then-rename)."""
    Path("logs").mkdir(exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except OSError as e:
        log.warning(f"Impossible de sauvegarder l'état watchdog : {e}")


def _can_send_alert(state: dict, alert_key: str) -> bool:
    """WD-FIX4 — Anti-spam : 1 alerte max par tranche de ANTI_SPAM_H heures."""
    last = state.get("last_alert", {}).get(alert_key)
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last_dt).total_seconds() > ANTI_SPAM_H * 3600
    except ValueError:
        return True


def _mark_alert_sent(state: dict, alert_key: str) -> None:
    state.setdefault("last_alert", {})[alert_key] = datetime.now(timezone.utc).isoformat()


# ═════════════════════════════════════════════════════════════════════════════
# ENVOI EMAIL (WD-FIX5 — retry exponentiel + WD-FIX32 — SSL strict)
# ═════════════════════════════════════════════════════════════════════════════

def send_alert(subject: str, body: str, html_body: str = "") -> bool:
    """
    WD-FIX5  — Retry exponentiel 3 tentatives : 30s / 60s / 120s.
    WD-FIX32 — ssl.create_default_context() force la validation du certificat.
    """
    if not SMTP_USER or not SMTP_PASS:
        log.warning(f"Email non configuré — alerte non envoyée : {subject}")
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
                server.starttls(context=ssl.create_default_context())  # WD-FIX32
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(SMTP_USER, ALERT_TO.split(","), msg.as_string())
            log.info(f"Email envoyé → {ALERT_TO} | {subject}")
            return True
        except smtplib.SMTPAuthenticationError as e:
            log.error(f"SMTP auth échouée (tentative {attempt + 1}/3) : {e}")
            break
        except (smtplib.SMTPException, OSError) as e:
            delay = 30 * (2 ** attempt)
            log.warning(f"SMTP tentative {attempt + 1}/3 échouée : {e} — retry dans {delay}s")
            if attempt < 2:
                time.sleep(delay)

    log.error(f"SMTP : 3 tentatives épuisées — alerte non envoyée : {subject}")
    return False


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 0 — Health endpoint HTTP (WD-FIX25 + WD-FIX29)
# ═════════════════════════════════════════════════════════════════════════════

def check_health_endpoint() -> dict[str, Any]:
    """
    WD-FIX25 — Interroge http://localhost:{HEALTH_PORT}/health.
    WD-FIX29 — json.loads isolé avec except JSONDecodeError/UnicodeDecodeError.
    """
    result: dict[str, Any] = {
        "ok": False, "status": "unreachable", "version": None, "detail": ""
    }
    url = f"http://localhost:{HEALTH_PORT}/health"

    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            raw = resp.read()

        # WD-FIX29 — exceptions spécifiques, pas un except Exception générique
        try:
            data = json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError as e:
            result["detail"] = f"Health endpoint : réponse non-UTF8 — {e}"
            log.warning(result["detail"])
            return result
        except json.JSONDecodeError as e:
            result["detail"] = f"Health endpoint : JSON invalide — {e}"
            log.warning(result["detail"])
            return result

        result.update(
            ok      = data.get("status") == "ok",
            status  = data.get("status", "unknown"),
            version = data.get("version"),
            detail  = f"Health OK (v{data.get('version','?')}, ts={data.get('ts','?')})",
        )
        log.info(f"Health endpoint : {result['detail']} ✓")

    except urllib.error.URLError:
        result["detail"] = (
            f"Health endpoint injoignable (port {HEALTH_PORT})"
            " — process non actif ou port fermé"
        )
        log.info(f"Health endpoint : {result['detail']}")
    except OSError as e:
        result["detail"] = f"Health endpoint erreur réseau : {e}"
        log.warning(result["detail"])

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 1 — Dernier run pipeline (WD-FIX3/16/17/18/30)
# ═════════════════════════════════════════════════════════════════════════════

def check_last_run() -> tuple[bool, float, str]:
    """
    WD-FIX3  — Triple source : log file + SQLite + health endpoint.
    WD-FIX16 — Parsing format JSON structuré sentinel_main.py v3.43.
    WD-FIX17 — Tokens : "CYCLE TERMINE EN" / "RAPPORT MENSUEL TERMINE EN".
    WD-FIX18 — Toutes comparaisons datetime en UTC aware.
    WD-FIX30 — _tail_lines() sans charger le fichier entier.
    WD-FIX31 — SQLite en lecture seule (mode=ro).
    """
    global _warnings, _critiques

    age_h_log: float        = float("inf")
    age_h_db:  float        = float("inf")
    last_dt_log: datetime | None = None
    last_dt_db:  datetime | None = None
    now_utc = datetime.now(timezone.utc)

    # ── Source 1 : log file ───────────────────────────────────────────────────
    if LOGFILE.exists():
        lines = _tail_lines(LOGFILE, n=2000)
        for line in reversed(lines):
            if not any(tok in line for tok in TOKENS_SUCCES):
                continue
            ts_match = re.search(r'"ts"s*:s*"([^"]+)"', line)
            if ts_match:
                try:
                    last_dt_log = datetime.fromisoformat(ts_match.group(1))
                    if last_dt_log.tzinfo is None:
                        last_dt_log = last_dt_log.replace(tzinfo=timezone.utc)
                    age_h_log = (now_utc - last_dt_log).total_seconds() / 3600
                    break
                except ValueError:
                    continue
    else:
        log.warning(f"Fichier log absent : {LOGFILE}")

    # ── Source 2 : SQLite (WD-FIX31 — lecture seule) ─────────────────────────
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
            row  = conn.execute(
                "SELECT date FROM reports ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row:
                last_dt_db = datetime.fromisoformat(row[0])
                if last_dt_db.tzinfo is None:
                    last_dt_db = last_dt_db.replace(tzinfo=timezone.utc)
                age_h_db = (now_utc - last_dt_db).total_seconds() / 3600
        except sqlite3.Error as e:
            log.warning(f"SQLite lecture dernier rapport : {e}")

    # ── Décision : source la plus récente ─────────────────────────────────────
    age_h   = min(age_h_log, age_h_db)
    source  = "log" if age_h_log <= age_h_db else "SQLite"
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
# VÉRIFICATION 2 — Santé SQLite (WD-FIX3 + WD-FIX31)
# ═════════════════════════════════════════════════════════════════════════════

def check_sqlite_health() -> dict[str, Any]:
    """WD-FIX3/31 — Intégrité SQLite en lecture seule, stats pour l'email."""
    global _warnings, _critiques
    result: dict[str, Any] = {"ok": True, "detail": ""}

    if not DB_PATH.exists():
        result.update(ok=True, detail="DB absente (1er déploiement)")
        return result

    try:
        # WD-FIX31 — lecture seule : jamais bloquant pour sentinel_main.py
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)

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

        # Comptages
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

        result["size_kb"] = DB_PATH.stat().st_size // 1024
        conn.close()
        result.update(ok=True, detail=f"{result.get('n_reports', 0)} rapports")

    except sqlite3.Error as e:
        _critiques += 1
        result.update(ok=False, detail=f"Erreur connexion SQLite : {e}")
        log.error(f"SQLite connexion : {e}")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 3 — Espace disque (WD-FIX6)
# ═════════════════════════════════════════════════════════════════════════════

def check_disk_space() -> dict[str, Any]:
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
        result = {"free_mb": -1, "used_pct": -1, "ok": True}
    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 4 — Output directory (WD-FIX14 + WD-FIX20)
# ═════════════════════════════════════════════════════════════════════════════

def check_output_fresh() -> dict[str, Any]:
    """
    WD-FIX14 — Vérifie que le dernier rapport HTML a bien été généré.
    WD-FIX20 — Glob pattern corrigé : "SENTINEL_*.html".
    """
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

    latest  = html_files[0]
    age_h   = (time.time() - latest.stat().st_mtime) / 3600
    size_kb = latest.stat().st_size // 1024
    result.update(last_html=latest.name, age_h=round(age_h, 1), size_kb=size_kb, ok=True)

    if age_h > SEUIL_H_ALERTE + 2:
        _warnings += 1
        log.warning(f"output/ : dernier HTML vieux de {age_h:.1f}h — {latest.name}")
    else:
        log.info(f"output/ : {latest.name} ({age_h:.1f}h, {size_kb} KB) ✓")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 5 — Process zombie (WD-FIX15 + WD-FIX23 + WD-FIX34)
# ═════════════════════════════════════════════════════════════════════════════

def check_zombie_process() -> bool:
    """
    WD-FIX15 — Détecte si sentinel_main.py tourne depuis trop longtemps.
    WD-FIX23 — Seuil étendu à 180 min le 1er du mois.
    WD-FIX34 — Cross-platform via psutil. Fallback pgrep+/proc sur Linux.
    """
    global _warnings

    zombie_threshold_min = 180 if datetime.now().day == 1 else 90

    # ── Branche psutil (Linux / macOS / Windows) ──────────────────────────────
    if _PSUTIL_AVAILABLE:
        try:
            for proc in psutil.process_iter(["pid", "cmdline", "create_time"]):
                try:
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    if "sentinel_main.py" not in cmdline:
                        continue
                    age_min = (time.time() - proc.info["create_time"]) / 60
                    if age_min > zombie_threshold_min:
                        _warnings += 1
                        log.warning(
                            f"Process sentinel_main.py (PID {proc.info['pid']}) actif "
                            f"depuis {age_min:.0f} min — possible blocage "
                            f"(seuil : {zombie_threshold_min} min)"
                        )
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception as e:
            log.warning(f"check_zombie (psutil) : {e}")
        return False

    # ── Fallback pgrep + /proc (Linux uniquement) ─────────────────────────────
    try:
        result = subprocess.run(
            ["pgrep", "-f", "sentinel_main.py"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return False

        for pid in result.stdout.strip().split():
            try:
                stat_file   = Path(f"/proc/{pid}/stat")
                uptime_file = Path("/proc/uptime")
                if stat_file.exists() and uptime_file.exists():
                    start_info = stat_file.read_text().split()
                    uptime     = float(uptime_file.read_text().split()[0])
                    hz         = os.sysconf("SC_CLK_TCK")
                    starttime  = int(start_info[21]) / hz
                    age_min    = (uptime - starttime) / 60
                    if age_min > zombie_threshold_min:
                        _warnings += 1
                        log.warning(
                            f"Process sentinel_main.py (PID {pid}) actif depuis "
                            f"{age_min:.0f} min — possible blocage "
                            f"(seuil : {zombie_threshold_min} min)"
                        )
                        return True
            except (OSError, IndexError, ValueError):
                pass
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    return False


# ═════════════════════════════════════════════════════════════════════════════
# ROTATION DES LOGS (WD-FIX7 + WD-FIX26 + WD-FIX33)
# ═════════════════════════════════════════════════════════════════════════════

def rotate_logs_if_needed() -> bool:
    """
    WD-FIX7  — Rotation si > LOG_MAX_MB. Garde 4 archives (.1 → .4).
    WD-FIX26 — Log explicite post-rotation pour éviter faux positif.
    WD-FIX33 — Bypass si SENTINEL_NO_ROTATE=1 (conflit logrotate).
    """
    # WD-FIX33
    if DISABLE_INTERNAL_ROTATION:
        log.info("Rotation interne désactivée (SENTINEL_NO_ROTATE=1) — logrotate actif")
        return False

    if not LOGFILE.exists():
        return False

    size_mb = LOGFILE.stat().st_size // (1024 * 1024)
    if size_mb < LOG_MAX_MB and not FORCE_ROTATE:
        return False

    log.info(f"Rotation logs : sentinel.log = {size_mb} MB → rotation")
    try:
        for i in range(3, 0, -1):
            src = LOGFILE.with_suffix(f".log.{i}")
            dst = LOGFILE.with_suffix(f".log.{i + 1}")
            if src.exists():
                src.replace(dst)
        LOGFILE.replace(LOGFILE.with_suffix(".log.1"))
        LOGFILE.touch()
        log.info(
            "Rotation terminée ✓ — "
            "check_last_run() utilisera SQLite comme source principale ce cycle"
        )
        return True
    except OSError as e:
        log.warning(f"Rotation logs échouée : {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# PURGE OUTPUT (WD-FIX27)
# ═════════════════════════════════════════════════════════════════════════════

def _purge_output_files(days: int = 30) -> int:
    """
    WD-FIX27 — Renommée _purge_output_files() pour éviter conflit avec
               purge_old_reports() de report_builder.py.
    """
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
        log.info(f"Purge output/ : {removed} fichiers > {days}j supprimés")
    return removed


# ═════════════════════════════════════════════════════════════════════════════
# AUTO-RESTART (WD-FIX8 + WD-FIX19 + WD-FIX34 + WD-FIX35 + WD-FIX36)
# ═════════════════════════════════════════════════════════════════════════════

def _is_sentinel_running() -> bool:
    """
    WD-FIX34 — Vérifie si sentinel_main.py tourne. Cross-platform.
    """
    if _PSUTIL_AVAILABLE:
        try:
            for proc in psutil.process_iter(["cmdline"]):
                try:
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    if "sentinel_main.py" in cmdline:
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception:
            pass
        return False
    else:
        try:
            r = subprocess.run(
                ["pgrep", "-f", "sentinel_main.py"],
                capture_output=True, text=True, timeout=5
            )
            return r.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False


def auto_restart_pipeline() -> bool:
    """
    WD-FIX8  — Relance sentinel_main.py en arrière-plan.
    WD-FIX19 — File handle fermé dans le parent après Popen.
    WD-FIX34 — Vérification process via _is_sentinel_running().
    WD-FIX35 — DETACHED_PROCESS + CREATE_NO_WINDOW sur Windows.
    WD-FIX36 — pythonw.exe sur Windows (pas de fenêtre console).
    """
    if _is_sentinel_running():
        log.warning("Auto-restart annulé : sentinel_main.py déjà en cours")
        return False

    log.info("Auto-restart : lancement de sentinel_main.py...")
    try:
        log_file   = open("logs/sentinel_restart.log", "a")
        is_windows = platform.system() == "Windows"

        if is_windows:
            # WD-FIX36 — pythonw.exe : pas de fenêtre console
            python_exec = Path(sys.executable).parent / "pythonw.exe"
            if not python_exec.exists():
                python_exec = Path(sys.executable)
            # WD-FIX35 — flags Windows
            DETACHED_PROCESS = 0x00000008
            CREATE_NO_WINDOW = 0x08000000
            proc = subprocess.Popen(
                [str(python_exec), "sentinel_main.py"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=DETACHED_PROCESS | CREATE_NO_WINDOW,
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, "sentinel_main.py"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        log_file.close()  # WD-FIX19
        log.info(f"Auto-restart : sentinel_main.py lancé (PID {proc.pid})")
        return True
    except (OSError, subprocess.SubprocessError) as e:
        log.error(f"Auto-restart échoué : {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# CONSTRUCTION EMAIL ALERTE (WD-FIX10 + WD-FIX22)
# ═════════════════════════════════════════════════════════════════════════════

def _build_alert_email(
    run_ok:    bool,
    run_age_h: float,
    run_msg:   str,
    db:        dict,
    disk:      dict,
    output:    dict,
    health:    dict,
    zombie:    bool,
    restarted: bool,
) -> tuple[str, str, str]:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    subject = (
        f"🔴 SENTINEL WATCHDOG — Pipeline MORT depuis {run_age_h:.0f}h — {now_str}"
        if _critiques > 0 else
        f"⚠️ SENTINEL WATCHDOG — Warning pipeline — {now_str}"
    )

    lines = [
        f"SENTINEL v{VERSION} — Rapport Watchdog — {now_str}",
        "=" * 60,
        "",
        f"ÉTAT PIPELINE : {'❌ MORT' if not run_ok else '⚠️ WARNING'}",
        f"Dernier run   : {run_msg}",
        f"Health HTTP   : {health.get('detail', 'non vérifié')}",
        "",
        "BASE DE DONNÉES :",
        f"  • Rapports     : {db.get('n_reports', '?')}",
        f"  • Seenhashes   : {db.get('n_seenhashes', '?')}",
        f"  • Alertes      : {db.get('n_alertes', '?')}",
        f"  • Taille DB    : {db.get('size_kb', '?')} KB",
        f"  • Intégrité    : {'✓ OK' if db.get('ok') else '❌ CORROMPUE'}",
        "",
        "STOCKAGE :",
        f"  • Disque libre : {disk.get('free_mb', '?')} MB ({disk.get('used_pct', '?')}% utilisé)",
        f"  • Dernier HTML : {output.get('last_html', 'aucun')} ({output.get('age_h', '?')}h)",
        "",
    ]

    if zombie:
        lines += ["⚠️ PROCESS ZOMBIE détecté : sentinel_main.py bloqué", ""]
    if restarted:
        lines += ["✅ Auto-restart déclenché : sentinel_main.py relancé", ""]

    lines += [
        "ACTIONS RECOMMANDÉES :",
        f"  1. Logs pipeline  : tail -100 {LOGFILE}",
        "  2. Cron           : crontab -l | grep sentinel",
        "  3. Test manuel    : python sentinel_main.py",
        f"  4. Kill zombie    : kill $(pgrep -f sentinel_main.py)",
        "",
        f"Log watchdog  : logs/watchdog.log",
        f"État watchdog : {STATE_FILE}",
    ]

    body = "
".join(lines)

    sqlite_cell = (
        "❌ CORROMPUE" if not db.get("ok")
        else f"✓ {db.get('n_reports', '?')} rapports"
    )

    html = (
        f'<html><body style="font-family:monospace;font-size:13px">'
        f'<h2 style="color:{"#c0392b" if _critiques > 0 else "#e67e22"}">{subject}</h2>'
        f'<table border="1" cellpadding="6" cellspacing="0">'
        f"<tr><th>Vérification</th><th>Résultat</th></tr>"
        f"<tr><td>Pipeline</td><td>{run_msg}</td></tr>"
        f"<tr><td>Health HTTP</td><td>{health.get('detail', '?')}</td></tr>"
        f"<tr><td>SQLite</td><td>{sqlite_cell}</td></tr>"
        f"<tr><td>Disque</td><td>{disk.get('free_mb', '?')} MB libres</td></tr>"
        f"<tr><td>Dernier HTML</td><td>{output.get('last_html', 'aucun')} ({output.get('age_h', '?')}h)</td></tr>"
        f"<tr><td>Process zombie</td><td>{'⚠️ Détecté' if zombie else '✓ OK'}</td></tr>"
        f"<tr><td>Auto-restart</td><td>{'✅ Relancé' if restarted else '—'}</td></tr>"
        f"</table>"
        f'<pre style="margin-top:16px;background:#f4f4f4;padding:12px">'
        + "
".join(lines[-8:])
        + "</pre></body></html>"
    )

    return subject, body, html


# ═════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ QUOTIDIEN (WD-FIX11)
# ═════════════════════════════════════════════════════════════════════════════

def send_daily_summary(run_age_h: float, db: dict, disk: dict, health: dict) -> None:
    if not SMTP_USER or not SMTP_PASS:
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"✅ SENTINEL — Pipeline opérationnel — {now_str}"
    body = (
        f"SENTINEL v{VERSION} — Résumé quotidien — {now_str}
"
        f"{'=' * 50}

"
        f"Pipeline     : ✓ Dernier run il y a {run_age_h:.1f}h
"
        f"Health HTTP  : {health.get('detail', 'non vérifié')}
"
        f"Rapports     : {db.get('n_reports', '?')} dans SQLite
"
        f"Seenhashes   : {db.get('n_seenhashes', '?')} articles dédupliqués
"
        f"Alertes      : {db.get('n_alertes', '?')} actives
"
        f"Disque       : {disk.get('free_mb', '?')} MB libres ({disk.get('used_pct', '?')}% utilisé)
"
        f"DB intègre   : {'Oui ✓' if db.get('ok') else 'NON — vérifier !'}
"
    )
    send_alert(subject, body)
    log.info("Résumé quotidien envoyé ✓")


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def run_watchdog() -> int:
    global _warnings, _critiques

    # WD-FIX24 — réinitialisation des compteurs
    _warnings  = 0
    _critiques = 0

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    log.info("=" * 60)
    log.info(f"SENTINEL v{VERSION} — Watchdog démarré — {now_str}")
    log.info("=" * 60)

    state = _load_state()

    # Rotation logs
    rotated = rotate_logs_if_needed()
    if rotated:
        log.info("Log roté — source principale ce cycle : SQLite")

    # Purge output/
    _purge_output_files(days=30)

    # Vérifications
    run_ok,   run_age_h, run_msg = check_last_run()
    db_result                    = check_sqlite_health()
    disk_result                  = check_disk_space()
    output_result                = check_output_fresh()
    health_result                = check_health_endpoint()
    zombie                       = check_zombie_process()

    log.info(f"Bilan watchdog : {_critiques} critique(s), {_warnings} warning(s)")
    restarted = False

    if _critiques > 0:
        if _can_send_alert(state, "pipeline_mort"):
            if AUTO_RESTART and not run_ok:
                restarted = auto_restart_pipeline()
            subject, body, html = _build_alert_email(
                run_ok, run_age_h, run_msg,
                db_result, disk_result, output_result,
                health_result, zombie, restarted,
            )
            send_alert(subject, body, html)
            _mark_alert_sent(state, "pipeline_mort")
        else:
            log.info("Anti-spam : alerte 'pipeline_mort' déjà envoyée dans les 4h — skip")

    elif _warnings > 0:
        if _can_send_alert(state, "pipeline_warn"):
            subject = f"⚠️ SENTINEL — Warning pipeline — {now_str}"
            body    = (
                f"SENTINEL Watchdog v{VERSION}

"
                f"{run_msg}

"
                f"Disque : {disk_result.get('free_mb', '?')} MB libres
"
                f"Health : {health_result.get('detail', '?')}
"
            )
            send_alert(subject, body)
            _mark_alert_sent(state, "pipeline_warn")
        else:
            log.info("Anti-spam : alerte 'pipeline_warn' déjà envoyée dans les 4h — skip")
    else:
        log.info(f"Pipeline OK ✓ | {run_msg}")

    # Résumé quotidien (WD-FIX11)
    last_summary    = state.get("last_summary")
    summary_elapsed = float("inf")
    if last_summary:
        try:
            ls_dt = datetime.fromisoformat(last_summary)
            if ls_dt.tzinfo is None:
                ls_dt = ls_dt.replace(tzinfo=timezone.utc)
            summary_elapsed = (datetime.now(timezone.utc) - ls_dt).total_seconds()
        except ValueError:
            pass

    if FORCE_SUMMARY or (
        run_ok and _critiques == 0 and _warnings == 0
        and summary_elapsed > 20 * 3600
    ):
        send_daily_summary(run_age_h, db_result, disk_result, health_result)
        state["last_summary"] = datetime.now(timezone.utc).isoformat()

    # Sauvegarde état (WD-FIX9 + WD-FIX28)
    exit_code = 2 if _critiques > 0 else (1 if _warnings > 0 else 0)

    state.setdefault("runs", []).append({
        "ts":        datetime.now(timezone.utc).isoformat(),
        "ok":        run_ok,
        "age_h":     round(run_age_h, 1) if run_age_h != float("inf") else None,
        "warnings":  _warnings,
        "critiques": _critiques,
        "exit_code": exit_code,
    })
    state["runs"] = state["runs"][-30:]
    _save_state(state)

    log.info(f"Watchdog terminé — exit code {exit_code}")
    log.info("=" * 60)
    return exit_code


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        exit_code = run_watchdog()
        sys.exit(exit_code)
    except Exception as fatal:
        # WD-FIX12 — reraise pour que cron/supervisor détecte l'échec
        log.critical(f"FATAL watchdog.py : {fatal}", exc_info=True)
        raise
