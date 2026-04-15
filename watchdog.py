#!/usr/bin/env python3
# watchdog.py — SENTINEL v3.60 — Surveillance cron & auto-recovery
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
# WD-FIX16 — parsing log JSON structuré (format {"ts":...,"msg":...})
# WD-FIX17 — tokens succès : "CYCLE TERMINE EN" / "RAPPORT MENSUEL TERMINE EN"
# WD-FIX18 — datetime naive/aware unifiés → timezone.utc partout
# WD-FIX19 — open() sans close() dans auto_restart_pipeline() corrigé
# WD-FIX20 — glob pattern corrigé : "SENTINEL_*.html"
# WD-FIX21 — VERSION synchronisée
# WD-FIX22 — annotation retour _build_alert_email : tuple[str,str,str]
# WD-FIX23 — seuil zombie étendu à 180 min le 1er du mois
# WD-FIX24 — compteurs _warnings/_critiques réinitialisés en début run
# WD-FIX25 — health endpoint HTTP vérifié (3e source de vérité)
# WD-FIX26 — log explicite après rotation (évite faux positif)
# WD-FIX27 — purge output/ renommée _purge_output_files()
# WD-FIX28 — exit_code sauvegardé dans state["runs"]
# WD-FIX29 — json.loads isolé : except JSONDecodeError/UnicodeDecodeError
# WD-FIX30 — _tail_lines() via collections.deque (sans charger le fichier en RAM)
# WD-FIX31 — SQLite en lecture seule (uri=True, mode=ro)
# WD-FIX32 — ssl.create_default_context() sur starttls() (anti-MITM)
# WD-FIX33 — SENTINEL_NO_ROTATE=1 désactive la rotation interne
# WD-FIX34 — psutil remplace pgrep + /proc : cross-platform
# WD-FIX35 — DETACHED_PROCESS + CREATE_NO_WINDOW sur Windows
# WD-FIX36 — pythonw.exe sur Windows (pas de fenêtre console)
#
# Corrections v3.60 — Audit inter-scripts 2026-04 :
# WD-60-FIX1  — Import config.py centralisé (VERSION, SMTP_PASS, TOKENS_SUCCES…)
#               SMTP_PASS lit maintenant SMTP_PASS/SMTP_PASSWORD/GMAIL_APP_PASSWORD
# WD-60-FIX2  — VERSION synchronisée sur VERSION du projet (3.60)
# WD-60-FIX3  — _build_alert_email() : datetime.now() → datetime.now(timezone.utc)
# WD-60-FIX4  — send_daily_summary() : datetime.now() → datetime.now(timezone.utc)
# WD-60-FIX5  — TOKENS_SUCCES importés depuis config.py (alignés v3.60)
# WD-60-FIX6  — Chemins absolus via config.PROJECT_ROOT
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python watchdog.py               Vérification complète
#   python watchdog.py --restart     Auto-restart si pipeline mort
#   python watchdog.py --summary     Force l'envoi du résumé quotidien
#   python watchdog.py --rotate-logs Force la rotation des logs
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

# ── Charger .env + config central (WD-60-FIX1) ───────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import config as _cfg
    _CONFIG_AVAILABLE = True
except ImportError:
    _CONFIG_AVAILABLE = False

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
# CONSTANTES & CONFIGURATION — WD-60-FIX1/2/5/6
# ═════════════════════════════════════════════════════════════════════════════

# WD-60-FIX2 : VERSION depuis config.py (source unique)
VERSION = _cfg.VERSION if _CONFIG_AVAILABLE else "3.60"

# Seuils temporels
SEUIL_H_ALERTE = int(os.environ.get("SENTINEL_MAX_SILENCE", "26"))
SEUIL_H_WARN   = 20
ANTI_SPAM_H    = 4
LOG_MAX_MB     = 50
DISK_CRIT_MB   = 200
DISK_WARN_MB   = 500

# WD-60-FIX5 : TOKENS_SUCCES depuis config.py — alignés sur sentinel_main.py v3.60
TOKENS_SUCCES = _cfg.TOKENS_SUCCES if _CONFIG_AVAILABLE else (
    "CYCLE TERMINE EN", "RAPPORT MENSUEL TERMINE EN"
)

# WD-60-FIX6 : Chemins absolus
if _CONFIG_AVAILABLE:
    _PROJECT_ROOT = _cfg.PROJECT_ROOT
    LOGFILE       = _cfg.SENTINEL_LOG
    DB_PATH       = _cfg.DB_PATH
else:
    _PROJECT_ROOT = Path(__file__).resolve().parent
    LOGFILE       = Path(os.environ.get("SENTINEL_LOG", "logs/sentinel.log"))
    DB_PATH       = Path(os.environ.get("SENTINEL_DB",  "data/sentinel.db"))

STATE_FILE = _PROJECT_ROOT / "logs" / "watchdog_state.json"
OUTPUT_DIR = _PROJECT_ROOT / "output"

# WD-60-FIX1 : SMTP depuis config.py (gère les 3 noms de variable)
if _CONFIG_AVAILABLE:
    SMTP_HOST = _cfg.SMTP_HOST
    SMTP_PORT = _cfg.SMTP_PORT
    SMTP_USER = _cfg.SMTP_USER
    SMTP_PASS = _cfg.SMTP_PASS
    ALERT_TO  = _cfg.REPORT_EMAIL
else:
    SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
    SMTP_USER = os.environ.get("SMTP_USER", "")
    SMTP_PASS = (
        os.environ.get("SMTP_PASS")
        or os.environ.get("SMTP_PASSWORD")
        or os.environ.get("GMAIL_APP_PASSWORD")
        or ""
    )
    ALERT_TO = os.environ.get("REPORT_EMAIL", SMTP_USER)

# Health endpoint (WD-FIX25)
HEALTH_PORT = int(os.environ.get("SENTINEL_HEALTH_PORT", "8765"))

# WD-FIX33 — désactive rotation interne si logrotate est actif
DISABLE_INTERNAL_ROTATION = os.environ.get("SENTINEL_NO_ROTATE", "0") == "1"

# Flags CLI
AUTO_RESTART  = "--restart"     in sys.argv
FORCE_SUMMARY = "--summary"     in sys.argv
FORCE_ROTATE  = "--rotate-logs" in sys.argv

# Compteurs — réinitialisés dans run_watchdog() (WD-FIX24)
_warnings  = 0
_critiques = 0

# ═════════════════════════════════════════════════════════════════════════════
# UTILITAIRE — Lecture des N dernières lignes sans charger le fichier en RAM
# ═════════════════════════════════════════════════════════════════════════════

def _tail_lines(path: Path, n: int = 2000) -> list[str]:
    """WD-FIX30 — collections.deque : évite d'allouer ~50 MB pour 2000 lignes."""
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
    STATE_FILE.parent.mkdir(exist_ok=True)
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
    # WD-60-FIX3 : datetime.now(timezone.utc) — plus jamais naïf
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

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(body, "plain", "utf-8"))
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))

    # WD-FIX5 : 3 délais de retry
    delays = [30, 60, 120]
    for attempt in range(3):
        try:
            if SMTP_PORT == 465:
                ctx = ssl.create_default_context()
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as server:
                    server.login(SMTP_USER, SMTP_PASS)
                    server.sendmail(SMTP_USER, ALERT_TO.split(","), msg.as_string())
            else:
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
            delay = delays[attempt]
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
    WD-FIX16 — Parsing format JSON structuré sentinel_main.py v3.60.
    WD-FIX17 — Tokens : "CYCLE TERMINE EN" / "RAPPORT MENSUEL TERMINE EN".
    WD-FIX18 — Toutes comparaisons datetime en UTC aware.
    WD-FIX30 — _tail_lines() sans charger le fichier entier.
    WD-FIX31 — SQLite en lecture seule (mode=ro).
    WD-60-FIX5 — TOKENS_SUCCES depuis config (alignés v3.60).
    """
    global _warnings, _critiques

    age_h_log: float         = float("inf")
    age_h_db:  float         = float("inf")
    last_dt_log: datetime | None = None
    last_dt_db:  datetime | None = None
    now_utc = datetime.now(timezone.utc)  # WD-60-FIX3 : UTC systématique

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
        conn = None
        try:
            db_uri = DB_PATH.as_uri() + "?mode=ro"
            conn   = sqlite3.connect(db_uri, uri=True, timeout=5)
            row    = conn.execute(
                "SELECT date FROM reports ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if row:
                last_dt_db = datetime.fromisoformat(row[0])
                if last_dt_db.tzinfo is None:
                    last_dt_db = last_dt_db.replace(tzinfo=timezone.utc)
                age_h_db = (now_utc - last_dt_db).total_seconds() / 3600
        except sqlite3.Error as e:
            log.warning(f"SQLite check_last_run : {e}")
        finally:
            if conn:
                conn.close()

    # ── Source 3 : health endpoint ────────────────────────────────────────────
    health = check_health_endpoint()

    # ── Décision finale (on prend le plus récent des deux sources) ─────────────
    age_h = min(age_h_log, age_h_db)
    source = "log" if age_h_log <= age_h_db else "db"

    if age_h == float("inf"):
        if health["ok"]:
            log.info("Aucun run trouvé en log/DB mais health endpoint OK — pipeline récemment démarré")
            return True, 0.0, "health_endpoint_ok"
        _critiques += 1
        log.error("Aucun run trouvé (log + DB + health endpoint) — pipeline jamais exécuté ou vide")
        return False, float("inf"), "no_run_found"

    detail = f"Dernier run il y a {age_h:.1f}h (source: {source})"
    log.info(detail)

    if age_h > SEUIL_H_ALERTE:
        _critiques += 1
        log.error(f"Pipeline SILENCIEUX depuis {age_h:.1f}h > seuil {SEUIL_H_ALERTE}h")
        return False, age_h, detail
    elif age_h > SEUIL_H_WARN:
        _warnings += 1
        log.warning(f"Pipeline silence {age_h:.1f}h > seuil warn {SEUIL_H_WARN}h")
        return True, age_h, detail

    return True, age_h, detail

# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 2 — Espace disque (WD-FIX6)
# ═════════════════════════════════════════════════════════════════════════════

def check_disk_space() -> dict[str, Any]:
    """WD-FIX6 — Vérification espace disque + estimation croissance."""
    result: dict[str, Any] = {"ok": True, "free_mb": 0, "detail": ""}
    try:
        usage    = shutil.disk_usage(str(_PROJECT_ROOT))
        free_mb  = usage.free // (1024 * 1024)
        total_mb = usage.total // (1024 * 1024)
        used_pct = 100 * usage.used // usage.total

        result["free_mb"] = free_mb
        result["detail"]  = f"Disque : {free_mb} MB libre / {total_mb} MB ({used_pct}% utilisé)"

        if free_mb < DISK_CRIT_MB:
            _critiques_count = globals().get("_critiques", 0)
            global _critiques
            _critiques += 1
            result["ok"]  = False
            result["level"] = "critical"
            log.error(f"DISQUE CRITIQUE : {free_mb} MB < {DISK_CRIT_MB} MB")
        elif free_mb < DISK_WARN_MB:
            global _warnings
            _warnings += 1
            result["level"] = "warning"
            log.warning(f"Disque bas : {free_mb} MB < {DISK_WARN_MB} MB")
        else:
            result["level"] = "ok"
            log.info(result["detail"])

    except OSError as e:
        result["detail"] = f"Impossible de vérifier l'espace disque : {e}"
        log.warning(result["detail"])
    return result

# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 3 — Base de données SQLite (WD-FIX3)
# ═════════════════════════════════════════════════════════════════════════════

def check_sqlite_db() -> dict[str, Any]:
    """
    WD-FIX3  — SQLite comme 2e source de vérité.
    WD-FIX31 — Lecture seule (mode=ro).
    Vérifie intégrité, compte lignes, détecte BUG-DB-1 (UNIQUE absent).
    """
    result: dict[str, Any] = {"ok": False, "detail": "", "bugdb1": False}

    if not DB_PATH.exists():
        global _critiques
        _critiques += 1
        result["detail"] = f"Base de données absente : {DB_PATH}"
        log.error(result["detail"])
        return result

    conn = None
    try:
        db_uri = DB_PATH.as_uri() + "?mode=ro"
        conn   = sqlite3.connect(db_uri, uri=True, timeout=5)

        # Intégrité
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        if integrity[0] != "ok":
            _critiques += 1
            result["detail"] = f"DB corrompue : {integrity[0]}"
            log.error(result["detail"])
            return result

        # Comptes
        counts = {}
        for table in ("reports", "metrics", "seenhashes", "tendances", "alertes"):
            try:
                row           = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = row[0] if row else 0
            except sqlite3.OperationalError:
                counts[table] = -1  # table absente

        # BUG-DB-1 : vérification UNIQUE sur reports.date
        schema_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reports'"
        ).fetchone()
        if schema_sql and "UNIQUE" not in schema_sql[0].upper():
            _critiques += 1
            result["bugdb1"] = True
            log.error(
                "BUG-DB-1 : contrainte UNIQUE absente sur reports.date — "
                "UPSERT échoue silencieusement → doublons possibles. "
                "Corriger le DDL dans db_manager.py."
            )

        # Colonne github_days_back (HC-51-FIX5)
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(metrics)").fetchall()]
            if "github_days_back" not in cols:
                global _warnings
                _warnings += 1
                log.warning(
                    "DB : colonne 'github_days_back' absente de 'metrics' — "
                    "anti-biais GitHub inopérant. Lancer initdb() pour la migration."
                )
        except sqlite3.OperationalError:
            pass

        result["ok"]     = True
        result["counts"] = counts
        result["detail"] = f"DB OK — {counts}"
        log.info(result["detail"])

    except sqlite3.Error as e:
        _critiques += 1
        result["detail"] = f"Erreur SQLite : {e}"
        log.error(result["detail"])
    finally:
        if conn:
            conn.close()

    return result

# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 4 — Dernier rapport HTML (WD-FIX14)
# ═════════════════════════════════════════════════════════════════════════════

def check_output_files() -> dict[str, Any]:
    """WD-FIX14 — Vérifie la présence d'un rapport HTML récent."""
    result: dict[str, Any] = {"ok": False, "latest": None, "age_h": None}

    if not OUTPUT_DIR.is_dir():
        log.warning(f"Répertoire output/ absent : {OUTPUT_DIR}")
        return result

    # WD-FIX20 : pattern corrigé "SENTINEL_*.html"
    html_files = sorted(OUTPUT_DIR.glob("SENTINEL_*.html"), key=lambda p: p.stat().st_mtime)
    if not html_files:
        global _warnings
        _warnings += 1
        log.warning("Aucun rapport HTML trouvé dans output/")
        return result

    latest     = html_files[-1]
    age_h      = (time.time() - latest.stat().st_mtime) / 3600
    result.update(ok=True, latest=str(latest.name), age_h=round(age_h, 1))

    if age_h > SEUIL_H_ALERTE + 2:
        _warnings += 1
        log.warning(f"Dernier rapport HTML : {latest.name} — {age_h:.1f}h (ancien)")
    else:
        log.info(f"Dernier rapport HTML : {latest.name} — {age_h:.1f}h ✓")

    return result

# ═════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION 5 — Détection process zombie (WD-FIX15/34)
# ═════════════════════════════════════════════════════════════════════════════

def check_zombie() -> bool:
    """
    WD-FIX15 — Détecte sentinel_main.py bloqué.
    WD-FIX23 — Seuil étendu à 180 min le 1er du mois (rapport mensuel).
    WD-FIX34 — psutil cross-platform (Linux/macOS/Windows).
    """
    today = datetime.now(timezone.utc)
    zombie_threshold_min = 180 if today.day == 1 else 90

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
                        global _warnings
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
                        global _warnings
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
            except OSError as e:
                log.warning(f"Impossible de supprimer {f.name} : {e}")
    if removed:
        log.info(f"Purge output/ : {removed} fichiers anciens supprimés (>{days}j)")
    return removed

# ═════════════════════════════════════════════════════════════════════════════
# AUTO-RESTART (WD-FIX8)
# ═════════════════════════════════════════════════════════════════════════════

def auto_restart_pipeline() -> bool:
    """
    WD-FIX8  — Redémarre sentinel_main.py en arrière-plan si mort.
    WD-FIX19 — open() sans close() corrigé → context manager with.
    WD-FIX35 — Flags Windows : DETACHED_PROCESS + CREATE_NO_WINDOW.
    WD-FIX36 — pythonw.exe sur Windows.
    """
    main_script = _PROJECT_ROOT / "sentinel_main.py"
    if not main_script.exists():
        log.error(f"auto_restart : {main_script} introuvable")
        return False

    log_restart = _PROJECT_ROOT / "logs" / "restart.log"
    log.info(f"Auto-restart : lancement de {main_script}")

    try:
        if platform.system() == "Windows":
            python_exe = "pythonw.exe"
            kwargs: dict = {
                "creationflags": 0x00000008 | 0x08000000,  # DETACHED_PROCESS | CREATE_NO_WINDOW
            }
        else:
            python_exe = sys.executable
            kwargs = {
                "start_new_session": True,
            }

        # WD-FIX19 : context manager — plus d'open() sans close()
        with open(log_restart, "a", encoding="utf-8") as log_fh:
            proc = subprocess.Popen(
                [python_exe, str(main_script)],
                stdout=log_fh,
                stderr=log_fh,
                cwd=str(_PROJECT_ROOT),
                **kwargs,
            )

        log.info(f"Auto-restart : sentinel_main.py lancé (PID {proc.pid})")
        return True

    except Exception as e:
        log.error(f"Auto-restart échoué : {e}")
        return False

# ═════════════════════════════════════════════════════════════════════════════
# CONSTRUCTION EMAIL D'ALERTE — WD-60-FIX3 (datetime UTC)
# ═════════════════════════════════════════════════════════════════════════════

def _build_alert_email(
    age_h:    float,
    db_stats: dict,
    disk:     dict,
    detail:   str = "",
) -> tuple[str, str, str]:
    """
    WD-FIX10 — Alerte enrichie avec statistiques DB + disque.
    WD-FIX22 — Annotation de retour : tuple[str, str, str] (subject, text, html).
    WD-60-FIX3 — datetime.now(timezone.utc) — plus jamais de datetime naïf.
    """
    # WD-60-FIX3 : UTC systématique
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    age_label = f"{age_h:.1f}h" if age_h < float("inf") else "INCONNU"

    subject = f"🚨 SENTINEL ALERTE — Pipeline silencieux depuis {age_label} [{now_str}]"

    text_body = (
        f"SENTINEL — Alerte pipeline
"
        f"{'=' * 50}
"
        f"Heure UTC     : {now_str}
"
        f"Silence       : {age_label}
"
        f"Seuil alerte  : {SEUIL_H_ALERTE}h

"
        f"ÉTAT DB
"
        f"-------
"
        f"Intégrité     : {'OK' if db_stats.get('ok') else 'ERREUR'}
"
        f"Lignes        : {db_stats.get('counts', {})}
"
        f"BUG-DB-1      : {'OUI ⚠' if db_stats.get('bugdb1') else 'Non'}

"
        f"DISQUE
"
        f"------
"
        f"Espace libre  : {disk.get('free_mb', '?')} MB

"
        f"DÉTAIL
"
        f"------
"
        f"{detail}

"
        f"Actions recommandées :
"
        f"  1. Vérifier logs/sentinel.log
"
        f"  2. python3 watchdog.py --restart
"
        f"  3. python3 health_check.py
"
    )

    html_body = f"""<html><body>
<h2 style="color:#c0392b">🚨 SENTINEL — Alerte pipeline</h2>
<table border="1" cellpadding="6" style="border-collapse:collapse">
  <tr><th>Heure UTC</th><td>{now_str}</td></tr>
  <tr><th>Silence pipeline</th><td><b>{age_label}</b></td></tr>
  <tr><th>Seuil alerte</th><td>{SEUIL_H_ALERTE}h</td></tr>
  <tr><th>DB intégrité</th><td>{'✅ OK' if db_stats.get('ok') else '❌ ERREUR'}</td></tr>
  <tr><th>BUG-DB-1 (UNIQUE)</th><td>{'⚠️ OUI' if db_stats.get('bugdb1') else 'Non'}</td></tr>
  <tr><th>Espace disque libre</th><td>{disk.get('free_mb','?')} MB</td></tr>
</table>
<pre style="background:#f4f4f4;padding:12px">{detail}</pre>
<p><b>Actions :</b> vérifier logs/sentinel.log, relancer via watchdog.py --restart</p>
</body></html>"""

    return subject, text_body, html_body

# ═════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ QUOTIDIEN (WD-FIX11) — WD-60-FIX4 (datetime UTC)
# ═════════════════════════════════════════════════════════════════════════════

def send_daily_summary(
    state:    dict,
    age_h:    float,
    db_stats: dict,
    disk:     dict,
) -> None:
    """
    WD-FIX11  — Email résumé quotidien "pipeline vivant".
    WD-60-FIX4 — datetime.now(timezone.utc) — plus jamais de datetime naïf.
    """
    # Anti-spam : 1 résumé max / 20h
    last_summary = state.get("last_summary")
    if last_summary:
        try:
            last_dt = datetime.fromisoformat(last_summary)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            # WD-60-FIX4 : UTC
            if (datetime.now(timezone.utc) - last_dt).total_seconds() < 20 * 3600:
                log.info("Résumé quotidien déjà envoyé (<20h) — skipped")
                return
        except ValueError:
            pass

    # WD-60-FIX4 : UTC systématique
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    age_label  = f"{age_h:.1f}h" if age_h < float("inf") else "?"
    disk_free  = disk.get("free_mb", "?")
    db_ok      = "✅ OK" if db_stats.get("ok") else "❌ ERREUR"

    subject = f"✅ SENTINEL Résumé quotidien — Pipeline actif [{now_str}]"
    body    = (
        f"SENTINEL — Résumé quotidien
"
        f"{'=' * 40}
"
        f"Heure UTC    : {now_str}
"
        f"Dernier run  : il y a {age_label}
"
        f"DB           : {db_ok}
"
        f"Disque libre : {disk_free} MB
"
        f"Warnings     : {_warnings}
"
        f"Critiques    : {_critiques}
"
    )

    if send_alert(subject, body):
        # WD-60-FIX4 : UTC
        state["last_summary"] = datetime.now(timezone.utc).isoformat()
        log.info("Résumé quotidien envoyé ✓")
    else:
        log.warning("Résumé quotidien : échec envoi email")

# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATEUR PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def run_watchdog() -> int:
    """
    Exécute toutes les vérifications et retourne le code de sortie :
      0 = OK, 1 = warning, 2 = critique.
    WD-FIX24 — Compteurs réinitialisés en début de run.
    WD-FIX28 — exit_code sauvegardé dans state["runs"].
    """
    global _warnings, _critiques
    _warnings  = 0
    _critiques = 0

    log.info(f"=== WATCHDOG SENTINEL v{VERSION} — {datetime.now(timezone.utc).isoformat()} ===")

    state = _load_state()

    # ── 1. Rotation logs ──────────────────────────────────────────────────────
    if FORCE_ROTATE or not DISABLE_INTERNAL_ROTATION:
        rotate_logs_if_needed()

    # ── 2. Vérifications ──────────────────────────────────────────────────────
    pipeline_ok, age_h, detail = check_last_run()
    db_stats                   = check_sqlite_db()
    disk                       = check_disk_space()
    output                     = check_output_files()

    # Zombie uniquement si pipeline semble actif
    zombie = check_zombie()
    if zombie:
        log.warning("ZOMBIE détecté — sentinel_main.py bloqué")

    # ── 3. Décision alerte ────────────────────────────────────────────────────
    is_critical = (not pipeline_ok) or (not db_stats.get("ok")) or _critiques > 0

    if is_critical and _can_send_alert(state, "pipeline_dead"):
        subject, text_body, html_body = _build_alert_email(age_h, db_stats, disk, detail)
        if send_alert(subject, text_body, html_body):
            _mark_alert_sent(state, "pipeline_dead")

    # ── 4. Auto-restart ───────────────────────────────────────────────────────
    if AUTO_RESTART and not pipeline_ok and not zombie:
        restarted = auto_restart_pipeline()
        log.info(f"Auto-restart : {'succès' if restarted else 'échec'}")

    # ── 5. Résumé quotidien ───────────────────────────────────────────────────
    if FORCE_SUMMARY or (pipeline_ok and not is_critical):
        send_daily_summary(state, age_h, db_stats, disk)

    # ── 6. Purge output anciens ───────────────────────────────────────────────
    _purge_output_files(days=30)

    # ── 7. Sauvegarde état (WD-FIX9/28) ──────────────────────────────────────
    exit_code = 2 if is_critical else (1 if _warnings > 0 else 0)
    state.setdefault("runs", []).append({
        "ts":        datetime.now(timezone.utc).isoformat(),
        "age_h":     age_h if age_h < float("inf") else None,
        "ok":        pipeline_ok,
        "warnings":  _warnings,
        "critiques": _critiques,
        "exit_code": exit_code,
    })
    # Garder seulement les 30 dernières entrées
    state["runs"] = state["runs"][-30:]
    _save_state(state)

    # ── 8. Rapport JSON (WD-FIX9) ─────────────────────────────────────────────
    report_path = _PROJECT_ROOT / "logs" / "watchdog_state.json"
    log.info(f"État watchdog → {report_path}")

    summary = (
        f"Watchdog v{VERSION} : "
        f"pipeline={'OK' if pipeline_ok else 'MORT'}, "
        f"age={age_h:.1f}h, "
        f"warnings={_warnings}, critiques={_critiques}, "
        f"exit={exit_code}"
    )
    if exit_code == 0:
        log.info(summary)
    elif exit_code == 1:
        log.warning(summary)
    else:
        log.error(summary)

    return exit_code

# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        code = run_watchdog()
        sys.exit(code)
    except KeyboardInterrupt:
        log.info("Watchdog interrompu par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        log.error(f"Exception non gérée dans watchdog : {e}", exc_info=True)
        sys.exit(2)  # WD-FIX12 — cron détecte l'échec
