#!/usr/bin/env python3
# config.py — SENTINEL v3.60 — Configuration centralisée
# =============================================================================
# SOURCE DE VÉRITÉ UNIQUE pour toutes les constantes du projet.
# Importé par tous les scripts : sentinel_main, sentinel_api, mailer,
# watchdog, health_check, ops_patents, samgov_scraper, telegram_scraper…
#
# Corrections audit 2026-04 :
#   CFG-FIX1  VERSION unique — élimine les 15 versions divergentes
#   CFG-FIX2  SMTP_PASS normalisé — fallback sur les 3 noms de variable
#             (SMTP_PASS / SMTP_PASSWORD / GMAIL_APP_PASSWORD)
#   CFG-FIX3  DRY_RUN unifié — SENTINEL_DRY_RUN + SENTINEL_MAILER_DRY_RUN
#   CFG-FIX4  PROJECT_ROOT absolu — Path(__file__).resolve().parent
#             Tous les chemins dérivés sont absolus (invalides en cron sinon)
#   CFG-FIX5  HAIKU_MODEL exposé ici — un seul endroit pour les noms de modèles
#   CFG-FIX6  TOKENS_SUCCES alignés sur sentinel_main.py v3.60
#   CFG-FIX7  Création automatique des répertoires requis
# =============================================================================

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# VERSION — CFG-FIX1 : source de vérité unique pour tout le projet
# ---------------------------------------------------------------------------
VERSION = "3.60"

# ---------------------------------------------------------------------------
# CHEMINS ABSOLUS — CFG-FIX4
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "data"
LOGS_DIR     = PROJECT_ROOT / "logs"
OUTPUT_DIR   = PROJECT_ROOT / "output"
PROMPTS_DIR  = PROJECT_ROOT / "prompts"
BACKUPS_DIR  = PROJECT_ROOT / "backups"

# CFG-FIX7 : création automatique des répertoires requis
for _d in [DATA_DIR, LOGS_DIR, OUTPUT_DIR, PROMPTS_DIR, BACKUPS_DIR,
           OUTPUT_DIR / "charts"]:
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# MODÈLES ANTHROPIC — CFG-FIX5
# ---------------------------------------------------------------------------
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")
HAIKU_MODEL  = os.environ.get("HAIKU_MODEL",    "claude-haiku-4-5")

# ---------------------------------------------------------------------------
# SMTP — CFG-FIX2 : normalisation des 3 noms de variable possibles
# ---------------------------------------------------------------------------
SMTP_HOST    = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT    = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER    = os.environ.get("SMTP_USER", "")
SMTP_PASS    = (
    os.environ.get("SMTP_PASS")
    or os.environ.get("SMTP_PASSWORD")
    or os.environ.get("GMAIL_APP_PASSWORD")
    or ""
)
REPORT_EMAIL = os.environ.get("REPORT_EMAIL", SMTP_USER)

# ---------------------------------------------------------------------------
# DRY RUN — CFG-FIX3 : source unique, cohérente avec mailer.py et health_check
# ---------------------------------------------------------------------------
DRY_RUN = (
    os.environ.get("SENTINEL_DRY_RUN", "").lower() in ("1", "true", "yes")
    or os.environ.get("SENTINEL_MAILER_DRY_RUN", "0").strip() == "1"
)

# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------
REPORT_RETENTION_DAYS = int(os.environ.get("SENTINEL_RETENTION_DAYS", "30"))
GITHUB_COOLDOWN_DAYS  = int(os.environ.get("GITHUB_COOLDOWN_DAYS",     "7"))
GITHUB_MAX_LOOKBACK   = int(os.environ.get("GITHUB_MAX_LOOKBACK",      "21"))
DAYS_BACK_CFG         = int(os.environ.get("GITHUB_DAYS_BACK",         "7"))
MAX_CONTRACTS         = int(os.environ.get("SENTINEL_MAX_CONTRACTS",   "50"))
MAX_TOKENS            = int(os.environ.get("SENTINEL_MAX_TOKENS",      "16000"))
TAVILY_MAX            = int(os.environ.get("SENTINEL_TAVILY_MAX",      "5"))
HEALTH_PORT           = int(os.environ.get("SENTINEL_HEALTH_PORT",     "8765"))
HC_TIMEOUT            = int(os.environ.get("SENTINEL_HC_TIMEOUT",      "8"))

# ---------------------------------------------------------------------------
# BASE DE DONNÉES
# ---------------------------------------------------------------------------
DB_PATH = Path(os.environ.get("SENTINEL_DB", str(DATA_DIR / "sentinel.db")))

# ---------------------------------------------------------------------------
# CLÉS API
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY",    "")
SAM_GOV_API_KEY   = os.environ.get("SAM_GOV_API_KEY",  "")
TELEGRAM_API_ID   = os.environ.get("TELEGRAM_API_ID",  "")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH","")
DEEPL_API_KEY     = os.environ.get("DEEPL_API_KEY",    "")

# ---------------------------------------------------------------------------
# WATCHDOG
# ---------------------------------------------------------------------------
SENTINEL_LOG            = LOGS_DIR / "sentinel.log"
SENTINEL_MAX_SILENCE_H  = int(os.environ.get("SENTINEL_MAX_SILENCE", "26"))
LOG_MAX_MB              = 50
DISK_CRIT_MB            = 200
DISK_WARN_MB            = 500
ANTI_SPAM_H             = 4

# CFG-FIX6 : tokens alignés sur sentinel_main.py v3.60
# Vérifiés contre les logger.info() réels de sentinel_main.py
TOKENS_SUCCES = ("CYCLE TERMINE EN", "RAPPORT MENSUEL TERMINE EN")

# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------
EXPORT_CSV    = os.environ.get("SENTINEL_EXPORT_CSV", "0") == "1"
CSV_MODE      = os.environ.get("SENTINEL_CSV_MODE", "both")   # both|standard|excel
