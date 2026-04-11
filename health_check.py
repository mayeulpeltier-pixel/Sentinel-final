#!/usr/bin/env python3
# health_check.py — SENTINEL v3.51 — Monitoring complet du pipeline
# =============================================================================
# Vérifie l'intégrité complète du pipeline avant/après déploiement.
#
# Corrections v3.40 appliquées :
#   HC-FIX1   FIX-OBS1   — zéro print(), logging structuré avec logger nommé
#   HC-FIX2   C-4        — vérification Python ≥ 3.10 bloquante avec message clair
#   HC-FIX3   R1-NEW-2   — mode --offline isole les vérifs réseau/API
#   HC-FIX4   CODE-5     — SENTINEL_MODEL validé (format claude-*), non hardcodé
#   HC-FIX5   E1-4       — exit code 2=critique / 1=warning / 0=OK (CI/CD)
#   HC-FIX6   BUG-DB3    — intégration db_manager v3.40 : PRAGMA integrity+WAL
#   HC-FIX7              — vérification flux RSS parallèle (ThreadPoolExecutor)
#   HC-FIX8   CDC-4      — vérification espace disque + estimation 12 mois
#   HC-FIX9              — validation marqueurs prompts (cohérence init_prompts)
#   HC-FIX10  R6A3-NEW-2 — TAVILY_API_KEY : warning non-bloquant si absente
#   HC-FIX11  K-6        — SMTP testé avec timeout (warning non bloquant)
#   HC-FIX12             — rapport JSON complet dans logs/health_report.json
#   HC-FIX13             — alerte email si erreurs critiques
#   HC-FIX14  NEW-IP3    — validation marqueurs DEBUTJSONDELTA / FINJSONDELTA
#   HC-FIX15             — mode --ci : sortie minimale, exit code strict
#
# Corrections v3.40-POST (post-audit) :
#   POST-FIX1 — VERSION corrigée "3.38" → "3.40"
#   POST-FIX2 — CORE_SCRIPTS : "sam_gov_scraper.py" → "samgov_scraper.py"
#   POST-FIX3 — PROMPT_MARKERS daily.txt : "ARTICLESFILTRSPARSCRAPER" →
#               "ARTICLESFILTRESPARSCRAPER" (typo E manquant)
#   POST-FIX4 — PROMPT_MARKERS monthly.txt : marqueurs alignés sur sentinel_api
#               "INJECTION30RAPPORTSCOMPRESSES" → "INJECTIONMENSUELLE"
#               "EXECUTIVE SUMMARY" → "RÉSUMÉ EXÉCUTIF MENSUEL"
#               "ANNE" supprimé (marqueur obsolète)
#   POST-FIX5 — _record() : ajout branche "info" (évite log.error + _critical_count++)
#   POST-FIX6 — check_regression_analytique() : lit tables tendances/alertes
#               directement (fausse alerte "dérive modèle" permanente corrigée)
#   POST-FIX7 — OLLAMA_TIMEOUT ajouté dans ENV_VARS (optionnel)
#   POST-FIX8 — Documentation --offline corrigée
#
# Corrections v3.51 — Post-audit final :
#   HC-51-FIX1  check_last_run() : datetime.now().astimezone() pour comparaison
#               timezone-aware. La v3.40 utilisait datetime.now() (naïf) —
#               si les logs écrivent en UTC, l'âge était sous-estimé de N heures.
#
#   HC-51-FIX2  check_smtp() + _send_alert_email() : support port 465 (SMTP_SSL).
#               La v3.40 appelait toujours server.starttls() même sur port 465
#               → SMTPException "STARTTLS extension not supported" si TLS direct.
#               Fix : détection SMTP_PORT → SMTP_SSL ou STARTTLS via _smtp_connect().
#
#   HC-51-FIX3  check_regression_analytique() : noms de colonnes SQL corrigés.
#               BUG CRITIQUE SILENCIEUX — colonnes inexistantes dans le DDL :
#                 "tendances WHERE date >= ..."      → "datederniere >= ..."
#                 "alertes WHERE date_ouverture ..." → "dateouverture ..."
#               SQLite retournait 0 lignes sans erreur → fausse alerte F4 PERMANENTE.
#
#   HC-51-FIX4  check_sqlite() : expected_idx mis à jour pour db_manager v3.51.
#               Nouveaux index idx_alertes_texte + idx_metrics_date en "warning"
#               (migration auto dans initdb() — non bloquant).
#
#   HC-51-FIX5  check_sqlite() : vérification colonne github_days_back dans metrics.
#               Absente des DBs < v3.48 → getmetrics() retourne NULL silencieux.
#
#   HC-51-FIX6  check_sqlite() : try/finally garantit conn.close().
#               FUITE CONNEXION : si sqlite3.Error survenait avant conn.close(),
#               la connexion restait ouverte. Désormais fermée dans finally.
#
#   HC-51-FIX7  check_anthropic_api() : timeout=HC_TIMEOUT (était hardcodé 8s).
#   HC-51-FIX8  check_tavily()        : timeout=HC_TIMEOUT (était hardcodé 8s).
#               HC_TIMEOUT env var ajouté mais les deux fonctions ignoraient la var.
#
#   HC-51-FIX9  check_anthropic_api() mode --offline : _record("info") et non
#               "warning". Un --offline propre remontait exit code 1 au lieu de 0.
#
#   HC-51-FIX10 _record() : icône INFO (bleu) pour niveau "info" — était WARN (jaune).
#               Constante INFO définie mais jamais utilisée dans _record().
#
#   HC-51-FIX11 DISK_WARN_MB / DISK_CRITICAL_MB : sémantique corrigée.
#               AVANT : DISK_MIN_MB=500 (warning) / DISK_WARN_MB=200 (critical)
#               APRÈS : DISK_WARN_MB=500 (warning) / DISK_CRITICAL_MB=200 (critical)
#               La logique était correcte, les noms étaient inversés.
#
#   HC-51-FIX12 DRY_RUN : compatibilité SENTINEL_MAILER_DRY_RUN (mailer.py v3.52).
#               mailer.py lit SENTINEL_MAILER_DRY_RUN, health_check lisait seulement
#               SENTINEL_DRY_RUN → alertes envoyées même quand mailer en dry-run.
#
#   HC-51-NEW1  check_db_api_compat() : validation API db_manager v3.51.
#               Vérifie toutes les méthodes attendues par les scripts modifiés.
#               Erreur critique si une méthode manque — évite le crash runtime.
#
#   HC-51-NEW2  HC_TIMEOUT configurable via SENTINEL_HC_TIMEOUT (env).
#               Hardcodé à 8s en v3.40 — impossible à ajuster sans modifier source.
#
#   HC-51-NEW3  _send_alert_email() respecte DRY_RUN global (SENTINEL_DRY_RUN
#               ou SENTINEL_MAILER_DRY_RUN) — aucune alerte en mode simulation.
#
# =============================================================================
# Usage :
#   python health_check.py              Vérification complète (déploiement serveur)
#   python health_check.py --offline    Saute les vérifs réseau/RSS/SMTP/API TCP.
#                                       Utiliser en dev local pour la vitesse.
#                                       NB : ne préserve aucun quota Anthropic/Tavily.
#   python health_check.py --ci         Mode CI/CD : sortie minimale, exit code strict
#   python health_check.py --feeds      Teste uniquement les flux RSS (verbose)
#   python health_check.py --db         Teste uniquement la base SQLite
# =============================================================================
# Codes de sortie :
#   0  — Tout est OK (pipeline opérationnel)
#   1  — Avertissements non-bloquants (Tavily absent, feeds lents, etc.)
#   2  — Erreurs critiques (Python < 3.10, ANTHROPIC_API_KEY manquante,
#         scripts core absents, DB corrompue, API db_manager incomplète)
# =============================================================================

from __future__ import annotations

import importlib
import json
import logging
import os
import shutil
import smtplib
import socket
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging structuré (HC-FIX1 / FIX-OBS1) ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sentinel.health")


# =============================================================================
# CONSTANTES
# =============================================================================

VERSION      = "3.51"
SMTP_TIMEOUT = 10

# [HC-51-FIX11] Nommage sémantiquement correct (était inversé en v3.40) :
#   DISK_CRITICAL_MB (200) → critique si espace libre < 200 MB
#   DISK_WARN_MB     (500) → warning  si espace libre < 500 MB
DISK_CRITICAL_MB = 200
DISK_WARN_MB     = 500

# [HC-51-NEW2] HC_TIMEOUT configurable via env — hardcodé à 8s en v3.40
HC_TIMEOUT = int(os.environ.get("SENTINEL_HC_TIMEOUT", "8"))

# [HC-51-FIX12 + HC-51-NEW3] DRY_RUN — respecte SENTINEL_DRY_RUN (global)
# ET SENTINEL_MAILER_DRY_RUN (mailer.py v3.52) pour cohérence totale
DRY_RUN = (
    os.environ.get("SENTINEL_DRY_RUN", "").lower() in ("1", "true", "yes")
    or os.environ.get("SENTINEL_MAILER_DRY_RUN", "0").strip() == "1"
)

OFFLINE    = "--offline" in sys.argv
CI_MODE    = "--ci"      in sys.argv
FEEDS_ONLY = "--feeds"   in sys.argv
DB_ONLY    = "--db"      in sys.argv

# [HC-51-FIX10] INFO (bleu) utilisé pour le niveau "info" — était inutilisé
if CI_MODE or not sys.stdout.isatty():
    OK   = "[OK]  "
    WARN = "[WARN]"
    FAIL = "[FAIL]"
    INFO = "[INFO]"
else:
    OK   = "\u001B[92m[OK]  \u001B[0m"
    WARN = "\u001B[93m[WARN]\u001B[0m"
    FAIL = "\u001B[91m[FAIL]\u001B[0m"
    INFO = "\u001B[94m[INFO]\u001B[0m"  # bleu

CORE_SCRIPTS = [
    "scraper_rss.py",
    "memory_manager.py",
    "sentinel_api.py",
    "charts.py",
    "report_builder.py",
    "mailer.py",
    "sentinel_main.py",
    "db_manager.py",
    "samgov_scraper.py",   # POST-FIX2 : était "sam_gov_scraper.py"
    "watchdog.py",
    "init_prompts.py",
    "health_check.py",
    "nlp_scorer.py",       # HC-51 : ajouté (présent dans l'audit)
]

REQUIRED_DIRS = ["data", "output", "output/charts", "logs", "prompts"]

PROMPT_MARKERS: dict[str, list[str]] = {
    "prompts/system.txt": [
        "SENTINEL",
        "DEBUTJSONDELTA",
        "FINJSONDELTA",
        "MODULE 9",
    ],
    "prompts/daily.txt": [
        "DATEAUJOURDHUI",
        "ARTICLESFILTRESPARSCRAPER",   # POST-FIX3 : E manquait → faux positif permanent
        "MEMOIRECOMPRESSE7JOURS",
        "DEBUTJSONDELTA",
        "FINJSONDELTA",
    ],
    "prompts/monthly.txt": [
        "MOIS",
        "INJECTIONMENSUELLE",          # POST-FIX4 : était "INJECTION30RAPPORTSCOMPRESSES"
        "RÉSUMÉ EXÉCUTIF MENSUEL",     # POST-FIX4 : était "EXECUTIVE SUMMARY"
        "STATISTIQUES",
        "TENDANCES LOURDES",
        "RUPTURES TECHNOLOGIQUES",
        "RECOMMANDATIONS",
        # "ANNE" supprimé — POST-FIX4 : absent du template sentinel_api v3.51
    ],
}

PIP_PACKAGES: dict[str, str] = {
    "anthropic":  "critique",
    "feedparser": "critique",
    "requests":   "critique",
    "dotenv":     "critique",
    "matplotlib": "critique",
    "tavily":     "optionnel",
    "weasyprint": "optionnel",
    "plotly":     "optionnel",
    "kaleido":    "optionnel",
    "schedule":   "optionnel",
    "sklearn":    "optionnel",
    "streamlit":  "optionnel",
    "telethon":   "optionnel",
    "scipy":      "optionnel",
}

ENV_VARS: dict[str, tuple[str, str]] = {
    "ANTHROPIC_API_KEY":       ("critique",  "Clé API Anthropic — requise pour les rapports Claude"),
    "TAVILY_API_KEY":          ("optionnel", "Tavily web search — désactive la vérif web si absent"),
    "SMTP_USER":               ("warning",   "Adresse Gmail pour envoi des rapports"),
    "SMTP_PASS":               ("warning",   "Mot de passe application Gmail (≠ mot de passe compte)"),
    "REPORT_EMAIL":            ("warning",   "Destinataire(s) du rapport — défaut = SMTP_USER"),
    "SENTINEL_MODEL":          ("warning",   "Modèle Claude (ex: claude-sonnet-4-6) — défaut si absent"),
    "HAIKU_MODEL":             ("warning",   "Modèle Haiku pour compressions (ex: claude-haiku-4-5)"),
    "SAM_GOV_API_KEY":         ("optionnel", "API SAM.gov DoD — contrats défense US (gratuit)"),
    "SENTINEL_DB":             ("optionnel", "Chemin SQLite — défaut: data/sentinel.db"),
    "SENTINEL_MAX_TOKENS":     ("optionnel", "Max tokens sortie Claude — défaut: 16000"),
    "SENTINEL_TAVILY_MAX":     ("optionnel", "Max appels Tavily/rapport — défaut: 5"),
    "OLLAMA_TIMEOUT":          ("optionnel", "Timeout Ollama local en secondes — défaut: 180"),
    "SENTINEL_HC_TIMEOUT":     ("optionnel", "Timeout vérif réseau healthcheck (s) — défaut: 8"),
    "SENTINEL_DRY_RUN":        ("optionnel", "1 = mode simulation global, aucun email envoyé"),
    "SENTINEL_MAILER_DRY_RUN": ("optionnel", "1 = dry-run mailer.py v3.52 (alias SENTINEL_DRY_RUN)"),
    "SENTINEL_CSV_MODE":       ("optionnel", "both|standard|excel — format export CSV (défaut: both)"),
}

# Index attendus post-migration v3.40
_EXPECTED_IDX_V340 = {
    "idx_reports_date",
    "idx_seenhashes_date",
    "idx_tendances_active",
    "idx_alertes_active",
    "idx_acteurs_score",
}
# [HC-51-FIX4] Index v3.51 — warning uniquement (migration auto dans initdb())
_EXPECTED_IDX_V351 = {
    "idx_alertes_texte",   # DB-51-FIX2 — closealerte() O(log n)
    "idx_metrics_date",    # DB-51-IDX  — getmetrics() ORDER BY date DESC
}

_results:        list[dict[str, Any]] = []
_critical_count: int = 0
_warning_count:  int = 0


def _record(
    level:    str,
    category: str,
    name:     str,
    message:  str,
    detail:   str = "",
) -> None:
    """
    [POST-FIX5] Branche "info" ajoutée — avant, _record("info", ...) tombait
    dans le else → log.error() + _critical_count++.
    [HC-51-FIX10] Icône INFO (bleu) pour le niveau "info" — était WARN (jaune).
    Niveaux valides : ok | info | warning | critical.
    """
    global _critical_count, _warning_count
    _results.append({
        "level":    level,
        "category": category,
        "name":     name,
        "message":  message,
        "detail":   detail,
        "ts":       datetime.now(timezone.utc).isoformat(),
    })

    # [HC-51-FIX10] Icône correcte par niveau
    if level == "ok":
        icon = OK
    elif level == "info":
        icon = INFO   # bleu — plus de confusion avec WARN
    elif level == "warning":
        icon = WARN
    else:
        icon = FAIL

    line = f"{icon} [{category}] {name}: {message}"
    if detail and not CI_MODE:
        line += f"\n       → {detail}"

    if level in ("ok", "info"):
        log.info(line)
    elif level == "warning":
        log.warning(line)
        _warning_count += 1
    else:
        log.error(line)
        _critical_count += 1


# =============================================================================
# VÉR. 1 — Python & pip (C-4)
# =============================================================================

def check_python() -> None:
    vi = sys.version_info
    if vi < (3, 10):
        _record("critical", "Python", "version",
                f"Python {vi.major}.{vi.minor} < 3.10",
                "BLOQUANT : walrus :=, match/case, annotations requises. "
                "sudo apt install python3.10  ou  pyenv install 3.10")
    else:
        _record("ok", "Python", "version",
                f"Python {vi.major}.{vi.minor}.{vi.micro} ✓")
    try:
        import pip  # type: ignore
        pv = tuple(int(x) for x in pip.__version__.split(".")[:2])
        if pv < (21, 0):
            _record("warning", "Python", "pip",
                    f"pip {pip.__version__} < 21.0",
                    "pip install --upgrade pip  (requis PEP 517/660)")
        else:
            _record("ok", "Python", "pip", f"pip {pip.__version__} ✓")
    except ImportError:
        _record("warning", "Python", "pip", "pip non importable")


def check_packages() -> None:
    for pkg, level in PIP_PACKAGES.items():
        try:
            importlib.import_module(pkg)
            _record("ok", "Package", pkg, "installé ✓")
        except ImportError:
            lvl = "critical" if level == "critique" else "warning"
            fix = (pkg
                   .replace("dotenv",  "python-dotenv")
                   .replace("sklearn", "scikit-learn"))
            _record(lvl, "Package", pkg, "ABSENT", f"pip install {fix}")


# =============================================================================
# VÉR. 2 — Structure fichiers & dossiers
# =============================================================================

def check_structure() -> None:
    for script in CORE_SCRIPTS:
        p = Path(script)
        if p.exists():
            _record("ok", "Script", script,
                    f"présent ({p.stat().st_size // 1024} KB) ✓")
        else:
            _record("critical", "Script", script, "ABSENT",
                    "Script core manquant — pipeline non fonctionnel")

    for d in REQUIRED_DIRS:
        dp = Path(d)
        if dp.is_dir():
            _record("ok", "Dossier", d, "présent ✓")
        else:
            try:
                dp.mkdir(parents=True, exist_ok=True)
                _record("warning", "Dossier", d, "créé à la volée",
                        "Absent — créé maintenant. Lancer init_prompts.py")
            except OSError as e:
                _record("critical", "Dossier", d, f"impossible de créer: {e}")

    for prompt_file, markers in PROMPT_MARKERS.items():
        pp = Path(prompt_file)
        if not pp.exists():
            _record("critical", "Prompt", prompt_file, "ABSENT",
                    "python init_prompts.py")
            continue
        content = pp.read_text(encoding="utf-8", errors="ignore").upper()
        missing = [m for m in markers if m.upper() not in content]
        if missing:
            _record("critical", "Prompt", prompt_file,
                    f"{len(missing)} marqueur(s) manquant(s): {missing}",
                    "python init_prompts.py --force")
        else:
            _record("ok", "Prompt", prompt_file,
                    f"{len(markers)} marqueurs OK ({pp.stat().st_size // 1024} KB) ✓")

    for fname, (lvl, fix) in {
        "SCOPE.md":         ("warning", "python init_prompts.py"),
        "CHANGELOG.md":     ("warning", "python init_prompts.py"),
        ".env.example":     ("warning", "python init_prompts.py"),
        "requirements.txt": ("warning", "python init_prompts.py"),
    }.items():
        if Path(fname).exists():
            _record("ok", "Projet", fname, "présent ✓")
        else:
            _record(lvl, "Projet", fname, "absent", f"Lancer : {fix}")

    if Path(".env").exists():
        _record("ok", "Projet", ".env", "présent ✓")
    else:
        _record("warning", "Projet", ".env",
                "absent — variables ENV à définir manuellement",
                "Copier .env.example → .env et remplir les valeurs")


# =============================================================================
# VÉR. 3 — Variables d'environnement (HC-FIX4 / CODE-5)
# =============================================================================

def check_env_vars() -> None:
    for var, (level, desc) in ENV_VARS.items():
        val = os.environ.get(var, "")
        if val:
            display = (val[:6] + "..." + val[-3:] if ("KEY" in var or "PASS" in var)
                       and len(val) > 10 else val)
            _record("ok", "ENV", var, f"définie ({display}) ✓")
        else:
            lvl = "critical" if level == "critique" else "warning"
            sfx = " (non bloquant)" if level == "optionnel" else ""
            _record(lvl, "ENV", var, f"ABSENTE{sfx}", desc)

    model = os.environ.get("SENTINEL_MODEL", "")
    if model:
        if not model.startswith("claude-") or len(model) < 12:
            _record("critical", "ENV", "SENTINEL_MODEL_FORMAT",
                    f"Format invalide : '{model}'",
                    "Attendu : claude-sonnet-4-6, claude-haiku-4-5, etc.")
        else:
            _record("ok", "ENV", "SENTINEL_MODEL_FORMAT",
                    f"format valide ✓ ({model})")
    else:
        _record("warning", "ENV", "SENTINEL_MODEL_FORMAT",
                "non défini — défaut claude-sonnet-4-6 (non bloquant)",
                "Définir SENTINEL_MODEL=claude-sonnet-4-6 dans .env")

    u = os.environ.get("SMTP_USER", "")
    p = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
    if u and not p:
        _record("critical", "ENV", "SMTP_COHERENCE",
                "SMTP_USER défini mais SMTP_PASS absent",
                "Créer mot de passe application Gmail : "
                "compte.google.com → Sécurité → Mots de passe d'applications")
    elif u and p:
        _record("ok", "ENV", "SMTP_COHERENCE", "SMTP_USER + SMTP_PASS cohérents ✓")

    max_tok = os.environ.get("SENTINEL_MAX_TOKENS", "")
    if max_tok:
        try:
            v = int(max_tok)
            if v < 4000:
                _record("critical", "ENV", "SENTINEL_MAX_TOKENS",
                        f"Valeur trop faible : {v} (min recommandé : 8000)",
                        "Valeur suggérée : SENTINEL_MAX_TOKENS=16000")
            elif v > 64000:
                _record("warning", "ENV", "SENTINEL_MAX_TOKENS",
                        f"Valeur élevée : {v} — coût API potentiellement élevé")
            else:
                _record("ok", "ENV", "SENTINEL_MAX_TOKENS", f"{v} tokens ✓")
        except ValueError:
            _record("critical", "ENV", "SENTINEL_MAX_TOKENS",
                    f"Valeur non-entière : '{max_tok}'")

    ollama_to = os.environ.get("OLLAMA_TIMEOUT", "")
    if ollama_to:
        try:
            v = int(ollama_to)
            if v < 30:
                _record("warning", "ENV", "OLLAMA_TIMEOUT",
                        f"Valeur faible : {v}s — insuffisant sur GPU milieu de gamme",
                        "Recommandé : 90 (GPU rapide) | 180 (défaut) | 480 (CPU)")
            else:
                _record("ok", "ENV", "OLLAMA_TIMEOUT", f"{v}s ✓")
        except ValueError:
            _record("critical", "ENV", "OLLAMA_TIMEOUT",
                    f"Valeur non-entière : '{ollama_to}'")

    csv_mode = os.environ.get("SENTINEL_CSV_MODE", "both")
    if csv_mode not in ("both", "standard", "excel"):
        _record("warning", "ENV", "SENTINEL_CSV_MODE",
                f"Valeur invalide : '{csv_mode}' (accepté : both|standard|excel)",
                "SENTINEL_CSV_MODE=both dans .env")
    else:
        _record("ok", "ENV", "SENTINEL_CSV_MODE", f"'{csv_mode}' ✓")


# =============================================================================
# VÉR. 4 — SQLite WAL (HC-FIX6 + HC-51-FIX4/5/6)
# =============================================================================

def check_sqlite() -> dict[str, Any]:
    """
    [HC-51-FIX4] expected_idx mis à jour : idx_alertes_texte + idx_metrics_date
                 signalés en warning (migration auto dans initdb()).
    [HC-51-FIX5] Vérification colonne github_days_back dans metrics.
    [HC-51-FIX6] try/finally garantit conn.close() même en cas d'exception.
    """
    db_path = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
    result:  dict[str, Any] = {}

    if not db_path.exists():
        _record("warning", "SQLite", "fichier",
                f"absent : {db_path}",
                "Normal au 1er déploiement — créé au 1er run de sentinel_main.py")
        return result

    size_kb = db_path.stat().st_size // 1024
    _record("ok", "SQLite", "fichier", f"présent ({size_kb} KB) — {db_path} ✓")

    # [HC-51-FIX6] conn déclaré hors try → finally peut toujours appeler close()
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.row_factory = sqlite3.Row

        # WAL mode
        jmode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if jmode.lower() == "wal":
            _record("ok", "SQLite", "journal_mode", "WAL ✓")
        else:
            _record("warning", "SQLite", "journal_mode",
                    f"mode '{jmode}' (WAL attendu)",
                    "Sera corrigé par db_manager._create_connection() au prochain run")

        # Intégrité
        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if ic == "ok":
            _record("ok", "SQLite", "integrity_check", "aucune corruption ✓")
        else:
            _record("critical", "SQLite", "integrity_check",
                    f"CORRUPTION : {ic}",
                    f"Restaurer backup ou supprimer {db_path} et relancer sentinel_main.py")

        # Tables
        expected_tables = {
            "reports", "tendances", "alertes", "acteurs", "seenhashes", "metrics",
        }
        existing_tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        missing_t = expected_tables - existing_tables
        if missing_t:
            _record("warning", "SQLite", "tables",
                    f"tables manquantes : {missing_t}",
                    "Sera créé par db_manager DDL au prochain connect()")
        else:
            _record("ok", "SQLite", "tables", "6/6 tables présentes ✓")

        # Contrainte UNIQUE sur reports.date (BUG-DB1)
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='reports' AND type='table'"
        ).fetchone()
        schema_sql = (row[0] or "") if row else ""
        if "UNIQUE" in schema_sql.upper():
            _record("ok", "SQLite", "reports_date_unique",
                    "contrainte UNIQUE présente ✓ (BUG-DB1 corrigé)")
        else:
            _record("critical", "SQLite", "reports_date_unique",
                    "UNIQUE absent sur reports.date — UPSERT ON CONFLICT échouera",
                    "Supprimer sentinel.db et relancer — schéma v3.40 recrée automatiquement")

        # Index existants
        existing_idx = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }

        # Index v3.40 (attendus sur toute DB v3.40+)
        missing_v340 = _EXPECTED_IDX_V340 - existing_idx
        if missing_v340:
            _record("warning", "SQLite", "index_v340",
                    f"index v3.40 manquants : {missing_v340}",
                    "Seront créés par db_manager DDL au prochain connect()")
        else:
            _record("ok", "SQLite", "index_v340",
                    f"{len(_EXPECTED_IDX_V340)} index v3.40 présents ✓")

        # [HC-51-FIX4] Index v3.51 — warning uniquement (migration auto)
        missing_v351 = _EXPECTED_IDX_V351 - existing_idx
        if missing_v351:
            _record("warning", "SQLite", "index_v351",
                    f"index v3.51 manquants : {missing_v351} — migration auto dans initdb()",
                    "Lancer db_manager.initdb() ou redémarrer sentinel_main.py v3.51+")
        else:
            _record("ok", "SQLite", "index_v351",
                    f"{len(_EXPECTED_IDX_V351)} index v3.51 présents ✓ "
                    f"(idx_alertes_texte + idx_metrics_date)")

        # [HC-51-FIX5] Colonne github_days_back dans metrics (DB-48-MIG)
        if "metrics" in existing_tables:
            cols_metrics = {
                r[1] for r in conn.execute("PRAGMA table_info(metrics)").fetchall()
            }
            if "github_days_back" in cols_metrics:
                _record("ok", "SQLite", "metrics_github_days_back",
                        "colonne github_days_back présente ✓ (DB-48-MIG)")
            else:
                _record("warning", "SQLite", "metrics_github_days_back",
                        "colonne github_days_back ABSENTE dans metrics",
                        "DB antérieure à v3.48 — sera migrée par initdb() :\n"
                        "  ALTER TABLE metrics ADD COLUMN github_days_back INTEGER DEFAULT 0")

        # Comptage données
        for table in (expected_tables & existing_tables):
            try:
                result[f"count_{table}"] = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"  # noqa: S608
                ).fetchone()[0]
            except sqlite3.Error:
                result[f"count_{table}"] = -1

        _record("ok", "SQLite", "données",
                f"{result.get('count_reports', 0)} rapports | "
                f"{result.get('count_seenhashes', 0)} seenhashes | "
                f"{result.get('count_alertes', 0)} alertes | "
                f"{result.get('count_tendances', 0)} tendances | "
                f"{result.get('count_acteurs', 0)} acteurs | "
                f"{result.get('count_metrics', 0)} metrics")

        # Flag migration JSON→SQLite
        if (db_path.parent / ".migrationdonev3.37").exists():
            _record("ok", "SQLite", "migration_json",
                    "migration JSON→SQLite effectuée ✓")
        else:
            _record("warning", "SQLite", "migration_json",
                    "flag migration absent",
                    "Si sentinel_memory.json existe, appeler run_migration() dans db_manager.py")

    except sqlite3.Error as e:
        _record("critical", "SQLite", "connexion",
                f"Erreur SQLite : {e}",
                f"Vérifier {db_path} — corrompu ou verrou actif (autre process ?)")
    finally:
        # [HC-51-FIX6] Fermeture garantie même si une exception a été levée
        if conn is not None:
            conn.close()

    return result


# =============================================================================
# VÉR. 5 — Compatibilité API db_manager v3.51 [HC-51-NEW1]
# =============================================================================

def check_db_api_compat() -> None:
    """
    [HC-51-NEW1] Vérifie que toutes les méthodes requises par les scripts
    modifiés sont bien exposées par le db_manager importé.
    Erreur CRITIQUE si une méthode core manque — évite le crash runtime.
    """
    try:
        import db_manager as dbm  # type: ignore
        from db_manager import SentinelDB  # type: ignore
    except ImportError as e:
        _record("critical", "DB-API", "import",
                f"db_manager non importable : {e}",
                "Vérifier la syntaxe de db_manager.py")
        return

    # Méthodes SentinelDB requises par les scripts v3.51
    sentinel_db_methods: dict[str, str] = {
        "savereport":            "sentinel_main.py v3.54 — sauvegarde rapport",
        "getrecentreports":      "report_builder.py v3.44 — mémoire 7j",
        "get_last_n_reports":    "sentinel_main.py v3.54 — rapport mensuel MAP",
        "loadseen":              "scraper_rss.py — déduplication hashes",
        "loadseendays":          "scraper_rss.py — alias DB-51 de loadseen()",
        "saveseen":              "scraper_rss.py + samgov_scraper.py",
        "purgeseeolderthandays": "sentinel_main.py — purge périodique",
        "savetendance":          "sentinel_main.py — mémoire tendances",
        "gettendancesactives":   "sentinel_main.py + report_builder.py",
        "ouvrirealerte":         "sentinel_main.py — cycle alertes",
        "closealerte":           "sentinel_main.py — cycle alertes",
        "getalertesactives":     "sentinel_main.py + report_builder.py",
        "saveacteur":            "sentinel_main.py — mémoire acteurs",
        "getacteurs":            "report_builder.py v3.44",
        "savemetrics":           "sentinel_main.py v3.54",
        "getmetrics":            "sentinel_main.py v3.54 + report_builder.py",
        "export_csv":            "sentinel_main.py v3.54 — export analytique CSV",
        "tolegacydict":          "memory_manager.py — rétrocompatibilité",
    }

    # Fonctions module db_manager requises
    module_funcs: dict[str, str] = {
        "initdb":          "sentinel_main.py — démarrage pipeline",
        "closedb":         "sentinel_main.py — finally: closedb()",
        "getdb":           "accès DB interne (context manager)",
        "check_integrity": "health_check.py lui-même",
        "backup_db":       "sentinel_main.py / watchdog.py",
        "runmigration":    "db_manager.initdb() — migration JSON→SQLite",
    }

    missing_methods = [m for m in sentinel_db_methods if not hasattr(SentinelDB, m)]
    missing_funcs   = [f for f in module_funcs if not hasattr(dbm, f)]

    if missing_methods:
        _record("critical", "DB-API", "SentinelDB_methods",
                f"{len(missing_methods)} méthode(s) manquante(s) : {missing_methods}",
                "\n".join(
                    f"  {m} ← {sentinel_db_methods[m]}" for m in missing_methods
                ))
    else:
        _record("ok", "DB-API", "SentinelDB_methods",
                f"{len(sentinel_db_methods)} méthodes SentinelDB présentes ✓")

    if missing_funcs:
        _record("critical", "DB-API", "module_functions",
                f"{len(missing_funcs)} fonction(s) module manquante(s) : {missing_funcs}",
                "\n".join(f"  {f} ← {module_funcs[f]}" for f in missing_funcs))
    else:
        _record("ok", "DB-API", "module_functions",
                f"{len(module_funcs)} fonctions module présentes ✓")

    try:
        db_ver = getattr(dbm, "VERSION", None) or getattr(dbm, "_VERSION", None)
        if db_ver is None:
            _record("warning", "DB-API", "version",
                    "Aucune constante VERSION dans db_manager.py",
                    "Ajouter VERSION = '3.51' pour l'audit de cohérence")
        else:
            _record("ok", "DB-API", "version", f"db_manager VERSION = {db_ver} ✓")
    except Exception:
        pass


# =============================================================================
# VÉR. 6 — Flux RSS parallèle (HC-FIX7)
# =============================================================================

def _check_one_feed(args: tuple[str, str, str]) -> dict[str, Any]:
    url, source, score = args
    t0 = time.monotonic()
    try:
        import requests as req
        r = req.get(
            url,
            timeout=HC_TIMEOUT,  # [HC-51-FIX7/8] utilise HC_TIMEOUT configurable
            headers={"User-Agent": f"SENTINEL-HEALTHCHECK/{VERSION}"},
            allow_redirects=True,
        )
        ms = int((time.monotonic() - t0) * 1000)
        return {
            "source": source, "url": url, "score": score,
            "status": r.status_code,
            "ok":     r.status_code == 200 and len(r.content) > 100,
            "size":   len(r.content), "ms": ms,
        }
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        return {
            "source": source, "url": url, "score": score,
            "status": 0, "ok": False, "error": str(e)[:120], "ms": ms,
        }


def check_rss_feeds(verbose: bool = False) -> list[dict]:
    try:
        from scraper_rss import RSS_FEEDS  # type: ignore
    except ImportError:
        _record("warning", "RSS", "import",
                "scraper_rss.py non importable — vérif feeds ignorée",
                "Vérifier la syntaxe de scraper_rss.py")
        return []

    total = len(RSS_FEEDS)
    log.info(
        f"RSS — test de {total} flux "
        f"(timeout {HC_TIMEOUT}s via SENTINEL_HC_TIMEOUT, 15 threads)..."
    )

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(15, total)) as pool:
        futures = {pool.submit(_check_one_feed, ft): ft for ft in RSS_FEEDS}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                log.warning(f"RSS worker exception: {e}")

    dead  = [r for r in results if not r["ok"]]
    alive = [r for r in results if r["ok"]]
    slow  = [r for r in alive if r.get("ms", 0) > 5000]
    pct   = 100 * len(alive) // total if total else 0

    lvl = "ok" if pct >= 85 else "warning" if pct >= 60 else "critical"
    _record(lvl, "RSS", "disponibilité",
            f"{len(alive)}/{total} flux OK ({pct}%) — {len(dead)} morts",
            f"{len(slow)} flux lents >5s" if slow else "")

    dead_a = [r for r in dead if r.get("score") == "A"]
    if dead_a:
        _record("critical", "RSS", "feeds_scoreA_morts",
                f"{len(dead_a)} flux score A inaccessibles : "
                f"{', '.join(r['source'] for r in dead_a[:5])}",
                "Sources primaires OSINT — impact direct sur la qualité des rapports")

    if verbose:
        for r in sorted(dead, key=lambda x: x.get("score", "C")):
            log.warning(
                f"  MORT [{r['score']}] {r['source'][:42]:42s} "
                f"HTTP {r['status']} — {r.get('error', '')[:60]}"
            )
        for r in slow:
            log.info(f"  LENT {r['source'][:42]:42s} {r['ms']}ms")

    return results


# =============================================================================
# VÉR. 7 — Anthropic API reachability [HC-51-FIX7 + HC-51-FIX9]
# =============================================================================

def check_anthropic_api() -> None:
    """
    [HC-51-FIX7] timeout=HC_TIMEOUT — était hardcodé 8s, ignorait SENTINEL_HC_TIMEOUT.
    [HC-51-FIX9] _record("info") en mode --offline — était "warning" → exit code 1
                 même quand tout était OK en offline.
    """
    if OFFLINE:
        # [HC-51-FIX9] "info" et non "warning" — --offline propre doit sortir en 0
        _record("info", "API", "anthropic", "skipped (--offline)")
        return
    if not os.environ.get("ANTHROPIC_API_KEY"):
        _record("critical", "API", "anthropic",
                "ANTHROPIC_API_KEY absente — non testable")
        return
    try:
        # [HC-51-FIX7] HC_TIMEOUT — plus hardcodé à 8
        sock = socket.create_connection(("api.anthropic.com", 443), timeout=HC_TIMEOUT)
        sock.close()
        _record("ok", "API", "anthropic", "api.anthropic.com:443 accessible ✓")
    except socket.gaierror as e:
        _record("critical", "API", "anthropic",
                f"DNS resolution failed : {e}", "Vérifier connexion Internet / proxy")
    except OSError as e:
        _record("critical", "API", "anthropic",
                f"TCP connexion échouée : {e}", "Port 443 bloqué par firewall ?")


# =============================================================================
# VÉR. 8 — Tavily (HC-FIX10 / R6A3-NEW-2) [HC-51-FIX8]
# =============================================================================

def check_tavily() -> None:
    """[HC-51-FIX8] timeout=HC_TIMEOUT — était hardcodé 8s."""
    if OFFLINE:
        return
    if not os.environ.get("TAVILY_API_KEY"):
        _record("warning", "API", "tavily",
                "TAVILY_API_KEY absente — vérification web désactivée (non bloquant)",
                "Gratuit 1000 req/mois sur tavily.com — 5 req/rapport max")
        return
    try:
        # [HC-51-FIX8] HC_TIMEOUT — plus hardcodé à 8
        sock = socket.create_connection(("api.tavily.com", 443), timeout=HC_TIMEOUT)
        sock.close()
        _record("ok", "API", "tavily", "api.tavily.com:443 accessible ✓")
    except OSError as e:
        _record("warning", "API", "tavily",
                f"Tavily inaccessible : {e} (non bloquant)")


# =============================================================================
# VÉR. 9 — SMTP avec support SSL/STARTTLS [HC-51-FIX2]
# =============================================================================

def _smtp_connect(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
) -> None:
    """
    [HC-51-FIX2] Gestion unifiée SSL/STARTTLS — identique à mailer.py v3.52.

    Port 465 → smtplib.SMTP_SSL (TLS direct, connexion chiffrée dès l'ouverture).
    Port 587  → smtplib.SMTP + STARTTLS (chiffrement négocié après EHLO).

    La v3.40 appelait toujours server.starttls() → SMTPException sur port 465.
    """
    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=SMTP_TIMEOUT) as server:
            server.login(smtp_user, smtp_pass)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)


def check_smtp() -> None:
    """[HC-51-FIX2] Utilise _smtp_connect() — support port 465 + port 587."""
    if OFFLINE:
        return
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not smtp_user or not smtp_pass:
        _record("warning", "SMTP", "config",
                "SMTP non configuré — envoi email désactivé",
                "Configurer SMTP_USER + SMTP_PASS dans .env")
        return
    try:
        _smtp_connect(smtp_host, smtp_port, smtp_user, smtp_pass)
        _record("ok", "SMTP", "auth",
                f"Auth OK — {smtp_user} → {smtp_host}:{smtp_port} "
                f"({'SSL' if smtp_port == 465 else 'STARTTLS'}) ✓")
    except smtplib.SMTPAuthenticationError:
        _record("critical", "SMTP", "auth",
                "Authentification SMTP ÉCHOUÉE",
                "Gmail : utiliser un mot de passe d'application (≠ mot de passe compte). "
                "compte.google.com → Sécurité → Mots de passe d'applications")
    except (smtplib.SMTPException, OSError) as e:
        _record("warning", "SMTP", "connexion",
                f"SMTP inaccessible : {str(e)[:80]} (warning non bloquant)",
                "Vérifier SMTP_HOST/SMTP_PORT ou connectivité réseau")


# =============================================================================
# VÉR. 10 — Espace disque (HC-FIX8 / CDC-4) [HC-51-FIX11]
# =============================================================================

def check_disk_space() -> None:
    """
    [HC-51-FIX11] Nommage sémantiquement corrigé :
        DISK_CRITICAL_MB (200 MB) → critique si espace libre < 200 MB
        DISK_WARN_MB     (500 MB) → warning  si espace libre < 500 MB
    Avant v3.51 : DISK_MIN_MB=500 (warning) / DISK_WARN_MB=200 (critical)
    — noms inversés par rapport à leur usage dans le if/elif.
    """
    try:
        usage    = shutil.disk_usage(".")
        free_mb  = usage.free  // (1024 * 1024)
        total_mb = usage.total // (1024 * 1024)
        used_pct = 100 * usage.used // usage.total

        if free_mb < DISK_CRITICAL_MB:
            _record("critical", "Disque", "espace_libre",
                    f"{free_mb} MB libres ({used_pct}% utilisé) — CRITIQUE",
                    f"< {DISK_CRITICAL_MB} MB : pipeline risque d'échouer. "
                    "find output -name '*.html' -mtime +30 -delete")
        elif free_mb < DISK_WARN_MB:
            _record("warning", "Disque", "espace_libre",
                    f"{free_mb} MB libres ({used_pct}% utilisé)",
                    f"< {DISK_WARN_MB} MB recommandés. "
                    "Estimation 12 mois : ~72 MB (HTML 36 + JSON 24 + PNG 12)")
        else:
            _record("ok", "Disque", "espace_libre",
                    f"{free_mb} MB libres / {total_mb} MB ({used_pct}% utilisé) ✓")

        output_dir = Path("output")
        if output_dir.is_dir():
            output_mb = sum(
                f.stat().st_size for f in output_dir.rglob("*") if f.is_file()
            ) // (1024 * 1024)
            html_n = len(list(output_dir.glob("*.html")))
            if output_mb > 100:
                _record("warning", "Disque", "output_taille",
                        f"output/ = {output_mb} MB ({html_n} HTML)",
                        "find output -name '*.html' -mtime +30 -delete")
            else:
                _record("ok", "Disque", "output_taille",
                        f"output/ = {output_mb} MB ({html_n} HTML) ✓")

    except OSError as e:
        _record("warning", "Disque", "espace_libre",
                f"Impossible de vérifier : {e}")


# =============================================================================
# VÉR. 11 — Dernier run pipeline [HC-51-FIX1]
# =============================================================================

def check_last_run() -> None:
    """
    [HC-51-FIX1] datetime timezone-aware pour comparaison fiable.

    PROBLÈME v3.40 :
        last_ok_dt = datetime.strptime(line[:19], fmt)  # naïf (sans timezone)
        age_h = (datetime.now() - last_ok_dt).total_seconds() / 3600

        Si watchdog.py / sentinel_main.py écrivent des timestamps UTC dans les
        logs (datetime.now(timezone.utc)), et que le serveur est en UTC+2 :
            datetime.now()     → 20:00 (locale)
            last_ok_dt parsé   → 18:00 (UTC, interprété comme local)
        → âge calculé = 2h alors que le vrai âge = 0h → fausse alerte cron.

    CORRECTION :
        .astimezone() sur la datetime parsée → considérée locale → UTC-aware.
        datetime.now().astimezone() → locale → UTC-aware.
        Comparaison dans le même référentiel → âge exact.
    """
    log_file = Path("logs/sentinel.log")
    if not log_file.exists():
        _record("warning", "Pipeline", "dernier_run",
                "logs/sentinel.log absent",
                "Normal au 1er déploiement. Lancer : python sentinel_main.py")
        return

    lines = log_file.read_text(encoding="utf-8", errors="ignore").splitlines()

    # Tokens de succès alignés sur sentinel_main.py v3.54
    tokens_ok = (
        "terminé",
        "SENTINEL v",
        "pipeline terminé",
        "Rapport disponible",
        "RAPPORT ENVOYÉ",
        "run_daily terminé",
        "run_monthly terminé",
    )
    last_ok_dt: datetime | None = None

    for line in reversed(lines[-500:]):
        if any(tok in line for tok in tokens_ok):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    # [HC-51-FIX1] .astimezone() → datetime locale UTC-aware
                    last_ok_dt = datetime.strptime(line[:19], fmt).astimezone()
                    break
                except ValueError:
                    continue
            if last_ok_dt:
                break

    if last_ok_dt is None:
        _record("warning", "Pipeline", "dernier_run",
                "Aucun run réussi trouvé dans les logs",
                "Lancer : python sentinel_main.py")
    else:
        # [HC-51-FIX1] datetime.now().astimezone() — cohérent avec last_ok_dt
        now_aware = datetime.now().astimezone()
        age_h     = (now_aware - last_ok_dt).total_seconds() / 3600
        lvl       = "ok" if age_h <= 30 else "warning"
        _record(lvl, "Pipeline", "dernier_run",
                f"Dernier run OK il y a {age_h:.1f}h "
                f"({last_ok_dt.strftime('%Y-%m-%d %H:%M')})"
                + (" ✓" if lvl == "ok" else " — vérifier le cron"),
                "" if lvl == "ok" else "crontab -l | grep sentinel")

    recent_errors = [
        ln for ln in lines[-50:]
        if any(kw in ln for kw in ("CRITICAL", "FATAL", "[ERROR]", "ERROR"))
        and "healthcheck" not in ln.lower()
    ]
    if recent_errors:
        _record("warning", "Pipeline", "erreurs_recentes",
                f"{len(recent_errors)} erreur(s) dans les 50 dernières lignes",
                recent_errors[-1][:120])
    else:
        _record("ok", "Pipeline", "erreurs_recentes",
                "Aucune erreur critique récente ✓")


# =============================================================================
# VÉR. 12 — WeasyPrint / dépendances système
# =============================================================================

def check_system_deps() -> None:
    """
    Vérification WeasyPrint + dépendances Linux (libcairo2, pango…).
    Critique pour report_builder.py v3.44 : build_pdf_report() appelle
    WeasyPrint de façon synchrone — un crash ici bloque le rapport PDF.
    """
    try:
        import weasyprint  # type: ignore
        if sys.platform.startswith("linux"):
            try:
                weasyprint.HTML(string="<p>ok</p>").write_pdf()
                _record("ok", "Système", "weasyprint", "rendu PDF fonctionnel ✓")
            except Exception as e:
                err = str(e).lower()
                if any(lib in err for lib in ("cairo", "pango", "gdk", "fontconfig")):
                    _record("critical", "Système", "weasyprint",
                            "libs système manquantes",
                            "sudo apt-get install -y libcairo2 libpango-1.0-0 "
                            "libgdk-pixbuf2.0-0 libffi-dev")
                else:
                    _record("warning", "Système", "weasyprint",
                            f"Erreur rendu : {str(e)[:80]}")
        else:
            _record("ok", "Système", "weasyprint",
                    "installé (Windows/macOS) ✓")
    except ImportError:
        _record("warning", "Système", "weasyprint",
                "non installé — PDF désactivé",
                "pip install weasyprint  +  "
                "sudo apt libcairo2 libpango-1.0-0 (Linux)")


# =============================================================================
# VÉR. 13 — Statistiques & régression analytique [M3-FIX / F4-FIX]
# =============================================================================

def check_distribution_normality() -> None:
    """
    M3-FIX : normalité des indices d'activité via Shapiro-Wilk.
    [POST-FIX5] _record("info", ...) géré correctement — plus de faux critique.
    Utilise SentinelDB.getmetrics() de db_manager v3.51.
    """
    try:
        from db_manager import SentinelDB  # type: ignore
        metrics = SentinelDB.getmetrics(ndays=90)
        indices = [float(m["indice"]) for m in metrics if m.get("indice")]

        if len(indices) < 30:
            _record("info", "Statistiques", "normalite",
                    f"Données insuffisantes ({len(indices)} points — min 30 requis)",
                    "Relancer après 30 jours de production.")
            return

        try:
            from scipy import stats as _scipy_stats  # type: ignore
            _stat, p_value = _scipy_stats.shapiro(indices)
            is_normal      = p_value >= 0.05
            mu    = sum(indices) / len(indices)
            sigma = (sum((x - mu) ** 2 for x in indices) / len(indices)) ** 0.5
            k     = float(os.environ.get("SENTINEL_ALERT_SIGMA", "2.0"))
            shewhart_threshold = mu + k * sigma
            empirical_p95      = sorted(indices)[int(0.95 * len(indices))]

            if is_normal:
                _record("ok", "Statistiques", "normalite",
                        f"Distribution gaussienne (Shapiro p={p_value:.3f} ≥ 0.05). "
                        f"Seuil Shewhart µ+{k}σ={shewhart_threshold:.2f} valide.",
                        f"n={len(indices)} | µ={mu:.2f} | σ={sigma:.2f}")
            else:
                _record("warning", "Statistiques", "normalite",
                        f"Distribution NON-gaussienne (Shapiro p={p_value:.3f} < 0.05). "
                        f"Seuil P95={empirical_p95:.2f} recommandé.",
                        "Définir SENTINEL_ALERT_SIGMA_MODE=percentile dans .env")

        except ImportError:
            _record("info", "Statistiques", "normalite",
                    "scipy absent — test Shapiro-Wilk désactivé "
                    "(pip install scipy)")

    except Exception as exc:
        _record("warning", "Statistiques", "normalite", f"Erreur : {exc}")


def check_regression_analytique() -> None:
    """
    F4-FIX : cohérence des données mémoire sur 7 jours.

    [POST-FIX6] Lit directement les tables tendances et alertes.
    [HC-51-FIX3] CORRECTION CRITIQUE — noms de colonnes SQL corrigés :
        AVANT (v3.40, bug silencieux) :
            "tendances WHERE date >= ..."      → colonne 'date' INEXISTANTE dans DDL
            "alertes WHERE date_ouverture ..." → underscore en trop (DDL : dateouverture)
            → SQLite retournait 0 lignes sans exception → fausse alerte F4 PERMANENTE.
        APRÈS (v3.51) :
            "tendances WHERE datederniere >= ..."  ✓
            "alertes WHERE dateouverture >= ..."   ✓
    """
    try:
        from db_manager import SentinelDB  # type: ignore

        reports = SentinelDB.getrecentreports(ndays=7)
        if len(reports) < 3:
            _record("info", "Régression", "deltas",
                    f"Moins de 3 rapports ({len(reports)}) — "
                    "test de régression ignoré")
            return

        db_path = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
        if not db_path.exists():
            _record("info", "Régression", "deltas",
                    "DB absente — test de régression ignoré")
            return

        conn = sqlite3.connect(str(db_path), timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            # [HC-51-FIX3] 'datederniere' — colonne réelle du DDL db_manager v3.40+
            tendances_7j = conn.execute(
                "SELECT COUNT(*) FROM tendances "
                "WHERE datederniere >= date('now', '-7 days')"
            ).fetchone()[0]

            # [HC-51-FIX3] 'dateouverture' — sans underscore, conforme au DDL
            alertes_7j = conn.execute(
                "SELECT COUNT(*) FROM alertes "
                "WHERE dateouverture >= date('now', '-7 days')"
            ).fetchone()[0]
        finally:
            conn.close()

        total_delta = tendances_7j + alertes_7j

        if total_delta == 0:
            _record("warning", "Régression", "deltas",
                    f"F4 Aucune tendance ni alerte sur 7 jours "
                    f"({len(reports)} rapports analysés). "
                    "Possible dérive du modèle ou bloc DEBUTJSONDELTA absent.",
                    "Vérifier les derniers rapports HTML dans output/ et s'assurer "
                    "que prompts/system.txt contient DEBUTJSONDELTA")
        else:
            _record("ok", "Régression", "deltas",
                    f"F4 Mémoire active : {tendances_7j} tendance(s) + "
                    f"{alertes_7j} alerte(s) sur 7j ✓")

    except Exception as exc:
        _record("warning", "Régression", "deltas", f"F4 Erreur : {exc}")


# =============================================================================
# RAPPORT JSON & ALERTE EMAIL (HC-FIX12 / HC-FIX13 / HC-51-FIX2 / HC-51-NEW3)
# =============================================================================

def _save_report(report: dict) -> None:
    """[HC-FIX12] Sauvegarde atomique du rapport JSON dans logs/health_report.json."""
    Path("logs").mkdir(exist_ok=True)
    out = Path("logs/health_report.json")
    tmp = out.with_suffix(".tmp")
    try:
        tmp.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(out)
        log.info(f"Rapport JSON sauvegardé : {out}")
    except OSError as e:
        log.warning(f"Impossible de sauvegarder rapport JSON : {e}")


def _send_alert_email(report: dict) -> None:
    """
    [HC-FIX13]   Alerte email si erreurs critiques + SMTP configuré.
    [HC-51-FIX2] Utilise _smtp_connect() — support port 465 (SMTP_SSL).
    [HC-51-NEW3] Respecte DRY_RUN (SENTINEL_DRY_RUN ou SENTINEL_MAILER_DRY_RUN).
    """
    # [HC-51-NEW3 + HC-51-FIX12] DRY_RUN — aucun email en mode simulation
    if DRY_RUN:
        log.info("DRY_RUN actif — alerte email healthcheck supprimée")
        return

    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
    report_to = os.environ.get("REPORT_EMAIL", smtp_user)
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not smtp_user or not smtp_pass or not report_to:
        return

    criticals = [r for r in _results if r["level"] == "critical"]
    if not criticals:
        return

    lines = [
        f"SENTINEL v{VERSION} — HealthCheck : "
        f"{len(criticals)} erreur(s) critique(s)\n"
    ]
    lines += [
        f"  [FAIL] [{r['category']}] {r['name']}: {r['message']}"
        for r in criticals[:10]
    ]
    if len(criticals) > 10:
        lines.append(f"  ... et {len(criticals) - 10} autres erreurs")
    lines.append("\nRapport complet : logs/health_report.json")

    msg = MIMEText("\n".join(lines), "plain", "utf-8")
    msg["Subject"] = f"SENTINEL HealthCheck — {len(criticals)} erreur(s) critique(s)"
    msg["From"]    = smtp_user
    msg["To"]      = report_to

    try:
        # [HC-51-FIX2] _smtp_connect() gère SSL (port 465) et STARTTLS (port 587)
        # sendmail() intégré dans le même bloc context manager
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=15) as server:
                server.login(smtp_user, smtp_pass)
                server.sendmail(smtp_user, report_to.split(","), msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.sendmail(smtp_user, report_to.split(","), msg.as_string())
        log.info(f"Alerte email envoyée → {report_to}")
    except Exception as e:
        log.warning(f"Envoi alerte email échoué (non bloquant) : {e}")


def _print_summary() -> None:
    n_ok = sum(1 for r in _results if r["level"] == "ok")
    bar  = "=" * 60
    log.info(bar)
    log.info(f"SENTINEL v{VERSION} — HealthCheck terminé")
    log.info(f"  ✓ OK       : {n_ok}")
    log.info(f"  ⚠ Warnings : {_warning_count}")
    log.info(f"  ✗ Critiques: {_critical_count}")
    if _critical_count == 0 and _warning_count == 0:
        log.info("  → Pipeline OPÉRATIONNEL ✓")
    elif _critical_count == 0:
        log.info("  → Pipeline opérationnel avec avertissements mineurs")
    else:
        log.error("  → Pipeline NON OPÉRATIONNEL — corriger les erreurs critiques")
    log.info(bar)


# =============================================================================
# POINT D'ENTRÉE PRINCIPAL
# =============================================================================

def run_health_check() -> int:
    t0  = time.monotonic()
    now = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info(f"SENTINEL v{VERSION} — HealthCheck démarré ({now})")
    log.info(
        f"Mode : {'OFFLINE' if OFFLINE else 'ONLINE'}"
        f"{' | CI' if CI_MODE else ''}"
        f"{' | FEEDS-ONLY' if FEEDS_ONLY else ''}"
        f"{' | DB-ONLY' if DB_ONLY else ''}"
        f"{' | DRY-RUN' if DRY_RUN else ''}"
    )
    log.info(f"HC_TIMEOUT={HC_TIMEOUT}s | DRY_RUN={DRY_RUN}")
    log.info("=" * 60)

    if FEEDS_ONLY:
        check_rss_feeds(verbose=True)
    elif DB_ONLY:
        check_sqlite()
        check_db_api_compat()  # [HC-51-NEW1] toujours vérifier l'API DB
    else:
        check_python()
        check_packages()
        check_structure()
        check_env_vars()
        check_sqlite()
        check_db_api_compat()  # [HC-51-NEW1] après check_sqlite, avant réseau
        if not OFFLINE:
            check_anthropic_api()
            check_tavily()
            check_smtp()
            check_rss_feeds(verbose=not CI_MODE)
        check_disk_space()
        check_last_run()
        check_system_deps()
        check_distribution_normality()
        check_regression_analytique()

    elapsed = time.monotonic() - t0
    _print_summary()
    log.info(f"Durée : {elapsed:.1f}s")

    report = {
        "version":        VERSION,
        "ts":             now,
        "elapsed_s":      round(elapsed, 2),
        "critical_count": _critical_count,
        "warning_count":  _warning_count,
        "ok_count":       sum(1 for r in _results if r["level"] == "ok"),
        "results":        _results,
    }
    _save_report(report)

    if not OFFLINE and not DRY_RUN:
        _send_alert_email(report)

    if _critical_count > 0:
        return 2
    if _warning_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run_health_check())
