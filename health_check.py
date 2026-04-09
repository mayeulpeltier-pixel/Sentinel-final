#!/usr/bin/env python3
# health_check.py — SENTINEL v3.40 — Monitoring complet du pipeline
# ─────────────────────────────────────────────────────────────────────────────
# Vérifie l'intégrité complète du pipeline avant/après déploiement.
# Corrections v3.40 appliquées :
#   HC-FIX1   FIX-OBS1   — zéro print(), logging structuré avec logger nommé
#   HC-FIX2   C-4        — vérification Python ≥ 3.10 bloquante avec message clair
#   HC-FIX3   R1-NEW-2   — mode --offline isole les vérifs réseau/API
#   HC-FIX4   CODE-5     — SENTINEL_MODEL validé (format claude-*), non hardcodé
#   HC-FIX5   E1-4       — exit code 2=critique / 1=warning / 0=OK (CI/CD compliant)
#   HC-FIX6   BUG-DB3    — intégration db_manager v3.40 : PRAGMA integrity_check + WAL
#   HC-FIX7              — vérification flux RSS parallèle (ThreadPoolExecutor)
#   HC-FIX8   CDC-4      — vérification espace disque + estimation stockage 12 mois
#   HC-FIX9              — validation marqueurs prompts (cohérence init_prompts.py)
#   HC-FIX10  R6A3-NEW-2 — TAVILY_API_KEY : warning non-bloquant si absente
#   HC-FIX11  K-6        — SMTP testé avec timeout (non-bloquant, warning seulement)
#   HC-FIX12             — rapport JSON complet dans logs/health_report.json
#   HC-FIX13             — alerte email si erreurs critiques
#   HC-FIX14  NEW-IP3    — validation marqueurs DEBUTJSONDELTA / FINJSONDELTA
#   HC-FIX15             — mode --ci : sortie minimale, exit code strict
#
# Corrections v3.40-POST (post-audit) :
#   POST-FIX1 — VERSION corrigée "3.38" → "3.40"
#   POST-FIX2 — CORE_SCRIPTS : "sam_gov_scraper.py" → "samgov_scraper.py"
#   POST-FIX3 — PROMPT_MARKERS daily.txt : "ARTICLESFILTRSPARSCRAPER" →
#               "ARTICLESFILTRESPARSCRAPER" (typo E manquant, faux positif permanent)
#   POST-FIX4 — PROMPT_MARKERS monthly.txt : marqueurs alignés sur sentinel_api v3.51
#               "INJECTION30RAPPORTSCOMPRESSES" → "INJECTIONMENSUELLE"
#               "EXECUTIVE SUMMARY" → "RÉSUMÉ EXÉCUTIF MENSUEL"
#               "ANNE" supprimé (marqueur obsolète absent du template)
#   POST-FIX5 — _record() : ajout branche "info" (évite log.error +
#               _critical_count++ sur des records informatifs)
#   POST-FIX6 — check_regression_analytique() : lit les tables tendances/alertes
#               directement (reports.compressed contient les métriques, pas les
#               deltas — fausse alerte "dérive modèle" permanente corrigée)
#   POST-FIX7 — OLLAMA_TIMEOUT ajouté dans ENV_VARS (optionnel, sentinel_api v3.51)
#   POST-FIX8 — Documentation --offline corrigée : ce mode ne préserve aucun
#               quota API (les vérifs réseau sont des TCP/HTTP purs, sans appel
#               à anthropic.messages.create() ni à Tavily search). Les vraies
#               raisons d'utiliser --offline : vitesse (72 feeds = 8-15s),
#               réseau restreint (VPN/proxy), logins SMTP répétés sur Gmail.
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python health_check.py              Vérification complète (déploiement serveur)
#   python health_check.py --offline    Saute les vérifs réseau/RSS/SMTP/API TCP.
#                                       Utiliser en dev local pour la vitesse
#                                       (gain ~8-15s sur les 72 feeds RSS) et pour
#                                       éviter les logins SMTP répétés sur Gmail.
#                                       NB : ne préserve aucun quota Anthropic/Tavily
#                                       — les vérifs réseau sont des TCP purs, sans
#                                       appel à messages.create() ni search().
#   python health_check.py --ci         Mode CI/CD : sortie minimale, exit code strict
#   python health_check.py --feeds      Teste uniquement les flux RSS (verbose)
#   python health_check.py --db         Teste uniquement la base SQLite
# ─────────────────────────────────────────────────────────────────────────────
# Codes de sortie :
#   0  — Tout est OK (pipeline opérationnel)
#   1  — Avertissements non-bloquants (Tavily absent, feeds lents, etc.)
#   2  — Erreurs critiques (Python < 3.10, ANTHROPIC_API_KEY manquante,
#         scripts core absents, DB corrompue)
#
# Quand utiliser quel mode :
#   Dev local         → --offline  (vitesse, pas de bruit réseau)
#   Avant déploiement → standard   (vérification complète)
#   CI/CD pipeline    → --ci       (sortie minimale, exit code strict)
# ─────────────────────────────────────────────────────────────────────────────

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


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ═════════════════════════════════════════════════════════════════════════════

VERSION      = "3.40"   # POST-FIX1 : était "3.38"
HC_TIMEOUT   = 8
SMTP_TIMEOUT = 10
DISK_MIN_MB  = 500
DISK_WARN_MB = 200

OFFLINE    = "--offline" in sys.argv
CI_MODE    = "--ci"      in sys.argv
FEEDS_ONLY = "--feeds"   in sys.argv
DB_ONLY    = "--db"      in sys.argv

if CI_MODE or not sys.stdout.isatty():
    OK   = "[OK]  "
    WARN = "[WARN]"
    FAIL = "[FAIL]"
    INFO = "[INFO]"
else:
    OK   = "\u001B[92m[OK]  \u001B[0m"
    WARN = "\u001B[93m[WARN]\u001B[0m"
    FAIL = "\u001B[91m[FAIL]\u001B[0m"
    INFO = "\u001B[94m[INFO]\u001B[0m"

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
}

ENV_VARS: dict[str, tuple[str, str]] = {
    "ANTHROPIC_API_KEY":   ("critique",  "Clé API Anthropic — requise pour les rapports Claude"),
    "TAVILY_API_KEY":      ("optionnel", "Tavily web search — désactive la vérif web si absent"),
    "SMTP_USER":           ("warning",   "Adresse Gmail pour envoi des rapports"),
    "SMTP_PASS":           ("warning",   "Mot de passe application Gmail (≠ mot de passe compte)"),
    "REPORT_EMAIL":        ("warning",   "Destinataire(s) du rapport — défaut = SMTP_USER"),
    "SENTINEL_MODEL":      ("warning",   "Modèle Claude (ex: claude-sonnet-4-6) — défaut si absent"),
    "HAIKU_MODEL":         ("warning",   "Modèle Haiku pour compressions (ex: claude-haiku-4-5)"),
    "SAM_GOV_API_KEY":     ("optionnel", "API SAM.gov DoD — contrats défense US (gratuit)"),
    "SENTINEL_DB":         ("optionnel", "Chemin SQLite — défaut: data/sentinel.db"),
    "SENTINEL_MAX_TOKENS": ("optionnel", "Max tokens sortie Claude — défaut: 16000"),
    "SENTINEL_TAVILY_MAX": ("optionnel", "Max appels Tavily/rapport — défaut: 5"),
    "OLLAMA_TIMEOUT":      ("optionnel", "Timeout Ollama local en secondes — défaut: 180"),
}

_results:        list[dict[str, Any]] = []
_critical_count: int = 0
_warning_count:  int = 0


def _record(level: str, category: str, name: str, message: str, detail: str = "") -> None:
    """
    POST-FIX5 : ajout branche "info" — avant, _record("info", ...) tombait dans
    le else → log.error() + _critical_count++. Niveaux : ok|info|warning|critical.
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
    icon = OK if level == "ok" else WARN if level in ("warning", "info") else FAIL
    line = f"{icon} [{category}] {name}: {message}"
    if detail and not CI_MODE:
        line += f"
       → {detail}"

    if level == "ok":
        log.info(line)
    elif level == "info":
        log.info(line)
    elif level == "warning":
        log.warning(line)
        _warning_count += 1
    else:
        log.error(line)
        _critical_count += 1


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 1 — Python & pip (C-4)
# ═════════════════════════════════════════════════════════════════════════════

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
                   .replace("dotenv", "python-dotenv")
                   .replace("sklearn", "scikit-learn"))
            _record(lvl, "Package", pkg, "ABSENT", f"pip install {fix}")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 2 — Structure fichiers & dossiers
# ═════════════════════════════════════════════════════════════════════════════

def check_structure() -> None:
    for script in CORE_SCRIPTS:
        p = Path(script)
        if p.exists():
            _record("ok", "Script", script, f"présent ({p.stat().st_size // 1024} KB) ✓")
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


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 3 — Variables d'environnement (HC-FIX4 / CODE-5)
# ═════════════════════════════════════════════════════════════════════════════

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
            _record("ok", "ENV", "SENTINEL_MODEL_FORMAT", f"format valide ✓ ({model})")
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


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 4 — SQLite WAL (HC-FIX6 / BUG-DB1 / BUG-DB3 / MAINT-DB3)
# ═════════════════════════════════════════════════════════════════════════════

def check_sqlite() -> dict[str, Any]:
    db_path = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
    result: dict[str, Any] = {}

    if not db_path.exists():
        _record("warning", "SQLite", "fichier",
                f"absent : {db_path}",
                "Normal au 1er déploiement — créé au premier run de sentinel_main.py")
        return result

    size_kb = db_path.stat().st_size // 1024
    _record("ok", "SQLite", "fichier", f"présent ({size_kb} KB) — {db_path} ✓")

    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.row_factory = sqlite3.Row

        jmode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if jmode.lower() == "wal":
            _record("ok", "SQLite", "journal_mode", "WAL ✓")
        else:
            _record("warning", "SQLite", "journal_mode",
                    f"mode '{jmode}' (WAL attendu)",
                    "Sera corrigé par db_manager.create_connection() au prochain run")

        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if ic == "ok":
            _record("ok", "SQLite", "integrity_check", "aucune corruption ✓")
        else:
            _record("critical", "SQLite", "integrity_check",
                    f"CORRUPTION : {ic}",
                    f"Restaurer backup ou supprimer {db_path} et relancer sentinel_main.py")

        expected = {"reports", "tendances", "alertes", "acteurs", "seenhashes", "metrics"}
        existing = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        missing_t = expected - existing
        if missing_t:
            _record("warning", "SQLite", "tables",
                    f"tables manquantes : {missing_t}",
                    "Sera créé par db_manager DDL au prochain connect()")
        else:
            _record("ok", "SQLite", "tables", "6/6 tables présentes ✓")

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

        expected_idx = {
            "idx_reports_date", "idx_seenhashes_date",
            "idx_tendances_active", "idx_alertes_active", "idx_acteurs_score",
        }
        existing_idx = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        missing_idx = expected_idx - existing_idx
        if missing_idx:
            _record("warning", "SQLite", "index",
                    f"index manquants : {missing_idx}",
                    "Seront créés par db_manager DDL au prochain connect()")
        else:
            _record("ok", "SQLite", "index", f"{len(existing_idx)} index présents ✓")

        for table in expected & existing:
            try:
                result[f"count_{table}"] = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            except sqlite3.Error:
                result[f"count_{table}"] = -1

        _record("ok", "SQLite", "données",
                f"{result.get('count_reports', 0)} rapports | "
                f"{result.get('count_seenhashes', 0)} seenhashes | "
                f"{result.get('count_alertes', 0)} alertes | "
                f"{result.get('count_tendances', 0)} tendances | "
                f"{result.get('count_acteurs', 0)} acteurs")

        if (db_path.parent / ".migrationdonev3.37").exists():
            _record("ok", "SQLite", "migration_json", "migration JSON→SQLite effectuée ✓")
        else:
            _record("warning", "SQLite", "migration_json",
                    "flag migration absent",
                    "Si sentinel_memory.json existe, appeler run_migration() dans db_manager.py")

        conn.close()

    except sqlite3.Error as e:
        _record("critical", "SQLite", "connexion",
                f"Erreur SQLite : {e}",
                f"Vérifier {db_path} — corrompu ou verrou actif (autre process ?)")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 5 — Flux RSS parallèle (HC-FIX7)
# ═════════════════════════════════════════════════════════════════════════════

def _check_one_feed(args: tuple[str, str, str]) -> dict[str, Any]:
    url, source, score = args
    t0 = time.monotonic()
    try:
        import requests as req
        r = req.get(url, timeout=HC_TIMEOUT,
                    headers={"User-Agent": f"SENTINEL-HEALTHCHECK/{VERSION}"},
                    allow_redirects=True)
        ms = int((time.monotonic() - t0) * 1000)
        return {"source": source, "url": url, "score": score,
                "status": r.status_code,
                "ok": r.status_code == 200 and len(r.content) > 100,
                "size": len(r.content), "ms": ms}
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        return {"source": source, "url": url, "score": score,
                "status": 0, "ok": False, "error": str(e)[:120], "ms": ms}


def check_rss_feeds(verbose: bool = False) -> list[dict]:
    try:
        from scraper_rss import RSS_FEEDS  # type: ignore
    except ImportError:
        _record("warning", "RSS", "import",
                "scraper_rss.py non importable — vérif feeds ignorée",
                "Vérifier la syntaxe de scraper_rss.py")
        return []

    total = len(RSS_FEEDS)
    log.info(f"RSS — test de {total} flux (timeout {HC_TIMEOUT}s, 15 threads)...")

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


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 6 — Anthropic API reachability
# ═════════════════════════════════════════════════════════════════════════════

def check_anthropic_api() -> None:
    if OFFLINE:
        _record("warning", "API", "anthropic", "skipped (--offline)")
        return
    if not os.environ.get("ANTHROPIC_API_KEY"):
        _record("critical", "API", "anthropic", "ANTHROPIC_API_KEY absente — non testable")
        return
    try:
        sock = socket.create_connection(("api.anthropic.com", 443), timeout=8)
        sock.close()
        _record("ok", "API", "anthropic", "api.anthropic.com:443 accessible ✓")
    except socket.gaierror as e:
        _record("critical", "API", "anthropic",
                f"DNS resolution failed : {e}", "Vérifier connexion Internet / proxy")
    except OSError as e:
        _record("critical", "API", "anthropic",
                f"TCP connexion échouée : {e}", "Port 443 bloqué par firewall ?")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 7 — Tavily (HC-FIX10 / R6A3-NEW-2)
# ═════════════════════════════════════════════════════════════════════════════

def check_tavily() -> None:
    if OFFLINE:
        return
    if not os.environ.get("TAVILY_API_KEY"):
        _record("warning", "API", "tavily",
                "TAVILY_API_KEY absente — vérification web désactivée (non bloquant)",
                "Gratuit 1000 req/mois sur tavily.com — 5 req/rapport max")
        return
    try:
        sock = socket.create_connection(("api.tavily.com", 443), timeout=8)
        sock.close()
        _record("ok", "API", "tavily", "api.tavily.com:443 accessible ✓")
    except OSError as e:
        _record("warning", "API", "tavily",
                f"Tavily inaccessible : {e} (non bloquant)")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 8 — SMTP (HC-FIX11 / K-6)
# ═════════════════════════════════════════════════════════════════════════════

def check_smtp() -> None:
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
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
        _record("ok", "SMTP", "auth",
                f"Auth OK — {smtp_user} → {smtp_host}:{smtp_port} ✓")
    except smtplib.SMTPAuthenticationError:
        _record("critical", "SMTP", "auth",
                "Authentification SMTP ÉCHOUÉE",
                "Gmail : utiliser un mot de passe d'application (≠ mot de passe compte). "
                "compte.google.com → Sécurité → Mots de passe d'applications")
    except (smtplib.SMTPException, OSError) as e:
        _record("warning", "SMTP", "connexion",
                f"SMTP inaccessible : {str(e)[:80]} (warning non bloquant)",
                "Vérifier SMTP_HOST/SMTP_PORT ou connectivité réseau")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 9 — Espace disque (HC-FIX8 / CDC-4)
# ═════════════════════════════════════════════════════════════════════════════

def check_disk_space() -> None:
    try:
        usage    = shutil.disk_usage(".")
        free_mb  = usage.free  // (1024 * 1024)
        total_mb = usage.total // (1024 * 1024)
        used_pct = 100 * usage.used // usage.total

        if free_mb < DISK_WARN_MB:
            _record("critical", "Disque", "espace_libre",
                    f"{free_mb} MB libres ({used_pct}% utilisé) — CRITIQUE",
                    f"< {DISK_WARN_MB} MB : pipeline risque d'échouer. "
                    "find output -name '*.html' -mtime +30 -delete")
        elif free_mb < DISK_MIN_MB:
            _record("warning", "Disque", "espace_libre",
                    f"{free_mb} MB libres ({used_pct}% utilisé)",
                    f"< {DISK_MIN_MB} MB recommandés. "
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
        _record("warning", "Disque", "espace_libre", f"Impossible de vérifier : {e}")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 10 — Dernier run pipeline
# ═════════════════════════════════════════════════════════════════════════════

def check_last_run() -> None:
    log_file = Path("logs/sentinel.log")
    if not log_file.exists():
        _record("warning", "Pipeline", "dernier_run",
                "logs/sentinel.log absent",
                "Normal au 1er déploiement. Lancer : python sentinel_main.py")
        return

    lines = log_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    tokens_ok = ("terminé", "SENTINEL v", "pipeline terminé", "Rapport disponible")
    last_ok_dt: datetime | None = None

    for line in reversed(lines[-500:]):
        if any(tok in line for tok in tokens_ok):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    last_ok_dt = datetime.strptime(line[:19], fmt)
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
        age_h = (datetime.now() - last_ok_dt).total_seconds() / 3600
        lvl = "ok" if age_h <= 30 else "warning"
        _record(lvl, "Pipeline", "dernier_run",
                f"Dernier run OK il y a {age_h:.1f}h "
                f"({last_ok_dt.strftime('%Y-%m-%d %H:%M')})" +
                (" ✓" if lvl == "ok" else " — vérifier le cron"),
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
        _record("ok", "Pipeline", "erreurs_recentes", "Aucune erreur critique récente ✓")


# ═════════════════════════════════════════════════════════════════════════════
# VÉR. 11 — WeasyPrint / libs système
# ═════════════════════════════════════════════════════════════════════════════

def check_system_deps() -> None:
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
            _record("ok", "Système", "weasyprint", "installé (Windows/macOS) ✓")
    except ImportError:
        _record("warning", "Système", "weasyprint",
                "non installé — PDF désactivé",
                "pip install weasyprint  +  sudo apt libcairo2 libpango-1.0-0 (Linux)")


# ═════════════════════════════════════════════════════════════════════════════
# RAPPORT JSON & ALERTE EMAIL (HC-FIX12 / HC-FIX13)
# ═════════════════════════════════════════════════════════════════════════════

def _save_report(report: dict) -> None:
    """Sauvegarde atomique du rapport JSON (HC-FIX12)."""
    Path("logs").mkdir(exist_ok=True)
    out = Path("logs/health_report.json")
    tmp = out.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(out)
        log.info(f"Rapport JSON sauvegardé : {out}")
    except OSError as e:
        log.warning(f"Impossible de sauvegarder rapport JSON : {e}")


def _send_alert_email(report: dict) -> None:
    """HC-FIX13 — Alerte email si erreurs critiques + SMTP configuré."""
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_PASSWORD", "")
    report_to = os.environ.get("REPORT_EMAIL", smtp_user)
    if not smtp_user or not smtp_pass or not report_to:
        return
    criticals = [r for r in _results if r["level"] == "critical"]
    if not criticals:
        return
    lines = [f"SENTINEL v{VERSION} — HealthCheck : {len(criticals)} erreur(s) critique(s)
"]
    lines += [f"  [FAIL] [{r['category']}] {r['name']}: {r['message']}"
              for r in criticals[:10]]
    if len(criticals) > 10:
        lines.append(f"  ... et {len(criticals) - 10} autres erreurs")
    lines.append("
Rapport complet : logs/health_report.json")
    msg = MIMEText("
".join(lines), "plain", "utf-8")
    msg["Subject"] = f"SENTINEL HealthCheck — {len(criticals)} erreur(s) critique(s)"
    msg["From"]    = smtp_user
    msg["To"]      = report_to
    try:
        smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", "587"))
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.starttls()
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


# ═════════════════════════════════════════════════════════════════════════════
# M3-FIX — TEST DE NORMALITÉ SHAPIRO-WILK + SEUIL ADAPTATIF
# F4-FIX  — TESTS DE RÉGRESSION ANALYTIQUE MENSUELS
# ═════════════════════════════════════════════════════════════════════════════

def check_distribution_normality() -> None:
    """
    M3-FIX : teste la normalité des indices d'activité via Shapiro-Wilk.
    POST-FIX5 : _record("info", ...) correctement géré — plus de faux critique.
    """
    try:
        from db_manager import SentinelDB
        metrics = SentinelDB.getmetrics(ndays=90)
        indices = [float(m["indice"]) for m in metrics if m.get("indice")]

        if len(indices) < 30:
            _record("info", "Statistiques", "normalite",
                    f"Données insuffisantes ({len(indices)} points — min 30 requis)",
                    "Relancer après 30 jours de production.")
            return

        try:
            from scipy import stats as _scipy_stats
            stat, p_value = _scipy_stats.shapiro(indices)
            is_normal = p_value >= 0.05
            mu    = sum(indices) / len(indices)
            sigma = (sum((x - mu)**2 for x in indices) / len(indices)) ** 0.5
            k     = float(os.environ.get("SENTINEL_ALERT_SIGMA", "2.0"))
            shewhart_threshold = mu + k * sigma
            empirical_p95 = sorted(indices)[int(0.95 * len(indices))]

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
                    "scipy absent — test Shapiro-Wilk désactivé (pip install scipy)")

    except Exception as exc:
        _record("warning", "Statistiques", "normalite", f"Erreur : {exc}")


def check_regression_analytique() -> None:
    """
    F4-FIX : vérifie la cohérence des données mémoire sur 7 jours.

    POST-FIX6 : corrige la fausse alerte permanente.
    Avant : cherchait 'nouvelles_tendances' dans reports.compressed qui contient
    json.dumps(metrics) — jamais les clés mémoire → ratio 100% → fausse alerte.
    Après : lit directement les tables tendances et alertes (insérées par
    SentinelDB.savetendance() et SentinelDB.ouvrirealerte() dans sentinel_main.py).
    """
    try:
        from db_manager import SentinelDB

        reports = SentinelDB.getrecentreports(ndays=7)
        if len(reports) < 3:
            _record("info", "Régression", "deltas",
                    f"Moins de 3 rapports ({len(reports)}) — test de régression ignoré")
            return

        db_path = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
        conn = sqlite3.connect(str(db_path), timeout=5)
        conn.row_factory = sqlite3.Row

        try:
            tendances_7j = conn.execute(
                "SELECT COUNT(*) FROM tendances WHERE date >= date('now', '-7 days')"
            ).fetchone()[0]
            alertes_7j = conn.execute(
                "SELECT COUNT(*) FROM alertes "
                "WHERE date_ouverture >= date('now', '-7 days')"
            ).fetchone()[0]
        finally:
            conn.close()

        total_delta = tendances_7j + alertes_7j

        if total_delta == 0:
            _record("warning", "Régression", "deltas",
                    f"F4 Aucune tendance ni alerte sur 7 jours "
                    f"({len(reports)} rapports analysés). "
                    "Possible dérive du modèle Claude ou bloc DEBUTJSONDELTA absent.",
                    "Vérifier les 3 derniers rapports HTML dans output/ et s'assurer "
                    "que prompts/system.txt contient DEBUTJSONDELTA")
        else:
            _record("ok", "Régression", "deltas",
                    f"F4 Mémoire active : {tendances_7j} tendance(s) + "
                    f"{alertes_7j} alerte(s) sur 7j ✓")

    except Exception as exc:
        _record("warning", "Régression", "deltas", f"F4 Erreur : {exc}")


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def run_health_check() -> int:
    t0  = time.monotonic()
    now = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info(f"SENTINEL v{VERSION} — HealthCheck démarré ({now})")
    log.info(f"Mode : {'OFFLINE' if OFFLINE else 'ONLINE'}"
             f"{' | CI' if CI_MODE else ''}"
             f"{' | FEEDS-ONLY' if FEEDS_ONLY else ''}"
             f"{' | DB-ONLY' if DB_ONLY else ''}")
    log.info("=" * 60)

    if FEEDS_ONLY:
        check_rss_feeds(verbose=True)
    elif DB_ONLY:
        check_sqlite()
    else:
        check_python()
        check_packages()
        check_structure()
        check_env_vars()
        check_sqlite()
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

    if not OFFLINE:
        _send_alert_email(report)

    if _critical_count > 0:
        return 2
    if _warning_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run_health_check())
