#!/usr/bin/env python3
# sentinel_main.py --- SENTINEL v3.44 --- ORCHESTRATEUR PRINCIPAL
# =============================================================================
# v3.41 — Corrections post-audit :
#   MAIN-41-FIX1  SONNET_MODEL transmis à run_sentinel()
#   MAIN-41-FIX2  articles.extend(tg_arts or []) — guard None
#   MAIN-41-FIX3  Port SENTINEL_HEALTH_PORT : try/except ValueError
#   MAIN-41-FIX4  _load_optional_scraper() : guard spec/loader None
#   MAIN-41-FIX5  run_monthly_report() non vide — warning + stub documenté
#   MAIN-41-FIX6  Wrapper log() supprimé — logger.info/warning/error directs
#   MAIN-41-FIX7  Crash pipeline : tentative notification email (non bloquant)
#   MAIN-41-FIX8  REPORT_RETENTION_DAYS depuis ENV (n'était pas configurable)
#
# v3.42 — Intégration procurement :
#   MAIN-42-FIX1  run_all_procurement() intégré dans main() après scrape RSS
#
# v3.43 — Observabilité & rapport mensuel :
#   MAIN-43-FIX1  _timed() — helper de timing par module
#   MAIN-43-FIX2  run_monthly_report() — implémentation MapReduce
#                 Phase MAP  : Haiku compresse chaque rapport journalier
#                 Phase REDUCE: Sonnet synthétise les 4 résumés hebdo
#
# v3.44 — Performance & robustesse :
#   MAIN-44-FIX1  Parallélisation de la phase MAP dans run_monthly_report()
#                 via ThreadPoolExecutor(max_workers=8).
#                 Les 30 compressions Haiku étant indépendantes, elles
#                 s'exécutent en ~3-5s au lieu de ~30s en séquentiel.
#                 Même logique que samgov_scraper.py (50 keywords) et
#                 scraper_rss.py (72 flux).
#   MAIN-44-FIX2  _check_dependencies() — validation des dépendances au
#                 démarrage de main() ET run_monthly_report().
#                 Détecte les imports manquants AVANT d'entrer dans le
#                 pipeline, évite un crash en plein milieu de nuit.
#                 Placé dans une fonction dédiée (et non dans init_logging()
#                 qui ne gère que la configuration du logger).
# =============================================================================

import sys
import datetime
import json
import logging
import time as timeperf
import importlib.util as ilu
import os
from pathlib import Path
from typing import Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# CONFIGURATION & VERSION
# ---------------------------------------------------------------------------
VERSION      = "3.44"
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")

REPORT_RETENTION_DAYS = int(
    os.environ.get("SENTINEL_RETENTION_DAYS", "30")
)  # MAIN-41-FIX8

# Structure des dossiers
for d in ["logs", "data", "output", "backups"]:
    Path(d).mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING INDUSTRIEL
# ---------------------------------------------------------------------------
def init_logging():
    """Configure le logger structuré JSON. Ne fait QUE ça."""
    logging.basicConfig(
        level=logging.INFO,
        format='{"ts":"%(asctime)s","lvl":"%(levelname)s","v":"' + VERSION + '","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.FileHandler("logs/sentinel.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

init_logging()
logger = logging.getLogger("SENTINEL")

# ---------------------------------------------------------------------------
# VALIDATION DES DÉPENDANCES — MAIN-44-FIX2
# ---------------------------------------------------------------------------
# Placé ici (et NON dans init_logging()) : init_logging() ne configure que
# le logger. Mélanger deux responsabilités différentes dans une même fonction
# viole le principe de responsabilité unique et rendrait les tests difficiles.
# _check_dependencies() est appelée explicitement en début de main() et de
# run_monthly_report(), avant toute logique métier.
# ---------------------------------------------------------------------------
_REQUIRED_DAILY = [
    ("scraper_rss",    "scrape_all_feeds"),
    ("scraper_rss",    "format_for_claude"),
    ("memory_manager", "get_compressed_memory"),
    ("memory_manager", "update_memory"),
    ("sentinel_api",   "run_sentinel"),
    ("sentinel_api",   "extract_metrics_from_report"),
    ("sentinel_api",   "extract_memory_delta"),
    ("charts",         "generate_all_charts"),
    ("report_builder", "build_html_report"),
    ("report_builder", "purge_old_reports"),
    ("mailer",         "send_report"),
    ("db_manager",     "SentinelDB"),
    ("samgov_scraper", "run_all_procurement"),
]

_REQUIRED_MONTHLY = [
    ("memory_manager", "compress_text_haiku"),
    ("sentinel_api",   "run_sentinel_monthly"),
]

def _check_dependencies(extra: list[tuple[str, str]] | None = None) -> list[str]:
    """
    Vérifie que toutes les dépendances critiques sont importables et exposent
    les fonctions/classes attendues.

    Retourne la liste des manquants sous forme ["module.func()", ...].
    Liste vide = tout est OK.

    Paramètres :
        extra : dépendances supplémentaires à vérifier en plus des dépendances
                quotidiennes de base (ex : _REQUIRED_MONTHLY pour le mensuel).
    """
    checks = _REQUIRED_DAILY + (extra or [])
    missing: list[str] = []

    for module_name, attr_name in checks:
        try:
            mod = __import__(module_name)
            if not hasattr(mod, attr_name):
                missing.append(f"{module_name}.{attr_name}()")
        except ImportError:
            missing.append(f"{module_name} (module absent)")

    return missing

# ---------------------------------------------------------------------------
# TIMING PAR MODULE — MAIN-43-FIX1
# ---------------------------------------------------------------------------
def _timed(label: str, fn: Callable, *args: Any, **kwargs: Any) -> Any:
    """
    Exécute fn(*args, **kwargs), logue le temps écoulé avec le label donné,
    et retourne le résultat.

    Log produit :
        {"ts":"...","lvl":"INFO","v":"3.44","msg":"TIMING scrape_rss : 18.3s"}
    """
    t0 = timeperf.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = timeperf.perf_counter() - t0
    logger.info(f"TIMING {label} : {elapsed:.1f}s")
    return result

# ---------------------------------------------------------------------------
# HEALTH SERVER (Idempotent)
# ---------------------------------------------------------------------------
_HEALTH_SERVER_STARTED = False

def _ensure_health_server():
    global _HEALTH_SERVER_STARTED
    if _HEALTH_SERVER_STARTED:
        return

    # MAIN-41-FIX3 : protection ValueError si SENTINEL_HEALTH_PORT mal formé
    try:
        port = int(os.environ.get("SENTINEL_HEALTH_PORT", "8765"))
    except ValueError:
        logger.warning("SENTINEL_HEALTH_PORT invalide — utilisation du port 8765 par défaut")
        port = 8765

    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ["/health", "/"]:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                status = {
                    "status":  "ok",
                    "version": VERSION,
                    "ts":      datetime.datetime.now().isoformat(),
                }
                self.wfile.write(json.dumps(status).encode())
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, *args): pass

    def run_server():
        try:
            httpd = HTTPServer(("0.0.0.0", port), HealthHandler)
            httpd.serve_forever()
        except Exception:
            pass

    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    _HEALTH_SERVER_STARTED = True
    logger.info(f"Health server actif sur port {port}")

# ---------------------------------------------------------------------------
# IMPORTS DES MODULES SENTINEL
# ---------------------------------------------------------------------------
from scraper_rss import scrape_all_feeds, format_for_claude
from memory_manager import get_compressed_memory, update_memory
from sentinel_api import run_sentinel, extract_metrics_from_report, extract_memory_delta
from charts import generate_all_charts
from report_builder import build_html_report, purge_old_reports
from mailer import send_report
from db_manager import SentinelDB, closedb, initdb
from samgov_scraper import run_all_procurement  # MAIN-42-FIX1

# ---------------------------------------------------------------------------
# SCRAPERS OPTIONNELS
# ---------------------------------------------------------------------------
def _load_optional_scraper(name: str, module_path: str):
    if Path(module_path).exists():
        try:
            spec = ilu.spec_from_file_location(name, module_path)
            # MAIN-41-FIX4 : guard spec/loader None
            if spec is None or spec.loader is None:
                raise ImportError(f"Spec introuvable ou loader absent pour {module_path}")
            mod = ilu.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
        except Exception as e:
            logger.warning(f"Erreur chargement {name}: {e}")
    return None

telegram_mod = _load_optional_scraper("telegram_scraper", "telegram_scraper.py")
patents_mod  = _load_optional_scraper("ops_patents",       "ops_patents.py")

# ---------------------------------------------------------------------------
# LOGIQUE DE SAUVEGARDE (Centralisée)
# ---------------------------------------------------------------------------
def _process_report_data_to_db(report_text: str, metrics: dict, date_iso: str):
    """Sauvegarde les métriques, acteurs, tendances et alertes."""
    SentinelDB.savemetrics(
        date_iso,
        indice=metrics.get("indice", 5.0),
        alerte=metrics.get("alerte", "VERT"),
        nb_articles=metrics.get("nb_articles", 0),
        nb_pertinents=metrics.get("nb_pertinents", 0),
    )

    SentinelDB.savereport(
        date=date_iso,
        indice=metrics.get("indice", 5.0),
        alerte=metrics.get("alerte", "VERT"),
        compressed=json.dumps(metrics),
        rawtail=report_text[:3000],
    )

    deltas = extract_memory_delta(report_text)
    for t in deltas.get("nouvelles_tendances", []):
        SentinelDB.savetendance(t, date_iso)
    for a in deltas.get("alertes_ouvertes", []):
        SentinelDB.ouvrirealerte(a, date_iso)
    for c in deltas.get("alertes_closes", []):
        SentinelDB.closealerte(c, date_iso)

# ---------------------------------------------------------------------------
# PIPELINE QUOTIDIEN
# ---------------------------------------------------------------------------
def main():
    logger.info(f"=== DEMARRAGE SENTINEL v{VERSION} ===")
    start_time = timeperf.perf_counter()
    initdb()
    _ensure_health_server()

    # MAIN-44-FIX2 : validation dépendances avant toute logique métier
    missing = _check_dependencies()
    if missing:
        logger.error(
            f"DEPENDANCES MANQUANTES — pipeline quotidien annulé : "
            f"{', '.join(missing)}"
        )
        return

    date_iso = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    try:
        # 1. COLLECTE RSS — MAIN-43-FIX1 : chronométré
        articles = _timed("scrape_rss", scrape_all_feeds)

        # Ajout Telegram (si dispo)
        if telegram_mod:
            try:
                tg_arts = _timed("telegram", telegram_mod.run_telegram_scraper)
                articles.extend(tg_arts or [])  # MAIN-41-FIX2
            except Exception as e:
                logger.warning(f"Erreur Telegram: {e}")

        # Ajout Procurement DoD / TED EU / BOAMP FR — MAIN-42-FIX1
        try:
            contracts = _timed("procurement", run_all_procurement, daysback=2)
            articles.extend(contracts)
            logger.info(f"Procurement : {len(contracts)} contrats ajoutés au pipeline")
        except Exception as e:
            logger.warning(f"Erreur Procurement (non bloquant) : {e}")

        if not articles:
            logger.info("Aucune donnée collectée. Fin du cycle.")
            return

        # 2. ANALYSE — MAIN-43-FIX1 : chronométré
        formatted_data = format_for_claude(articles)
        memory_context = get_compressed_memory()

        # MAIN-41-FIX1 : SONNET_MODEL transmis à run_sentinel()
        report_text, _ = _timed(
            "claude_daily",
            run_sentinel,
            formatted_data,
            memory_context,
            model=SONNET_MODEL,
        )
        if not report_text:
            raise RuntimeError("Claude n'a retourné aucun rapport.")

        # 3. EXTRACTION & DB
        metrics = extract_metrics_from_report(report_text)
        _process_report_data_to_db(report_text, metrics, date_iso)

        # 4. RESTITUTION — MAIN-43-FIX1 : chronométré
        _timed("charts",  generate_all_charts, report_text)
        html_report = _timed("report_builder", build_html_report, report_text, metrics)

        if html_report and Path(html_report).exists():
            _timed("mailer", send_report, html_report)
            logger.info(f"Rapport envoyé : {html_report}")

        # 5. MAINTENANCE
        update_memory(report_text)
        purge_old_reports(days=REPORT_RETENTION_DAYS)  # MAIN-41-FIX8

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== CYCLE TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH PIPELINE: {e}", exc_info=True)
        # MAIN-41-FIX7 : tentative de notification email (non bloquant)
        try:
            send_report(None, subject=f"[SENTINEL CRASH] {type(e).__name__}: {e}")
        except Exception:
            pass
    finally:
        closedb()

# ---------------------------------------------------------------------------
# RAPPORT MENSUEL — MAIN-43-FIX2 + MAIN-44-FIX1
# ---------------------------------------------------------------------------
def run_monthly_report():
    """
    Génère le rapport mensuel stratégique SENTINEL via MapReduce.

    Architecture MapReduce :
        Phase MAP    : Haiku compresse chaque rapport journalier → ~300 chars
                       PARALLÉLISÉ via ThreadPoolExecutor(max_workers=8)
                       → ~3-5s au lieu de ~30s en séquentiel (MAIN-44-FIX1)
        Phase REDUCE : 4 résumés hebdo → Sonnet → rapport mensuel final

    Dépendances requises (vérifiées par _check_dependencies au démarrage) :
        - memory_manager.compress_text_haiku(text: str) -> str
        - sentinel_api.run_sentinel_monthly(injection, mois, model) -> tuple
    """
    logger.info(f"=== DEMARRAGE RAPPORT MENSUEL SENTINEL v{VERSION} ===")
    start_time = timeperf.perf_counter()
    initdb()

    # MAIN-44-FIX2 : validation dépendances AVANT toute logique métier
    # On vérifie les dépendances de base + les dépendances spécifiques au mensuel
    missing = _check_dependencies(extra=_REQUIRED_MONTHLY)
    if missing:
        logger.warning(
            f"DEPENDANCES MANQUANTES — rapport mensuel annulé : "
            f"{', '.join(missing)}"
        )
        closedb()
        return

    date_iso = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    mois     = datetime.datetime.now(datetime.timezone.utc).strftime("%B %Y")

    try:
        from memory_manager import compress_text_haiku
        from sentinel_api import run_sentinel_monthly

        # ── Récupération des 30 derniers rapports depuis SQLite ───────────
        rows = SentinelDB.get_last_n_reports(30)
        if not rows:
            logger.warning("run_monthly_report : aucun rapport en base — abandon.")
            return

        logger.info(f"MapReduce mensuel : {len(rows)} rapports récupérés depuis SentinelDB")

        # ── Phase MAP parallèle — MAIN-44-FIX1 ───────────────────────────
        # ThreadPoolExecutor : même pattern que scraper_rss.py (72 flux) et
        # samgov_scraper.py (50 keywords). Les appels Haiku sont I/O-bound
        # (réseau → API Anthropic), donc le threading est efficace.
        # max_workers=8 : équilibre entre parallélisme et limite de rate Anthropic.
        raw_texts: list[str] = [
            (row.get("rawtail") or "").strip()
            for row in rows
        ]
        # On filtre les rapports vides avant de soumettre au pool
        indexed_raws = [(i, t) for i, t in enumerate(raw_texts) if t]

        compressed_reports: list[str] = [""] * len(rows)  # pré-alloué pour l'ordre

        t_map_start = timeperf.perf_counter()

        def _compress_one(args: tuple[int, str]) -> tuple[int, str]:
            """Compresse un rapport et retourne (index_original, résumé)."""
            idx, raw = args
            try:
                return idx, compress_text_haiku(raw)
            except Exception as e:
                logger.warning(f"MAP rapport {idx + 1} : erreur Haiku : {e} — fallback tronqué")
                return idx, raw[:300]

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_compress_one, item): item[0] for item in indexed_raws}
            for future in as_completed(futures):
                try:
                    idx, summary = future.result()
                    compressed_reports[idx] = summary
                except Exception as e:
                    orig_idx = futures[future]
                    logger.warning(f"MAP future {orig_idx + 1} : erreur inattendue : {e}")
                    compressed_reports[orig_idx] = raw_texts[orig_idx][:300]

        # Filtrer les slots vides (rapports initialement vides)
        compressed_reports = [s for s in compressed_reports if s]

        t_map_elapsed = timeperf.perf_counter() - t_map_start
        logger.info(
            f"TIMING haiku_map_parallel ({len(compressed_reports)} rapports) : "
            f"{t_map_elapsed:.1f}s — "
            f"{sum(len(s) for s in compressed_reports)} chars total"
        )

        # ── Phase REDUCE : regroupement par semaine (4 × 7j) ─────────────
        def _chunk(lst: list, size: int) -> list[list]:
            return [lst[i:i + size] for i in range(0, len(lst), size)]

        weekly_blocks = _chunk(compressed_reports, 7)
        weekly_summaries: list[str] = []

        for w_idx, week in enumerate(weekly_blocks, 1):
            week_text = "
---
".join(week)
            try:
                w_summary = _timed(
                    f"haiku_reduce_week_{w_idx}",
                    compress_text_haiku,
                    week_text,
                )
                weekly_summaries.append(f"[SEMAINE {w_idx}]
{w_summary}")
            except Exception as e:
                logger.warning(f"REDUCE semaine {w_idx} : erreur : {e}")
                weekly_summaries.append(f"[SEMAINE {w_idx}]
{week_text[:500]}")

        injection = "

".join(weekly_summaries)
        logger.info(
            f"REDUCE terminé : {len(weekly_summaries)} blocs hebdo "
            f"({len(injection)} chars → Sonnet)"
        )

        # ── Synthèse Sonnet finale ────────────────────────────────────────
        report_text, _ = _timed(
            "claude_monthly",
            run_sentinel_monthly,
            injection,
            mois,
            model=SONNET_MODEL,
        )
        if not report_text:
            raise RuntimeError("run_sentinel_monthly() n'a retourné aucun rapport.")

        # ── Sauvegarde & envoi ────────────────────────────────────────────
        metrics = extract_metrics_from_report(report_text)

        SentinelDB.savereport(
            date=date_iso,
            indice=metrics.get("indice", 5.0),
            alerte=metrics.get("alerte", "VERT"),
            compressed=json.dumps({"type": "monthly", "mois": mois, **metrics}),
            rawtail=report_text[:3000],
        )

        html_report = _timed(
            "report_builder_monthly",
            build_html_report,
            report_text,
            metrics,
        )

        if html_report and Path(html_report).exists():
            _timed("mailer_monthly", send_report, html_report)
            logger.info(f"Rapport mensuel envoyé : {html_report}")

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== RAPPORT MENSUEL TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH RAPPORT MENSUEL: {e}", exc_info=True)
        try:
            send_report(None, subject=f"[SENTINEL CRASH MENSUEL] {type(e).__name__}: {e}")
        except Exception:
            pass
    finally:
        closedb()

# ---------------------------------------------------------------------------
# ENTRÉE PRINCIPALE
# Le 1er du mois : rapport mensuel (synthèse M-1) puis rapport quotidien (J).
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TODAY = datetime.date.today()
    if TODAY.day == 1:
        try:
            run_monthly_report()
        except Exception as e:
            logger.error(f"Erreur rapport mensuel: {e}", exc_info=True)

    main()
