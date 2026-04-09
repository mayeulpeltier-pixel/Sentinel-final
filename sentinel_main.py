#!/usr/bin/env python3
# sentinel_main.py --- SENTINEL v3.43 --- ORCHESTRATEUR PRINCIPAL
# =============================================================================
# Synthèse définitive : meilleurs éléments de toutes les versions.
#
# Corrections & améliorations v3.42 :
# - Logique 1er du mois : main() PUIS run_monthly_report()
# - run_monthly_report() : synthèse sur historique DB
# - Scraping brevets le vendredi via patents_mod
# - Timeout configurable sur run_sentinel() via concurrent.futures
# - SONNET_MODEL injecté dans l'env pour sentinel_api.py
# - Guards tg_arts / p_arts contre None
# - Port health server protégé contre ValueError
# - spec_from_file_location : vérif spec/loader + log explicite
# - Alerte email crash avec détail de l'erreur dans le sujet
# - bare except remplacé par except Exception loggé
# - REPORT_RETENTION_DAYS externalisé en variable d'env
# - rawtail étendu à 4000 caractères
# - Wrapper log() supprimé → logger direct
#
# Ajouts v3.43 :
# - Backup automatique via backup_db() de db_manager (WAL checkpoint inclus)
# - Maintenance DB via SentinelDB.maintenance() — VACUUM + ANALYZE + checkpoint
# - Enregistrement des acteurs stratégiques via SentinelDB.saveacteur()
#   signature correcte : (nom, pays, score, date)
# =============================================================================

import sys
import datetime
import json
import logging
import time as timeperf
import importlib.util as ilu
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# CONFIGURATION & VERSION
# ---------------------------------------------------------------------------
VERSION = "3.43"

# SONNET_MODEL injecté dans l'env pour que sentinel_api.py puisse le lire
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")
os.environ["SENTINEL_MODEL"] = SONNET_MODEL  # garantit la cohérence

REPORT_RETENTION_DAYS = int(os.environ.get("SENTINEL_RETENTION_DAYS", "30"))
RUN_SENTINEL_TIMEOUT  = int(os.environ.get("SENTINEL_API_TIMEOUT", "180"))  # secondes

# Structure des dossiers
for d in ["logs", "data", "output", "backups"]:
    Path(d).mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING INDUSTRIEL
# ---------------------------------------------------------------------------
def init_logging():
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
# HEALTH SERVER (Idempotent)
# ---------------------------------------------------------------------------
_HEALTH_SERVER_STARTED = False

def _ensure_health_server():
    global _HEALTH_SERVER_STARTED
    if _HEALTH_SERVER_STARTED:
        return

    try:
        port = int(os.environ.get("SENTINEL_HEALTH_PORT", "8765"))
    except ValueError:
        logger.warning("SENTINEL_HEALTH_PORT invalide, repli sur 8765")
        port = 8765

    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ["/health", "/"]:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "ok",
                    "version": VERSION,
                    "ts": datetime.datetime.now().isoformat()
                }).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):
            pass  # Supprime les logs HTTP verbeux

    def run_server():
        try:
            httpd = HTTPServer(("0.0.0.0", port), HealthHandler)
            httpd.serve_forever()
        except Exception as e:
            logger.error(f"Health server erreur: {e}")

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
from db_manager import SentinelDB, closedb, initdb, backup_db

# ---------------------------------------------------------------------------
# SCRAPERS OPTIONNELS
# ---------------------------------------------------------------------------
def _load_optional_scraper(name: str, module_path: str):
    """Charge un scraper optionnel depuis un chemin fichier.
    Retourne None proprement si absent ou si le chargement échoue.
    """
    if not Path(module_path).exists():
        return None
    try:
        spec = ilu.spec_from_file_location(name, module_path)
        if spec is None or spec.loader is None:
            logger.warning(f"{name}: spec invalide pour {module_path}, module ignoré")
            return None
        mod = ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    except Exception as e:
        logger.error(f"Erreur chargement {name}: {e}")
    return None

telegram_mod = _load_optional_scraper("telegram_scraper", "telegram_scraper.py")
patents_mod  = _load_optional_scraper("ops_patents", "ops_patents.py")

# ---------------------------------------------------------------------------
# APPEL CLAUDE AVEC TIMEOUT
# ---------------------------------------------------------------------------
def _run_sentinel_with_timeout(formatted_data: str, memory_context: str):
    """Appelle run_sentinel() avec un timeout configurable.
    Lève RuntimeError si le timeout est dépassé.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_sentinel, formatted_data, memory_context)
        try:
            return future.result(timeout=RUN_SENTINEL_TIMEOUT)
        except FuturesTimeout:
            raise RuntimeError(
                f"run_sentinel() timeout après {RUN_SENTINEL_TIMEOUT}s — "
                "vérifier la connexion à l'API Claude."
            )

# ---------------------------------------------------------------------------
# SAUVEGARDE DB CENTRALISÉE
# ---------------------------------------------------------------------------
def _process_report_data_to_db(report_text: str, metrics: dict, date_iso: str):
    """Sauvegarde métriques, rapport brut, tendances et alertes."""
    # 1. Métriques globales
    SentinelDB.savemetrics(
        date_iso,
        indice=metrics.get("indice", 5.0),
        alerte=metrics.get("alerte", "VERT"),
        nb_articles=metrics.get("nb_articles", 0),
        nb_pertinents=metrics.get("nb_pertinents", 0)
    )

    # 2. Rapport brut
    SentinelDB.savereport(
        date=date_iso,
        indice=metrics.get("indice", 5.0),
        alerte=metrics.get("alerte", "VERT"),
        compressed=json.dumps(metrics),
        rawtail=report_text[:4000]
    )

    # 3. Deltas (Tendances & Alertes)
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

    date_iso = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    try:
        # 1. COLLECTE
        articles = scrape_all_feeds()

        # Telegram (optionnel)
        if telegram_mod:
            try:
                tg_arts = telegram_mod.run_telegram_scraper()
                articles.extend(tg_arts or [])
            except Exception as e:
                logger.warning(f"Erreur Telegram: {e}")

        # Brevets OPS — uniquement le vendredi
        if datetime.datetime.now().weekday() == 4 and patents_mod:
            try:
                p_arts = patents_mod.run_uspto_watch()
                articles.extend(p_arts or [])
            except Exception as e:
                logger.warning(f"Erreur Patents: {e}")

        if not articles:
            logger.warning("Aucune donnée collectée. Fin du cycle.")
            return

        # 2. ANALYSE
        formatted_data = format_for_claude(articles)
        memory_context = get_compressed_memory()

        report_text, _ = _run_sentinel_with_timeout(formatted_data, memory_context)
        if not report_text:
            raise RuntimeError("Claude n'a retourné aucun rapport.")

        # 3. EXTRACTION & DB
        metrics = extract_metrics_from_report(report_text)
        _process_report_data_to_db(report_text, metrics, date_iso)

        # 4. RESTITUTION
        generate_all_charts(report_text)
        html_report = build_html_report(report_text, metrics)

        if html_report and Path(html_report).exists():
            send_report(html_report)
            logger.info(f"Rapport quotidien envoyé: {html_report}")

        # 5. MAINTENANCE
        update_memory(report_text)
        purge_old_reports(days=REPORT_RETENTION_DAYS)

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== CYCLE TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH PIPELINE: {e}", exc_info=True)
        try:
            send_report(None, subject=f"[SENTINEL v{VERSION}] CRASH PIPELINE — {e}")
        except Exception as mail_err:
            logger.error(f"Impossible d'envoyer l'alerte crash: {mail_err}")
    finally:
        closedb()

# ---------------------------------------------------------------------------
# RAPPORT MENSUEL (Synthèse stratégique sur 30 jours)
# ---------------------------------------------------------------------------
def run_monthly_report():
    """Génère un rapport stratégique mensuel depuis l'historique DB des 30 derniers jours.
    Appelé après main() le 1er de chaque mois.
    """
    logger.info("=== DEMARRAGE SYNTHESE MENSUELLE STRATEGIQUE ===")
    start_time = timeperf.perf_counter()
    initdb()

    date_iso = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    try:
        # Récupération de l'historique des 30 derniers rapports depuis la DB
        history = SentinelDB.getrecentreports(ndays=30)
        if not history:
            logger.warning("Pas assez de données (30j) pour le rapport mensuel.")
            return

        # Formatage texte des rapports historiques pour Claude
        formatted_history = "

".join([
            f"DATE: {r['date']}
{r['rawtail']}" for r in history
        ])
        memory_context = get_compressed_memory()

        report_text, _ = _run_sentinel_with_timeout(formatted_history, memory_context)
        if not report_text:
            raise RuntimeError("Claude n'a retourné aucun rapport mensuel.")

        # Extraction métriques + sauvegarde DB standard
        metrics = extract_metrics_from_report(report_text)
        _process_report_data_to_db(report_text, metrics, date_iso)

        # Enregistrement des acteurs stratégiques identifiés
        # saveacteur signature réelle : (nom: str, pays: str, score: float, date: str)
        deltas  = extract_memory_delta(report_text)
        acteurs = deltas.get("acteurs_strategiques", [])
        if acteurs:
            for acteur in acteurs:
                try:
                    if isinstance(acteur, dict):
                        SentinelDB.saveacteur(
                            nom=acteur.get("nom", str(acteur)),
                            pays=acteur.get("pays", ""),
                            score=float(acteur.get("score", 0.0)),
                            date=date_iso
                        )
                    else:
                        SentinelDB.saveacteur(
                            nom=str(acteur),
                            pays="",
                            score=0.0,
                            date=date_iso
                        )
                except Exception as e:
                    logger.warning(f"Erreur saveacteur ({acteur}): {e}")
            logger.info(f"{len(acteurs)} acteur(s) stratégique(s) enregistré(s)")
        else:
            logger.info("Aucun acteur stratégique identifié ce mois-ci")

        # Génération et envoi du rapport HTML mensuel
        generate_all_charts(report_text)
        html_report = build_html_report(report_text, metrics, monthly=True)

        if html_report and Path(html_report).exists():
            send_report(html_report, subject=f"[SENTINEL] Rapport mensuel — {date_iso}")
            logger.info(f"Rapport mensuel envoyé: {html_report}")

        update_memory(report_text)

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== RAPPORT MENSUEL TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH RAPPORT MENSUEL: {e}", exc_info=True)
        try:
            send_report(None, subject=f"[SENTINEL v{VERSION}] CRASH MENSUEL — {e}")
        except Exception as mail_err:
            logger.error(f"Impossible d'envoyer l'alerte crash mensuel: {mail_err}")
    finally:
        closedb()

# ---------------------------------------------------------------------------
# POINT D'ENTRÉE
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Pipeline quotidien — exécuté TOUS les jours sans exception
    main()

    # Le 1er du mois : backup + maintenance + synthèse mensuelle
    # (lancés APRÈS le quotidien pour bénéficier des données fraîches du jour)
    if datetime.date.today().day == 1:

        # 1. Backup atomique via backup_db() (WAL checkpoint + purge rotation)
        try:
            backup_db(dest_dir="backups", keep_days=365)
            logger.info("Backup DB mensuel terminé")
        except Exception as e:
            logger.error(f"Backup DB échoué : {e}")

        # 2. Maintenance DB (VACUUM + ANALYZE + WAL checkpoint)
        try:
            SentinelDB.maintenance()
            logger.info("Maintenance DB terminée (VACUUM + ANALYZE)")
        except Exception as e:
            logger.error(f"Maintenance DB échouée : {e}")

        # 3. Rapport mensuel stratégique
        try:
            run_monthly_report()
        except Exception as e:
            logger.error(f"Erreur déclenchement rapport mensuel: {e}", exc_info=True)
