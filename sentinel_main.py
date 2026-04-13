#!/usr/bin/env python3
# sentinel_main.py — SENTINEL v3.60 — ORCHESTRATEUR PRINCIPAL
# =============================================================================
# [historique v3.41 → v3.54 conservé dans le dépôt Git — omis ici]
#
# v3.52 :
#   MAIN-52-FIX1  Guard + log WARNING si github_days_back rejeté par DB
#   MAIN-52-FIX2  deltas = extract_memory_delta(...) or {} — guard None
#   MAIN-52-FIX3  _should_run_github() → (bool, int, str|None) — une seule lecture
#   MAIN-52-FIX4  Guard "if not filled" avant REDUCE
#   MAIN-52-FIX5  MAX_CONTRACTS — cap contrats avant extension articles
#   MAIN-52-FIX6  chr(10) → "
" dans les f-strings
#   MAIN-52-FIX7  Lock threading sur _ensure_health_server()
#
# v3.53 :
#   MAIN-53-FIX1  SyntaxError Python 3.10/3.11 — variable intermédiaire
#                 fallback_text extraite avant la f-string
#   MAIN-53-FIX2  logger.debug → logger.warning pour MAIN-52-FIX1
#
# v3.54 :
#   MAIN-54-FIX1  from __future__ import annotations
#   MAIN-54-FIX2  date_obj_monthly extrait en tête de run_monthly_report()
#   MAIN-54-FIX3  _maybe_export_csv() : guard hasattr(SentinelDB, "export_csv")
#   MAIN-54-FIX4  _OPTIONAL_MODULES vérifiés une seule fois via _check_optional_modules()
#
# v3.60 — Audit complet inter-scripts (corrections audit 2026-04) :
#
#   MAIN-60-FIX1  BUG-CRIT-1 CORRIGÉ : double savemetrics() éliminé.
#                 run_sentinel() appelé avec save_metrics=False dans main().
#                 L'appel extract_metrics_from_report(report_text, date_iso)
#                 qui suit dans main() est la SEULE source de savemetrics().
#                 Avant : run_sentinel() appelait savemetrics() EN PLUS de l'appel
#                 explicite dans main() → double écriture silencieuse en DB.
#
#   MAIN-60-FIX2  BUG-CRIT-2 CORRIGÉ : double extract_memory_delta() éliminé.
#                 memory_deltas capturés depuis run_sentinel() (plus "_").
#                 _process_report_data_to_db() reçoit memory_deltas en paramètre
#                 et n'appelle PLUS extract_memory_delta(report_text) en interne.
#                 Avant : les deltas retournés par run_sentinel() étaient ignorés
#                 ("_"), puis recalculés dans _process_report_data_to_db().
#
#   MAIN-60-FIX3  Chemins absolus : _PROJECT_ROOT via Path(__file__).resolve().parent.
#                 _GITHUB_LAST_RUN_PATH + mkdir() + log file en chemin absolu.
#                 Avant : Path("data/...") — relatif au CWD, invalide en cron.
#
#   MAIN-60-FIX4  run_monthly_report() : capture memory_deltas depuis
#                 run_sentinel_monthly() (était ignoré avec "_") et les sauvegarde
#                 en DB (tendances + alertes du rapport mensuel enfin persistées).
#
#   MAIN-60-FIX5  datetime.now(timezone.utc) systématique — élimine datetime.now()
#                 naïf dans _ensure_health_server() (ts du healthcheck UTC).
#
#   MAIN-60-FIX6  extract_memory_delta retiré des imports directs depuis sentinel_api.
#                 Les memory_deltas sont désormais retournés par run_sentinel() et
#                 passés en paramètre à _process_report_data_to_db().
#
#   MAIN-60-FIX7  _process_report_data_to_db() signature étendue :
#                 memory_deltas: dict | None = None
#                 Si fourni → utilisé directement. Si None → fallback
#                 extract_memory_delta(report_text) conservé pour rétrocompatibilité.
# =============================================================================

from __future__ import annotations  # MAIN-54-FIX1

import datetime
import json
import logging
import threading
import time as timeperf
import importlib.util as ilu
import os
from pathlib import Path
from typing import Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# CONFIGURATION & VERSION — MAIN-60-FIX3 : chemins absolus
# ---------------------------------------------------------------------------
VERSION      = "3.60"
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")

REPORT_RETENTION_DAYS = int(os.environ.get("SENTINEL_RETENTION_DAYS",  "30"))
GITHUB_COOLDOWN_DAYS  = int(os.environ.get("GITHUB_COOLDOWN_DAYS",     "7"))
GITHUB_MAX_LOOKBACK   = int(os.environ.get("GITHUB_MAX_LOOKBACK",      "21"))
DAYS_BACK_CFG         = int(os.environ.get("GITHUB_DAYS_BACK",         "7"))
MAX_CONTRACTS         = int(os.environ.get("SENTINEL_MAX_CONTRACTS",   "50"))

# MAIN-60-FIX3 : chemin absolu — Path("data/...") invalide en cron
_PROJECT_ROOT         = Path(__file__).resolve().parent
_GITHUB_LAST_RUN_PATH = _PROJECT_ROOT / "data" / "github_last_run.txt"

for _d in ["logs", "data", "output", "backups"]:
    (_PROJECT_ROOT / _d).mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING INDUSTRIEL
# ---------------------------------------------------------------------------
def init_logging() -> None:
    """Configure le logger structuré JSON. Ne fait QUE ça."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            '{"ts":"%(asctime)s","lvl":"%(levelname)s",'
            f'"v":"{VERSION}","msg":"%(message)s"}}'
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.FileHandler(
                str(_PROJECT_ROOT / "logs" / "sentinel.log"),  # MAIN-60-FIX3
                encoding="utf-8",
            ),
            logging.StreamHandler(),
        ],
    )

init_logging()
logger = logging.getLogger("SENTINEL")

# ---------------------------------------------------------------------------
# DÉCLENCHEMENT GITHUB — MAIN-52-FIX3
# ---------------------------------------------------------------------------
def _should_run_github() -> tuple[bool, int, str | None]:
    """
    Retourne (should_run, effective_days_back, last_run_iso).

    [MAIN-52-FIX3] Lecture unique du fichier — élimine la double lecture
    et la race condition théorique du pipeline v3.51.
    [MAIN-60-FIX3] _GITHUB_LAST_RUN_PATH absolu.

    Logique :
        1. Lit data/github_last_run.txt une seule fois
        2. Si delta >= GITHUB_COOLDOWN_DAYS → effective_days = min(delta, MAX_LOOKBACK)
        3. Si fichier absent   → True, DAYS_BACK_CFG, None
        4. Si illisible        → fallback weekday()==4, DAYS_BACK_CFG, None
    """
    today = datetime.date.today()
    try:
        if not _GITHUB_LAST_RUN_PATH.exists():
            logger.info("GITHUB _should_run: premier lancement → True")
            return True, DAYS_BACK_CFG, None

        raw        = _GITHUB_LAST_RUN_PATH.read_text(encoding="utf-8").strip()
        last_run   = datetime.date.fromisoformat(raw)
        delta_days = (today - last_run).days

        if delta_days >= GITHUB_COOLDOWN_DAYS:
            effective_days = min(delta_days, GITHUB_MAX_LOOKBACK)
            logger.info(
                f"GITHUB _should_run: delta={delta_days}j → True, "
                f"days_back={effective_days}j "
                f"({'rattrapage' if delta_days > GITHUB_COOLDOWN_DAYS else 'cycle normal'}, "
                f"cap={GITHUB_MAX_LOOKBACK}j)"
            )
            return True, effective_days, raw

        logger.info(
            f"GITHUB _should_run: cooldown actif "
            f"(delta={delta_days}j < {GITHUB_COOLDOWN_DAYS}j) → False"
        )
        return False, DAYS_BACK_CFG, raw

    except (ValueError, OSError) as e:
        logger.warning(
            f"GITHUB _should_run: lecture impossible ({e}) — fallback weekday()==4"
        )
        return today.weekday() == 4, DAYS_BACK_CFG, None

def _mark_github_ran() -> None:
    """Écriture atomique .tmp → replace. Appelé après scrape réussi."""
    today = datetime.date.today().isoformat()
    tmp   = _GITHUB_LAST_RUN_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(today, encoding="utf-8")
        tmp.replace(_GITHUB_LAST_RUN_PATH)
        logger.info(f"GITHUB last_run mis à jour → {today}")
    except OSError as e:
        logger.warning(f"GITHUB _mark_github_ran: écriture impossible ({e})")

# ---------------------------------------------------------------------------
# VALIDATION DES DÉPENDANCES — MAIN-44-FIX2
# ---------------------------------------------------------------------------
_REQUIRED_DAILY: list[tuple[str, str]] = [
    ("scraper_rss",    "scrape_all_feeds"),
    ("scraper_rss",    "format_for_claude"),
    ("memory_manager", "get_compressed_memory"),
    ("memory_manager", "update_memory"),
    ("sentinel_api",   "run_sentinel"),
    ("sentinel_api",   "extract_metrics_from_report"),
    # MAIN-60-FIX6 : extract_memory_delta retiré — n'est plus appelé en double
    # depuis main(). Accessible via sentinel_api.extract_memory_delta si besoin.
    ("charts",         "generate_all_charts"),
    ("report_builder", "build_html_report"),
    ("report_builder", "purge_old_reports"),
    ("mailer",         "send_report"),
    ("db_manager",     "SentinelDB"),
    ("samgov_scraper", "run_all_procurement"),
]

_REQUIRED_MONTHLY: list[tuple[str, str]] = [
    ("memory_manager", "compress_text_haiku"),
    ("sentinel_api",   "run_sentinel_monthly"),
]

# Modules optionnels — absence loggée en WARNING, pipeline non bloqué
# [MAIN-54-FIX4] Vérifiés une seule fois via _check_optional_modules()
_OPTIONAL_MODULES: list[tuple[str, str]] = [
    ("nlp_scorer", "run_nlp_pipeline"),
]

def _check_dependencies(
    extra: list[tuple[str, str]] | None = None,
) -> list[str]:
    """
    Vérifie importabilité + présence des attributs requis.
    Retourne liste vide si OK, liste de manquants sinon.
    Ne vérifie PAS les modules optionnels (voir _check_optional_modules).
    """
    checks  = _REQUIRED_DAILY + (extra or [])
    missing: list[str] = []
    for module_name, attr_name in checks:
        try:
            mod = __import__(module_name)
            if not hasattr(mod, attr_name):
                missing.append(f"{module_name}.{attr_name}()")
        except ImportError:
            missing.append(f"{module_name} (module absent)")
    return missing

def _check_optional_modules() -> None:
    """
    [MAIN-54-FIX4] Vérifie les modules optionnels une seule fois au démarrage.
    Loggue en INFO si présent, WARNING si absent — sans bloquer le pipeline.
    """
    for module_name, attr_name in _OPTIONAL_MODULES:
        try:
            mod = __import__(module_name)
            if hasattr(mod, attr_name):
                logger.info(f"MODULE OPTIONNEL OK : {module_name}.{attr_name}()")
            else:
                logger.warning(
                    f"MODULE OPTIONNEL : {module_name}.{attr_name}() absent "
                    f"— fonctionnalité désactivée"
                )
        except ImportError:
            logger.warning(
                f"MODULE OPTIONNEL : {module_name} absent "
                f"(pip install scikit-learn numpy) — NLP rerank désactivé"
            )

# ---------------------------------------------------------------------------
# TIMING PAR MODULE — MAIN-43-FIX1
# ---------------------------------------------------------------------------
def _timed(label: str, fn: Callable, *args: Any, **kwargs: Any) -> Any:
    t0      = timeperf.perf_counter()
    result  = fn(*args, **kwargs)
    elapsed = timeperf.perf_counter() - t0
    logger.info(f"TIMING {label} : {elapsed:.1f}s")
    return result

# ---------------------------------------------------------------------------
# HEALTH SERVER — MAIN-52-FIX7 / MAIN-60-FIX5
# ---------------------------------------------------------------------------
_HEALTH_SERVER_STARTED = False
_HEALTH_LOCK           = threading.Lock()

def _ensure_health_server() -> None:
    """
    Démarre le serveur HTTP de healthcheck en thread daemon.
    Protégé par un lock pour éviter un double démarrage théorique.
    MAIN-60-FIX5 : datetime.now(timezone.utc) — ts UTC dans la réponse JSON.
    """
    global _HEALTH_SERVER_STARTED
    with _HEALTH_LOCK:
        if _HEALTH_SERVER_STARTED:
            return

        try:
            port = int(os.environ.get("SENTINEL_HEALTH_PORT", "8765"))
        except ValueError:
            logger.warning("SENTINEL_HEALTH_PORT invalide — port 8765 par défaut")
            port = 8765

        from http.server import BaseHTTPRequestHandler, HTTPServer

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path in ["/health", "/"]:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({
                            "status":  "ok",
                            "version": VERSION,
                            # MAIN-60-FIX5 : UTC — jamais datetime naïf
                            "ts": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                        }).encode()
                    )
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass  # silencieux

        def _run_server() -> None:
            try:
                HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()
            except Exception:
                pass

        threading.Thread(target=_run_server, daemon=True).start()
        _HEALTH_SERVER_STARTED = True
        logger.info(f"Health server actif sur port {port}")

# ---------------------------------------------------------------------------
# IMPORTS DES MODULES SENTINEL
# ---------------------------------------------------------------------------
from scraper_rss    import scrape_all_feeds, format_for_claude           # noqa: E402
from memory_manager import get_compressed_memory, update_memory           # noqa: E402
from sentinel_api   import (                                               # noqa: E402
    run_sentinel,
    extract_metrics_from_report,
    # MAIN-60-FIX6 : extract_memory_delta retiré des imports directs.
    # Les memory_deltas sont désormais retournés par run_sentinel() et
    # passés en paramètre à _process_report_data_to_db(). Plus de double appel.
)
from charts         import generate_all_charts                             # noqa: E402
from report_builder import build_html_report, purge_old_reports            # noqa: E402
from mailer         import send_report                                     # noqa: E402
from db_manager     import SentinelDB, closedb, initdb                     # noqa: E402
from samgov_scraper import run_all_procurement                             # noqa: E402

# ---------------------------------------------------------------------------
# SCRAPERS OPTIONNELS — MAIN-41-FIX4
# ---------------------------------------------------------------------------
def _load_optional_scraper(name: str, module_path: str) -> Any:
    """Charge un scraper optionnel depuis son chemin fichier."""
    full_path = _PROJECT_ROOT / module_path   # MAIN-60-FIX3 : chemin absolu
    if full_path.exists():
        try:
            spec = ilu.spec_from_file_location(name, str(full_path))
            if spec is None or spec.loader is None:
                raise ImportError(f"Spec introuvable pour {full_path}")
            mod = ilu.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            return mod
        except Exception as e:
            logger.warning(f"Erreur chargement {name}: {e}")
    return None

telegram_mod = _load_optional_scraper("telegram_scraper", "telegram_scraper.py")
patents_mod  = _load_optional_scraper("ops_patents",       "ops_patents.py")
github_mod   = _load_optional_scraper("github_scraper",    "github_scraper.py")

# ---------------------------------------------------------------------------
# LOGIQUE DE SAUVEGARDE — MAIN-60-FIX2 / MAIN-60-FIX7
# ---------------------------------------------------------------------------
def _process_report_data_to_db(
    report_text:      str,
    metrics:          dict,
    date_iso:         str,
    github_days_back: int        = 0,
    memory_deltas:    dict | None = None,   # MAIN-60-FIX7 : nouveau paramètre
) -> None:
    """
    Sauvegarde métriques, rapport, tendances et alertes dans SentinelDB.

    [MAIN-48-B]    github_days_back : 0 = pas de GitHub, >0 = fenêtre effective.
    [MAIN-52-FIX1] Log WARNING si github_days_back risque d'être rejeté par DB.
    [MAIN-60-FIX2] BUG-CRIT-2 CORRIGÉ : memory_deltas reçu en paramètre depuis
                   run_sentinel() — plus de double extract_memory_delta().
    [MAIN-60-FIX7] Fallback extract_memory_delta(report_text) conservé si
                   memory_deltas=None pour rétrocompatibilité (appels directs).
    """
    if github_days_back > 0:
        logger.warning(
            f"DB savemetrics: github_days_back={github_days_back} transmis. "
            f"Vérifier que 'github_days_back' est dans _ALLOWED_METRIC_COLS "
            f"ET dans le DDL de db_manager.py — sinon la valeur est rejetée "
            f"silencieusement et le système anti-biais GitHub est inopérant. "
            f"(BUG-DB-1 — correction requise dans db_manager.py)"
        )

    SentinelDB.savemetrics(
        date_iso,
        indice           = metrics.get("indice",        5.0),
        alerte           = metrics.get("alerte",        "VERT"),
        nb_articles      = metrics.get("nb_articles",   0),
        nb_pertinents    = metrics.get("nb_pertinents", 0),
        github_days_back = github_days_back,
    )

    SentinelDB.savereport(
        date       = date_iso,
        indice     = metrics.get("indice", 5.0),
        alerte     = metrics.get("alerte", "VERT"),
        compressed = report_text,        # texte Claude complet
        rawtail    = report_text[:3000], # troncature — requêtes rapides dashboard
    )

    # MAIN-60-FIX2 : utiliser les deltas déjà extraits — éviter le double appel.
    # Si memory_deltas=None (appel legacy direct), fallback vers l'extraction locale.
    if memory_deltas is None:
        # Fallback rétrocompatibilité — ne devrait plus être atteint depuis main()
        from sentinel_api import extract_memory_delta as _extract_delta
        deltas = _extract_delta(report_text) or {}
        logger.debug(
            "_process_report_data_to_db : memory_deltas=None — "
            "fallback extract_memory_delta() (appel legacy)"
        )
    else:
        deltas = memory_deltas

    for t in deltas.get("nouvelles_tendances", []):
        SentinelDB.savetendance(t, date_iso)
    for a in deltas.get("alertes_ouvertes", []):
        SentinelDB.ouvrirealerte(a, date_iso)
    for c in deltas.get("alertes_closes", []):
        SentinelDB.closealerte(c, date_iso)

# ---------------------------------------------------------------------------
# EXPORT CSV OPTIONNEL — MAIN-54-FIX3
# ---------------------------------------------------------------------------
def _maybe_export_csv() -> dict:
    """
    Lance l'export CSV si SENTINEL_EXPORT_CSV=1 dans .env.
    [MAIN-54-FIX3] Guard hasattr() — évite AttributeError si SentinelDB.export_csv
    est absent de db_manager.py.
    Retourne un dict {nom: Path} des fichiers générés, vide si désactivé ou absent.
    """
    if os.environ.get("SENTINEL_EXPORT_CSV", "0") != "1":
        return {}
    if not hasattr(SentinelDB, "export_csv"):
        logger.warning(
            "SENTINEL_EXPORT_CSV=1 mais SentinelDB.export_csv() absent "
            "— export désactivé. Ajouter la méthode dans db_manager.py."
        )
        return {}
    try:
        csv_exports = _timed("csv_export", SentinelDB.export_csv)
        logger.info(f"CSV exportés : {list(csv_exports.keys())}")
        return csv_exports
    except Exception as e:
        logger.warning(f"Export CSV non bloquant : {e}")
        return {}

# ---------------------------------------------------------------------------
# PIPELINE QUOTIDIEN — MAIN-60-FIX1 / MAIN-60-FIX2
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info(f"=== DEMARRAGE SENTINEL v{VERSION} ===")
    start_time = timeperf.perf_counter()
    initdb()
    _ensure_health_server()

    # [MAIN-54-FIX4] Modules optionnels vérifiés une seule fois au démarrage
    _check_optional_modules()

    missing = _check_dependencies()
    if missing:
        logger.error(
            f"DEPENDANCES MANQUANTES — pipeline annulé : {', '.join(missing)}"
        )
        return

    date_obj = datetime.datetime.now(datetime.timezone.utc).date()
    date_iso = date_obj.isoformat()

    try:
        # ── 1. COLLECTE ──────────────────────────────────────────────────────
        articles: list[dict] = _timed("scrape_rss", scrape_all_feeds)

        if telegram_mod:
            try:
                tg_arts = _timed("telegram", telegram_mod.run_telegram_scraper)
                articles.extend(tg_arts or [])
            except Exception as e:
                logger.warning(f"Erreur Telegram: {e}")

        # [MAIN-52-FIX5] Cap contrats pour éviter overflow tokens
        try:
            contracts        = _timed("procurement", run_all_procurement, daysback=2)
            contracts_capped = (contracts or [])[:MAX_CONTRACTS]
            if len(contracts or []) > MAX_CONTRACTS:
                logger.warning(
                    f"Procurement : {len(contracts)} contrats → capé à {MAX_CONTRACTS} "
                    f"(configurer SENTINEL_MAX_CONTRACTS dans .env)"
                )
            articles.extend(contracts_capped)
            logger.info(f"Procurement : {len(contracts_capped)} contrats ajoutés")
        except Exception as e:
            logger.warning(f"Erreur Procurement (non bloquant) : {e}")

        # ── GitHub — MAIN-52-FIX3 : une seule lecture fichier ────────────────
        has_github:     bool       = False
        last_run_iso:   str | None = None
        effective_days: int        = DAYS_BACK_CFG

        if github_mod:
            should_run, effective_days, last_run_iso = _should_run_github()
            if should_run:
                try:
                    github_arts = _timed(
                        "github", github_mod.run_github_scraper, effective_days
                    )
                    if github_arts:
                        articles.extend(github_arts)
                        _mark_github_ran()
                        has_github = True
                        logger.info(
                            f"GITHUB : {len(github_arts)} articles ajoutés "
                            f"(days_back={effective_days}j) — report_type → friday_rd"
                        )
                    else:
                        logger.info("GITHUB : aucun article (cooldown non réinitialisé)")
                except Exception as e:
                    logger.warning(f"Erreur GitHub (non bloquant) : {e}")
            else:
                logger.info("GITHUB : cooldown actif — scraper ignoré ce cycle")

        if not articles:
            logger.info("Aucune donnée collectée. Fin du cycle.")
            return

        # ── 2. ANALYSE ───────────────────────────────────────────────────────
        formatted_data = format_for_claude(articles)
        memory_context = get_compressed_memory()

        # [MAIN-48-A] Injection <system_notice> si rattrapage GitHub
        if has_github and effective_days > GITHUB_COOLDOWN_DAYS:
            catchup_date_str = (
                f"le {last_run_iso}" if last_run_iso
                else f"il y a plus de {effective_days} jours"
            )
            catchup_note = (
                "<system_notice>
"
                f"RATTRAPAGE GITHUB : la veille R&D n'a pas pu être exécutée "
                f"normalement. Fenêtre élargie à {effective_days} jours "
                f"(dernière exécution : {catchup_date_str}). "
                "Le volume d'articles GitHub est supérieur à la normale — "
                "ne pas interpréter ce volume comme un pic d'activité soudain. "
                f"Mentionner ce rattrapage dans la section Veille R&D (MODULE 4-BIS) "
                f"sous la forme : 'Note : analyse R&D élargie à {effective_days}j "
                f"(rattrapage du cycle manqué {catchup_date_str})'.
"
                "</system_notice>

"
            )
            formatted_data = catchup_note + formatted_data
            logger.info(
                f"GITHUB <system_notice> injectée "
                f"(fenêtre={effective_days}j > cooldown={GITHUB_COOLDOWN_DAYS}j)"
            )

        report_type = "friday_rd" if has_github else "daily"
        logger.info(f"CLAUDE report_type={report_type!r}")

        # ── MAIN-60-FIX1 : save_metrics=False — évite le double savemetrics()
        # ── MAIN-60-FIX2 : capturer memory_deltas (plus "_")
        report_text, memory_deltas = _timed(
            "claude_daily",
            run_sentinel,
            formatted_data,
            memory_context,
            date_obj,
            model        = SONNET_MODEL,
            report_type  = report_type,
            save_metrics = False,   # MAIN-60-FIX1 : seul appel save ci-dessous
        )
        if not report_text:
            raise RuntimeError("Claude n'a retourné aucun rapport.")

        # ── 3. EXTRACTION & DB ───────────────────────────────────────────────
        # MAIN-60-FIX1 : extract_metrics_from_report() = SEULE source de savemetrics()
        metrics = extract_metrics_from_report(report_text, date_iso)  # save=True défaut

        # MAIN-60-FIX2 : memory_deltas passés en paramètre — plus de double extraction
        _process_report_data_to_db(
            report_text,
            metrics or {},
            date_iso,
            github_days_back = effective_days if has_github else 0,
            memory_deltas    = memory_deltas,   # MAIN-60-FIX2
        )

        # ── 4. RESTITUTION ───────────────────────────────────────────────────
        chart_dict  = _timed("charts", generate_all_charts, report_text)
        chart_paths = (
            list(chart_dict.values()) if isinstance(chart_dict, dict)
            else (chart_dict or [])
        )

        html_report = _timed(
            "report_builder",
            build_html_report,
            report_text,
            chart_paths,
            date_obj,
        )

        csv_attachments = _maybe_export_csv()

        if html_report and Path(html_report).exists():
            _timed(
                "mailer",
                send_report,
                html_report,
                csv_attachments=csv_attachments if csv_attachments else None,
            )
            logger.info(f"Rapport envoyé : {html_report}")

        # ── 5. MAINTENANCE ───────────────────────────────────────────────────
        update_memory(report_text)
        purge_old_reports(days=REPORT_RETENTION_DAYS)

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== CYCLE TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH PIPELINE: {e}", exc_info=True)
        try:
            send_report(None, subject=f"[SENTINEL CRASH] {type(e).__name__}: {e}")
        except Exception:
            pass
    finally:
        closedb()

# ---------------------------------------------------------------------------
# RAPPORT MENSUEL — MAIN-60-FIX4
# ---------------------------------------------------------------------------
def run_monthly_report() -> None:
    """
    Rapport mensuel SENTINEL via MapReduce.

    Phase MAP    : Haiku compresse chaque rapport journalier (parallèle, 8 workers)
    Phase REDUCE : blocs hebdo → Sonnet → rapport mensuel final

    [MAIN-54-FIX2] date_obj_monthly extrait en tête de fonction (unique calcul).
    [MAIN-52-FIX4] Guard "if not filled" avant REDUCE.
    [MAIN-53-FIX1] fallback_text en variable intermédiaire (pas de backslash en f-string).
    [MAIN-60-FIX4] memory_deltas capturés depuis run_sentinel_monthly() et persistés.
    """
    logger.info(f"=== DEMARRAGE RAPPORT MENSUEL SENTINEL v{VERSION} ===")
    start_time = timeperf.perf_counter()
    initdb()

    missing = _check_dependencies(extra=_REQUIRED_MONTHLY)
    if missing:
        logger.warning(
            f"DEPENDANCES MANQUANTES — rapport mensuel annulé : {', '.join(missing)}"
        )
        closedb()
        return

    # [MAIN-54-FIX2] Calcul unique — réutilisé dans savereport + build_html_report
    now_utc          = datetime.datetime.now(datetime.timezone.utc)
    date_obj_monthly = now_utc.date()
    date_iso         = date_obj_monthly.isoformat()
    mois             = now_utc.strftime("%B %Y")

    try:
        from memory_manager import compress_text_haiku   # noqa: PLC0415
        from sentinel_api   import run_sentinel_monthly  # noqa: PLC0415

        # ── Récupération des 30 derniers rapports ─────────────────────────
        rows = SentinelDB.get_last_n_reports(30)
        if not rows:
            logger.warning("run_monthly_report : aucun rapport en base — abandon.")
            return

        # Un seul appel getmetrics() pour github_days_back + nb_articles
        metrics_by_date:  dict[str, int] = {}
        articles_by_date: dict[str, int] = {}
        try:
            recent_metrics = SentinelDB.getmetrics(ndays=35)
            for m in recent_metrics:
                d = m["date"]
                metrics_by_date[d]  = int(m.get("github_days_back") or 0)
                articles_by_date[d] = int(m.get("nb_articles")      or 0)
        except Exception as e:
            logger.warning(
                f"REDUCE métriques : getmetrics() échoué ({e}) "
                f"— annotation + densité désactivées"
            )

        logger.info(f"MapReduce mensuel : {len(rows)} rapports récupérés")

        # ── Phase MAP parallèle ───────────────────────────────────────────
        raw_texts: list[str] = [
            (row.get("compressed") or row.get("rawtail") or "").strip()
            for row in rows
        ]
        indexed_raws:       list[tuple[int, str]] = [
            (i, t) for i, t in enumerate(raw_texts) if t
        ]
        compressed_reports: list[str] = [""] * len(rows)
        dates_by_index:     list[str] = [row.get("date", "") for row in rows]

        t_map_start = timeperf.perf_counter()

        def _compress_one(args: tuple[int, str]) -> tuple[int, str]:
            idx, raw = args
            try:
                return idx, compress_text_haiku(raw)
            except Exception as exc:
                logger.warning(
                    f"MAP rapport {idx + 1} : erreur Haiku ({exc}) — fallback"
                )
                return idx, raw[:300]

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(_compress_one, item): item[0]
                for item in indexed_raws
            }
            for future in as_completed(futures):
                try:
                    idx, summary = future.result()
                    compressed_reports[idx] = summary
                except Exception as exc:
                    orig_idx = futures[future]
                    logger.warning(f"MAP future {orig_idx + 1} : erreur ({exc})")
                    compressed_reports[orig_idx] = raw_texts[orig_idx][:300]

        filled: list[tuple[str, str]] = [
            (compressed_reports[i], dates_by_index[i])
            for i in range(len(rows))
            if compressed_reports[i]
        ]

        elapsed_map = timeperf.perf_counter() - t_map_start
        logger.info(
            f"TIMING haiku_map_parallel ({len(filled)} rapports) : "
            f"{elapsed_map:.1f}s — {sum(len(s) for s, _ in filled)} chars total"
        )

        # [MAIN-52-FIX4] Guard — évite un appel Sonnet sur chaîne vide
        if not filled:
            logger.error(
                "REDUCE mensuel : aucun rapport compressé disponible — abandon. "
                "Vérifier que des rapports journaliers existent en base "
                "(colonne reports.compressed ou reports.rawtail)."
            )
            return

        # ── Phase REDUCE : regroupement par semaine ───────────────────────
        def _chunk(lst: list, size: int) -> list[list]:
            return [lst[i:i + size] for i in range(0, len(lst), size)]

        weekly_summaries: list[str]                    = []
        catchup_weeks:    list[tuple[int, int, float]] = []
        chunks = _chunk(filled, 7)

        for w_idx, week in enumerate(chunks, 1):
            week_texts = [s for s, _ in week]
            week_dates = [d for _, d in week]

            max_gdb = max(
                (metrics_by_date.get(d, 0) for d in week_dates if d),
                default=0,
            )

            # [MAIN-50-A] Densité normalisée articles/jour
            total_articles   = sum(articles_by_date.get(d, 0) for d in week_dates if d)
            effective_window = (
                max_gdb if max_gdb > GITHUB_COOLDOWN_DAYS
                else GITHUB_COOLDOWN_DAYS
            )
            density = (
                round(total_articles / effective_window, 1)
                if effective_window > 0 else 0.0
            )

            if max_gdb > GITHUB_COOLDOWN_DAYS:
                catchup_weeks.append((w_idx, max_gdb, density))

            catchup_flag = (
                f" [RATTRAPAGE GITHUB {max_gdb}j | "
                f"densité normalisée : {density} articles/jour | "
                f"ne pas surpondérer]"
                if max_gdb > GITHUB_COOLDOWN_DAYS
                else ""
            )

            if catchup_flag:
                logger.info(
                    f"REDUCE semaine {w_idx} : "
                    f"max_gdb={max_gdb}j, density={density} art/j → annotation"
                )

            try:
                w_summary = _timed(
                    f"haiku_reduce_week_{w_idx}",
                    compress_text_haiku,
                    "
---
".join(week_texts),
                )
                weekly_summaries.append(
                    f"[SEMAINE {w_idx}{catchup_flag}]
{w_summary}"
                )
            except Exception as exc:
                logger.warning(f"REDUCE semaine {w_idx} : {exc}")
                # [MAIN-53-FIX1] Variable intermédiaire — backslash interdit en f-string
                fallback_text = "
".join(week_texts)[:500]
                weekly_summaries.append(
                    f"[SEMAINE {w_idx}{catchup_flag}]
{fallback_text}"
                )

        injection = "

".join(weekly_summaries)

        # [MAIN-49-A + MAIN-50-A] <context_metadata> si ≥1 semaine atypique
        if catchup_weeks:
            catchup_set = {w for w, _, _ in catchup_weeks}

            normal_weeks_densities: list[tuple[int, float]] = [
                (i + 1, round(
                    sum(articles_by_date.get(d, 0) for _, d in chunk)
                    / GITHUB_COOLDOWN_DAYS, 1
                ))
                for i, chunk in enumerate(chunks)
                if (i + 1) not in catchup_set
            ]

            normal_ref_str = ""
            if normal_weeks_densities:
                avg_normal = round(
                    sum(dens for _, dens in normal_weeks_densities)
                    / len(normal_weeks_densities), 1
                )
                normal_ref_str = (
                    f"Densité moyenne des semaines normales : {avg_normal} articles/jour. "
                    "Si la densité normalisée des semaines de rattrapage est proche "
                    "de cette valeur, confirmer l'absence de pic d'activité réel.
"
                )

            lines = "
".join(
                f"  - Semaine {w} : fenêtre {gdb}j, "
                f"densité normalisée {dens} articles/jour "
                f"(volume total élevé dû au rattrapage — comparer à la densité normale)."
                for w, gdb, dens in catchup_weeks
            )
            normal_weeks_list = [
                i for i in range(1, len(weekly_summaries) + 1)
                if i not in catchup_set
            ]
            normal_str = (
                f"Semaines {', '.join(str(w) for w in normal_weeks_list)} : "
                f"cycle normal ({GITHUB_COOLDOWN_DAYS}j)."
                if normal_weeks_list else ""
            )

            context_metadata = (
                "<context_metadata>
"
                f"BIAIS DE COLLECTE — RAPPORT MENSUEL {mois.upper()}
"
                "Les semaines suivantes présentent un volume GitHub anormalement "
                "élevé dû à un rattrapage de cycle :
"
                f"{lines}
"
                f"{normal_str}
"
                f"{normal_ref_str}"
                "Consignes applicables à l'INTÉGRALITÉ du rapport :
"
                "  1. Ne pas surpondérer l'activité GitHub des semaines annotées "
                "dans les tendances long-terme.
"
                "  2. La densité normalisée (articles/jour) est la métrique "
                "pertinente, pas le volume total.
"
                "  3. Si la densité normalisée est comparable aux semaines normales, "
                "conclure explicitement à l'absence de pic d'activité.
"
                "  4. Mentionner ce rattrapage en note méthodologique en fin de rapport.
"
                "</context_metadata>

"
            )
            injection = context_metadata + injection
            logger.info(
                f"REDUCE <context_metadata> injectée : "
                f"{len(catchup_weeks)} semaine(s) de rattrapage "
                f"({[f'S{w}={g}j/{d}art/j' for w, g, d in catchup_weeks]}) — "
                f"{len(context_metadata)} chars"
            )

        logger.info(
            f"REDUCE terminé : {len(weekly_summaries)} blocs "
            f"({len(injection)} chars → Sonnet)"
        )

        # ── Synthèse Sonnet finale — MAIN-60-FIX4 : capture memory_deltas ─────
        report_text, memory_deltas_monthly = _timed(
            "claude_monthly",
            run_sentinel_monthly,
            injection,
            mois,
            model=SONNET_MODEL,
        )
        if not report_text:
            raise RuntimeError("run_sentinel_monthly() n'a retourné aucun rapport.")

        metrics = extract_metrics_from_report(report_text, date_iso)

        # compressed = texte mensuel brut, rawtail = JSON métadonnées audit
        monthly_meta = json.dumps(
            {
                "type":          "monthly",
                "mois":          mois,
                "catchup_weeks": [
                    {"semaine": w, "gdb": g, "density": d}
                    for w, g, d in catchup_weeks
                ],
                **(metrics or {}),
            },
            ensure_ascii=False,
        )

        SentinelDB.savereport(
            date       = date_iso,
            indice     = (metrics or {}).get("indice", 5.0),
            alerte     = (metrics or {}).get("alerte", "VERT"),
            compressed = report_text,   # texte mensuel brut
            rawtail    = monthly_meta,  # JSON métadonnées audit
        )

        # MAIN-60-FIX4 : persister les deltas mémoire du rapport mensuel
        if any(memory_deltas_monthly.values()):
            for t in memory_deltas_monthly.get("nouvelles_tendances", []):
                SentinelDB.savetendance(t, date_iso)
            for a in memory_deltas_monthly.get("alertes_ouvertes", []):
                SentinelDB.ouvrirealerte(a, date_iso)
            for c in memory_deltas_monthly.get("alertes_closes", []):
                SentinelDB.closealerte(c, date_iso)
            logger.info(
                f"MONTHLY Deltas mémoire persistés : "
                f"{len(memory_deltas_monthly['nouvelles_tendances'])} tendances, "
                f"{len(memory_deltas_monthly['alertes_ouvertes'])} alertes ouvertes, "
                f"{len(memory_deltas_monthly['alertes_closes'])} alertes closes"
            )
        else:
            logger.debug("MONTHLY Aucun delta mémoire dans le rapport mensuel")

        chart_dict_monthly  = _timed("charts_monthly", generate_all_charts, report_text)
        chart_paths_monthly = (
            list(chart_dict_monthly.values())
            if isinstance(chart_dict_monthly, dict)
            else (chart_dict_monthly or [])
        )

        # [MAIN-54-FIX2] date_obj_monthly déjà calculé en tête — pas de double appel
        html_report = _timed(
            "report_builder_monthly",
            build_html_report,
            report_text,
            chart_paths_monthly,
            date_obj_monthly,
        )

        csv_attachments = _maybe_export_csv()

        if html_report and Path(html_report).exists():
            _timed(
                "mailer_monthly",
                send_report,
                html_report,
                csv_attachments=csv_attachments if csv_attachments else None,
            )
            logger.info(f"Rapport mensuel envoyé : {html_report}")

        elapsed = timeperf.perf_counter() - start_time
        logger.info(f"=== RAPPORT MENSUEL TERMINE EN {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"CRASH RAPPORT MENSUEL: {e}", exc_info=True)
        try:
            send_report(
                None,
                subject=f"[SENTINEL CRASH MENSUEL] {type(e).__name__}: {e}",
            )
        except Exception:
            pass
    finally:
        closedb()

# ---------------------------------------------------------------------------
# ENTRÉE PRINCIPALE
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TODAY = datetime.datetime.now(datetime.timezone.utc).date()  # MAIN-60-FIX5
    if TODAY.day == 1:
        try:
            run_monthly_report()
        except Exception as e:
            logger.error(f"Erreur rapport mensuel: {e}", exc_info=True)

    main()
