#!/usr/bin/env python3
# sentinel_main.py --- SENTINEL v3.50 --- ORCHESTRATEUR PRINCIPAL
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
#   MAIN-42-FIX1  run_all_procurement() intégré dans main()
#
# v3.43 — Observabilité & rapport mensuel :
#   MAIN-43-FIX1  _timed() — helper de timing par module
#   MAIN-43-FIX2  run_monthly_report() — implémentation MapReduce
#
# v3.44 — Performance & robustesse :
#   MAIN-44-FIX1  Parallélisation MAP via ThreadPoolExecutor(max_workers=8)
#   MAIN-44-FIX2  _check_dependencies() au démarrage de main() et mensuel
#
# v3.46 — Déclenchement GitHub & enrichissement prompt vendredi :
#   MAIN-46-A     _should_run_github() remplace weekday() == 4
#   MAIN-46-C     report_type "friday_rd" transmis à run_sentinel()
#
# v3.47 — Rattrapage GitHub robuste :
#   MAIN-47-A     _should_run_github() → (bool, int) — days_back adaptatif
#   MAIN-47-B     catchup_note injectée en tête de formatted_data
#
# v3.48 — Structuration Claude & persistance métriques GitHub :
#   MAIN-48-A     catchup_note wrappée dans <system_notice>
#   MAIN-48-B     github_days_back persisté dans SentinelDB.metrics
#   MAIN-48-C     Annotation [RATTRAPAGE GITHUB Xj] dans les blocs REDUCE
#
# v3.49 — Contrainte de biais persistante dans le rapport mensuel :
#   MAIN-49-A     <context_metadata> injectée en tête d'injection Sonnet
#                 si au moins une semaine de rattrapage détectée.
#                 Conditionnel, granularité par semaine, 4 consignes explicites.
#                 catchup_weeks persisté dans compressed pour audit.
#
# v3.50 — Densité normalisée dans les annotations de biais :
#   MAIN-50-A     Calcul déterministe de la densité articles/jour depuis DB.
#
#                 Idée source : "Trend-Normalizer" — donner à Sonnet un ratio
#                 densité pour qu'il confirme l'absence de pic d'activité réel.
#
#                 Pourquoi PAS dans Haiku (phase MAP) :
#                   1. Haiku n'a pas accès à github_days_back (dans metrics,
#                      pas dans rawtail). Injection manuelle requise → complexité.
#                   2. Compter des articles dans de la prose est non déterministe.
#                      nb_articles est déjà persisté exactement dans SentinelDB.
#                   3. Violation de responsabilité unique : Haiku MAP = compression,
#                      pas métrologie.
#
#                 Implémentation correcte : division directe depuis DB.
#                   articles_by_date chargé en même temps que metrics_by_date
#                   (un seul getmetrics() pour les deux — zéro appel API supp.)
#                   density = total_articles / effective_window (déterministe)
#                   effective_window = max_gdb si rattrapage, sinon COOLDOWN
#
#                 Intégration :
#                   catchup_weeks : list[tuple[int, int, float]]
#                   Élargi de (semaine, gdb) → (semaine, gdb, density)
#                   Annotation locale MAIN-48-C : densité inline
#                   <context_metadata> MAIN-49-A : densité par semaine
#                   compressed mensuel : density persistée pour audit futur
# =============================================================================

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
VERSION      = "3.50"
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")

REPORT_RETENTION_DAYS = int(os.environ.get("SENTINEL_RETENTION_DAYS", "30"))
GITHUB_COOLDOWN_DAYS  = int(os.environ.get("GITHUB_COOLDOWN_DAYS", "7"))
GITHUB_MAX_LOOKBACK   = int(os.environ.get("GITHUB_MAX_LOOKBACK", "21"))
DAYS_BACK_CFG         = int(os.environ.get("GITHUB_DAYS_BACK", "7"))
_GITHUB_LAST_RUN_PATH = Path("data/github_last_run.txt")

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
# DÉCLENCHEMENT GITHUB — MAIN-46-A → MAIN-47-A
# ---------------------------------------------------------------------------
def _should_run_github() -> tuple[bool, int]:
    """
    [MAIN-47-A] Retourne (should_run: bool, effective_days_back: int).
    Logique :
        1. Lit data/github_last_run.txt (date ISO YYYY-MM-DD)
        2. Si delta >= GITHUB_COOLDOWN_DAYS :
               effective_days = min(delta, GITHUB_MAX_LOOKBACK)
        3. Si fichier absent → True + DAYS_BACK_CFG
        4. Si illisible → fallback weekday() == 4 + DAYS_BACK_CFG
    """
    today = datetime.date.today()
    try:
        if not _GITHUB_LAST_RUN_PATH.exists():
            logger.info("GITHUB _should_run: premier lancement → True")
            return True, DAYS_BACK_CFG

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
            return True, effective_days

        logger.info(
            f"GITHUB _should_run: cooldown actif "
            f"(delta={delta_days}j < {GITHUB_COOLDOWN_DAYS}j) → False"
        )
        return False, DAYS_BACK_CFG

    except (ValueError, OSError) as e:
        logger.warning(
            f"GITHUB _should_run: lecture impossible ({e}) — fallback weekday()==4"
        )
        return today.weekday() == 4, DAYS_BACK_CFG


def _mark_github_ran() -> None:
    """[MAIN-46-A] Écriture atomique .tmp → replace. Après scrape réussi."""
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
    """Vérifie importabilité + présence des attributs. Liste vide = OK."""
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
        logger.warning("SENTINEL_HEALTH_PORT invalide — port 8765 par défaut")
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
                    "status":  "ok",
                    "version": VERSION,
                    "ts":      datetime.datetime.now().isoformat(),
                }).encode())
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, *args): pass

    def run_server():
        try:
            HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()
        except Exception:
            pass

    threading.Thread(target=run_server, daemon=True).start()
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
from samgov_scraper import run_all_procurement


# ---------------------------------------------------------------------------
# SCRAPERS OPTIONNELS
# ---------------------------------------------------------------------------
def _load_optional_scraper(name: str, module_path: str):
    if Path(module_path).exists():
        try:
            spec = ilu.spec_from_file_location(name, module_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Spec introuvable pour {module_path}")
            mod = ilu.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
        except Exception as e:
            logger.warning(f"Erreur chargement {name}: {e}")
    return None

telegram_mod = _load_optional_scraper("telegram_scraper", "telegram_scraper.py")
patents_mod  = _load_optional_scraper("ops_patents",       "ops_patents.py")
github_mod   = _load_optional_scraper("github_scraper",    "github_scraper.py")


# ---------------------------------------------------------------------------
# LOGIQUE DE SAUVEGARDE (Centralisée)
# ---------------------------------------------------------------------------
def _process_report_data_to_db(
    report_text: str,
    metrics: dict,
    date_iso: str,
    github_days_back: int = 0,
):
    """
    Sauvegarde métriques, rapport, tendances et alertes dans SentinelDB.
    [MAIN-48-B] github_days_back : 0 = pas de GitHub, >0 = fenêtre effective.
    """
    SentinelDB.savemetrics(
        date_iso,
        indice=metrics.get("indice", 5.0),
        alerte=metrics.get("alerte", "VERT"),
        nb_articles=metrics.get("nb_articles", 0),
        nb_pertinents=metrics.get("nb_pertinents", 0),
        github_days_back=github_days_back,
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

    missing = _check_dependencies()
    if missing:
        logger.error(f"DEPENDANCES MANQUANTES — pipeline annulé : {', '.join(missing)}")
        return

    date_obj = datetime.datetime.now(datetime.timezone.utc).date()
    date_iso = date_obj.isoformat()

    try:
        # ── 1. COLLECTE ──────────────────────────────────────────────────────
        articles = _timed("scrape_rss", scrape_all_feeds)

        if telegram_mod:
            try:
                tg_arts = _timed("telegram", telegram_mod.run_telegram_scraper)
                articles.extend(tg_arts or [])
            except Exception as e:
                logger.warning(f"Erreur Telegram: {e}")

        try:
            contracts = _timed("procurement", run_all_procurement, daysback=2)
            articles.extend(contracts)
            logger.info(f"Procurement : {len(contracts)} contrats ajoutés")
        except Exception as e:
            logger.warning(f"Erreur Procurement (non bloquant) : {e}")

        # ── GitHub — MAIN-46-A / MAIN-47-A ───────────────────────────────────
        has_github     = False
        last_run_iso   = None
        effective_days = DAYS_BACK_CFG

        if github_mod:
            should_run, effective_days = _should_run_github()
            if should_run:
                try:
                    if _GITHUB_LAST_RUN_PATH.exists():
                        last_run_iso = _GITHUB_LAST_RUN_PATH.read_text(
                            encoding="utf-8"
                        ).strip()
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

        # [MAIN-48-A] <system_notice> si rattrapage GitHub détecté
        if has_github and effective_days > GITHUB_COOLDOWN_DAYS:
            catchup_date_str = (
                f"le {last_run_iso}" if last_run_iso
                else f"il y a plus de {effective_days} jours"
            )
            catchup_note = (
                f"<system_notice>
"
                f"RATTRAPAGE GITHUB : la veille R&D n'a pas pu être exécutée "
                f"normalement. Fenêtre élargie à {effective_days} jours "
                f"(dernière exécution : {catchup_date_str}). "
                f"Le volume d'articles GitHub est supérieur à la normale — "
                f"ne pas interpréter ce volume comme un pic d'activité soudain. "
                f"Mentionner ce rattrapage dans la section Veille R&D (MODULE 4-BIS) "
                f"sous la forme : 'Note : analyse R&D élargie à {effective_days}j "
                f"(rattrapage du cycle manqué {catchup_date_str})'.
"
                f"</system_notice>

"
            )
            formatted_data = catchup_note + formatted_data
            logger.info(
                f"GITHUB <system_notice> injectée "
                f"(fenêtre={effective_days}j > cooldown={GITHUB_COOLDOWN_DAYS}j)"
            )

        report_type = "friday_rd" if has_github else "daily"
        logger.info(f"CLAUDE report_type={report_type!r}")

        report_text, _ = _timed(
            "claude_daily",
            run_sentinel,
            formatted_data,
            memory_context,
            date_obj,
            model=SONNET_MODEL,
            report_type=report_type,
        )
        if not report_text:
            raise RuntimeError("Claude n'a retourné aucun rapport.")

        # ── 3. EXTRACTION & DB ───────────────────────────────────────────────
        metrics = extract_metrics_from_report(report_text, date_iso)
        _process_report_data_to_db(
            report_text,
            metrics or {},
            date_iso,
            github_days_back=effective_days if has_github else 0,
        )

        # ── 4. RESTITUTION ───────────────────────────────────────────────────
        _timed("charts",         generate_all_charts,  report_text)
        html_report = _timed("report_builder", build_html_report, report_text, metrics)

        if html_report and Path(html_report).exists():
            _timed("mailer", send_report, html_report)
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
# RAPPORT MENSUEL
# ---------------------------------------------------------------------------
def run_monthly_report():
    """
    Rapport mensuel SENTINEL via MapReduce.

    Phase MAP    : Haiku compresse chaque rapport journalier (parallèle, 8 workers)
    Phase REDUCE : blocs hebdo → Sonnet → rapport mensuel final

    Annotations de biais (v3.48 → v3.50) :
        MAIN-48-C  Annotation locale [RATTRAPAGE GITHUB Xj + densité] dans le
                   titre de chaque bloc hebdo atypique.
        MAIN-49-A  <context_metadata> en tête d'injection si ≥1 semaine atypique.
        MAIN-50-A  Densité normalisée articles/jour intégrée aux deux annotations.
                   Calcul déterministe depuis DB (nb_articles / effective_window).
                   Zéro appel API supplémentaire — données déjà en base depuis v3.41.
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

    date_iso = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    mois     = datetime.datetime.now(datetime.timezone.utc).strftime("%B %Y")

    try:
        from memory_manager import compress_text_haiku
        from sentinel_api import run_sentinel_monthly

        # ── Récupération des 30 derniers rapports ─────────────────────────
        rows = SentinelDB.get_last_n_reports(30)
        if not rows:
            logger.warning("run_monthly_report : aucun rapport en base — abandon.")
            return

        # [MAIN-48-C / MAIN-50-A] Un seul appel getmetrics() pour :
        #   - github_days_back (annotation rattrapage)
        #   - nb_articles      (densité normalisée)
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

        # ── Phase MAP parallèle — MAIN-44-FIX1 ───────────────────────────
        raw_texts:    list[str]  = [(row.get("rawtail") or "").strip() for row in rows]
        indexed_raws: list[tuple[int, str]] = [
            (i, t) for i, t in enumerate(raw_texts) if t
        ]
        compressed_reports: list[str] = [""] * len(rows)
        dates_by_index:     list[str] = [row.get("date", "") for row in rows]

        t_map_start = timeperf.perf_counter()

        def _compress_one(args: tuple[int, str]) -> tuple[int, str]:
            idx, raw = args
            try:
                return idx, compress_text_haiku(raw)
            except Exception as e:
                logger.warning(f"MAP rapport {idx + 1} : erreur Haiku ({e}) — fallback")
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
                except Exception as e:
                    orig_idx = futures[future]
                    logger.warning(f"MAP future {orig_idx + 1} : erreur ({e})")
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

        # ── Phase REDUCE : regroupement par semaine ───────────────────────
        def _chunk(lst: list, size: int) -> list[list]:
            return [lst[i:i + size] for i in range(0, len(lst), size)]

        weekly_summaries: list[str] = []
        # [MAIN-49-A / MAIN-50-A] tuple élargi : (numéro_semaine, max_gdb, density)
        catchup_weeks: list[tuple[int, int, float]] = []

        for w_idx, week in enumerate(_chunk(filled, 7), 1):
            week_texts = [s for s, _ in week]
            week_dates = [d for _, d in week]

            # Détection rattrapage
            max_gdb = max(
                (metrics_by_date.get(d, 0) for d in week_dates if d),
                default=0,
            )

            # [MAIN-50-A] Densité normalisée : articles totaux / fenêtre effective
            # effective_window = max_gdb si rattrapage, sinon COOLDOWN (cycle normal)
            # Déterministe : nb_articles exact depuis DB, division simple.
            total_articles   = sum(articles_by_date.get(d, 0) for d in week_dates if d)
            effective_window = (
                max_gdb if max_gdb > GITHUB_COOLDOWN_DAYS
                else GITHUB_COOLDOWN_DAYS
            )
            density = (
                round(total_articles / effective_window, 1)
                if effective_window > 0 else 0.0
            )

            # [MAIN-49-A / MAIN-50-A] Mémorisation pour <context_metadata>
            if max_gdb > GITHUB_COOLDOWN_DAYS:
                catchup_weeks.append((w_idx, max_gdb, density))

            # [MAIN-48-C + MAIN-50-A] Annotation locale enrichie avec densité
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
                    f"max_gdb={max_gdb}j, density={density} articles/jour → annotation"
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
            except Exception as e:
                logger.warning(f"REDUCE semaine {w_idx} : {e}")
                weekly_summaries.append(
                    f"[SEMAINE {w_idx}{catchup_flag}]
{chr(10).join(week_texts)[:500]}"
                )

        injection = "

".join(weekly_summaries)

        # [MAIN-49-A + MAIN-50-A] <context_metadata> si ≥1 semaine atypique
        if catchup_weeks:
            # Semaines normales pour référence de densité (comparaison explicite)
            normal_weeks_densities = [
                (i + 1, round(
                    sum(articles_by_date.get(d, 0) for d in [dt for _, dt in chunk])
                    / GITHUB_COOLDOWN_DAYS, 1
                ))
                for i, chunk in enumerate(_chunk(filled, 7))
                if (i + 1) not in {w for w, _, _ in catchup_weeks}
            ]
            normal_ref_str = ""
            if normal_weeks_densities:
                avg_normal = round(
                    sum(dens for _, dens in normal_weeks_densities)
                    / len(normal_weeks_densities), 1
                )
                normal_ref_str = (
                    f"Densité moyenne des semaines normales : {avg_normal} articles/jour. "
                    f"Si la densité normalisée des semaines de rattrapage est proche "
                    f"de cette valeur, confirmer l'absence de pic d'activité réel.
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
                if i not in {w for w, _, _ in catchup_weeks}
            ]
            normal_str = (
                f"Semaines {', '.join(str(w) for w in normal_weeks_list)} : "
                f"cycle normal ({GITHUB_COOLDOWN_DAYS}j)."
                if normal_weeks_list else ""
            )

            context_metadata = (
                f"<context_metadata>
"
                f"BIAIS DE COLLECTE — RAPPORT MENSUEL {mois.upper()}
"
                f"Les semaines suivantes présentent un volume GitHub anormalement "
                f"élevé dû à un rattrapage de cycle :
"
                f"{lines}
"
                f"{normal_str}
"
                f"{normal_ref_str}"
                f"Consignes applicables à l'INTÉGRALITÉ du rapport :
"
                f"  1. Ne pas surpondérer l'activité GitHub des semaines annotées "
                f"dans les tendances long-terme.
"
                f"  2. La densité normalisée (articles/jour) est la métrique "
                f"pertinente, pas le volume total — utiliser cette densité pour "
                f"évaluer si l'activité est réellement supérieure à la normale.
"
                f"  3. Si la densité normalisée est comparable aux semaines normales, "
                f"conclure explicitement à l'absence de pic d'activité.
"
                f"  4. Mentionner ce rattrapage en note méthodologique en fin de rapport.
"
                f"</context_metadata>

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

        metrics = extract_metrics_from_report(report_text, date_iso)

        SentinelDB.savereport(
            date=date_iso,
            indice=(metrics or {}).get("indice", 5.0),
            alerte=(metrics or {}).get("alerte", "VERT"),
            compressed=json.dumps({
                "type":          "monthly",
                "mois":          mois,
                # [MAIN-50-A] density persistée pour audit futur
                "catchup_weeks": [
                    {"semaine": w, "gdb": g, "density": d}
                    for w, g, d in catchup_weeks
                ],
                **(metrics or {}),
            }),
            rawtail=report_text[:3000],
        )

        html_report = _timed(
            "report_builder_monthly", build_html_report, report_text, metrics
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
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TODAY = datetime.date.today()
    if TODAY.day == 1:
        try:
            run_monthly_report()
        except Exception as e:
            logger.error(f"Erreur rapport mensuel: {e}", exc_info=True)

    main()
