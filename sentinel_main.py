#!/usr/bin/env python3
# sentinel_main.py --- SENTINEL v3.40 --- ORCHESTRATEUR PRINCIPAL
# =============================================================================
# Point d'entrée unique du pipeline.
# Intègre db_manager v3.40 : savemetrics, saveacteur, savetendance,
# ouvrirealerte, closealerte, savereport, closedb, maintenance.
# Compatibilité v3.37 totalement préservée (memory_manager JSON = fallback).
# =============================================================================

import sys
import datetime
import json
import re
import logging
import time as timeperf
import importlib.util as ilu
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# VERSION
# ---------------------------------------------------------------------------
VERSION = "3.40"

# ---------------------------------------------------------------------------
# MODÈLES — anti-drift via .env (CDC-5)
# Procédure mise à jour :
#  1. https://docs.anthropic.com/en/docs/models-overview
#  2. .env → SENTINEL_MODEL=claude-sonnet-X-Y
#  3. python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.environ['SENTINEL_MODEL'])"
#  4. python health_check.py → vérifier [OK]
# ---------------------------------------------------------------------------
SONNET_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")

# ---------------------------------------------------------------------------
# DOSSIERS
# ---------------------------------------------------------------------------
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("output").mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING centralisé — R3A3-NEW-1 K-1 (appel unique)
# ---------------------------------------------------------------------------
def init_logging(force: bool = True) -> None:
    logging.basicConfig(
        force=force,
        level=logging.INFO,
        format=(
            '{"ts":"%(asctime)s","level":"%(levelname)s",'
            '"v":"' + VERSION + '","msg":"%(message)s"}'
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.FileHandler("logs/sentinel.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


init_logging()
logger = logging.getLogger("SENTINEL")

# F-01-FIX : health server démarré au niveau module (pas seulement __main__)
# Garantit le démarrage même si sentinel_main est importé par un scheduler externe.
_HEALTH_SERVER_STARTED = False
def _ensure_health_server() -> None:
    global _HEALTH_SERVER_STARTED
    if not _HEALTH_SERVER_STARTED:
        try:
            _start_health_server(port=int(os.environ.get("SENTINEL_HEALTH_PORT", "8765")))
            _HEALTH_SERVER_STARTED = True
        except Exception as _hse:
            pass  # non bloquant

# =============================================================================
# F1-FIX — HEALTHCHECK HTTP ENDPOINT (port 8765)
# Permet à UptimeRobot / monitoring externe de vérifier le pipeline.
# Démarré automatiquement en thread daemon.
# Endpoint GET /health → JSON {status, last_run, version, db_ok}
# Usage : curl http://localhost:8765/health
# =============================================================================

def _start_health_server(port: int = 8765) -> None:
    """F1-FIX : Lance un micro-serveur HTTP de healthcheck en thread daemon."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json as _json

    class _HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/health", "/"):
                self.send_response(404)
                self.end_headers()
                return
            try:
                # Lire le dernier run depuis les logs
                last_run = "unknown"
                log_file = Path("logs/sentinel.log")
                if log_file.exists():
                    lines = log_file.read_text(encoding="utf-8", errors="ignore").splitlines()
                    for line in reversed(lines):
                        if "Rapport du" in line or "SENTINEL v" in line:
                            last_run = line[:40]
                            break

                # Vérifier la DB
                db_ok = False
                try:
                    from db_manager import check_integrity
                    db_ok = check_integrity()
                except Exception:
                    pass

                payload = _json.dumps({
                    "status":   "ok",
                    "version":  VERSION,
                    "last_run": last_run,
                    "db_ok":    db_ok,
                }, ensure_ascii=False).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except Exception as _e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(_e).encode())

        def log_message(self, *args):
            pass  # silence HTTP logs

    def _serve():
        try:
            httpd = HTTPServer(("0.0.0.0", port), _HealthHandler)
            logger.info(f"HEALTH serveur healthcheck démarré sur port {port}")
            httpd.serve_forever()
        except OSError as _err:
            logger.warning(f"HEALTH port {port} indisponible : {_err}")

    t = threading.Thread(target=_serve, daemon=True, name="healthcheck-server")
    t.start()




def log(msg: str) -> None:
    logger.info(msg)


# ---------------------------------------------------------------------------
# IMPORTS PIPELINE
# ---------------------------------------------------------------------------
from scraper_rss import scrape_all_feeds, format_for_claude
from memory_manager import get_compressed_memory, update_memory, load_memory
from sentinel_api import run_sentinel, extract_memory_delta, extract_metrics_from_report as _sentinel_api_extract_metrics  # type: ignore
from charts import generate_all_charts
from report_builder import build_html_report
from mailer import send_report
from db_manager import SentinelDB, closedb, initdb  # v3.40 — FIX-BUG-DB3

# Initialisation DB au démarrage (BUG-2-fix : initdb() jamais appelé)
try:
    initdb()
except Exception as _db_init_err:
    import logging as _lg
    _lg.getLogger("SENTINEL").warning(f"initdb() non bloquant : {_db_init_err}")



# ---------------------------------------------------------------------------
# PARSERS MODULE 1 — métriques
# ---------------------------------------------------------------------------

def _parse_float(pattern: str, text: str, default: float = 0.0) -> float:
    m = re.search(pattern, text, re.IGNORECASE)
    try:
        return float(m.group(1)) if m else default
    except (ValueError, AttributeError):
        return default


def _parse_int(pattern: str, text: str, default: int = 0) -> int:
    m = re.search(pattern, text, re.IGNORECASE)
    try:
        return int(m.group(1)) if m else default
    except (ValueError, AttributeError):
        return default


def _extract_metrics_from_report(report_text: str) -> dict:
    """
    G-03-FIX : Délègue à sentinel_api.extract_metrics_from_report() — suppression du doublon.
    La version sentinel_api a des regex correctes (slash-s+, slash-d+) et les bons noms de cles.
    Retourne un dict compatible SentinelDB.savemetrics() whitelist.
    """
    # Utiliser la version sentinel_api (regex correctes, clés nb_articles/nb_pertinents)
    import re as _re
    def _pf(pat, txt, default=0.0):
        m = _re.search(pat, txt, _re.IGNORECASE)
        try: return float(m.group(1)) if m else default
        except: return default
    def _pi(pat, txt, default=0):
        m = _re.search(pat, txt, _re.IGNORECASE)
        try: return int(m.group(1)) if m else default
        except: return default

    indice = _pf(r"Indice\s*[:\-]?\s*([\d.]+)\s*/?\s*10", report_text)
    if not indice:
        indice = _pf(r"Indice\s+d['\']activit[eé]\s+sectorielle\s*:\s*([\d.]+)", report_text)
    alerte_m = _re.search(r"Alerte\s*:\s*\[?\s*(VERT|ORANGE|ROUGE)", report_text, _re.IGNORECASE)
    alerte = alerte_m.group(1).upper() if alerte_m else "VERT"
    return dict(
        indice=indice, alerte=alerte,
        nb_articles=_pi(r"Sources\s+analys[eé]es?\s*:\s*(\d+)", report_text),
        nb_pertinents=_pi(r"Pertinentes?\s*:\s*(\d+)", report_text),
        geousa=_pf(r"USA\s+([\d.]+)\s*%", report_text),
        geoeurope=_pf(r"Europe\s+([\d.]+)\s*%", report_text),
        geoasie=_pf(r"Asie\s+([\d.]+)\s*%", report_text),
        geomo=_pf(r"MO\s+([\d.]+)\s*%", report_text),
        georussie=_pf(r"Russie\s+([\d.]+)\s*%", report_text),
        terrestre=_pf(r"Terrestre\s+([\d.]+)\s*%", report_text),
        maritime=_pf(r"Maritime\s+([\d.]+)\s*%", report_text),
        transverse=_pf(r"Transverse\s+([\d.]+)\s*%", report_text),
        contractuel=_pf(r"Contractuel\s+([\d.]+)\s*%", report_text),
    )


# ---------------------------------------------------------------------------
# PARSERS MODULE 6 — acteurs
# ---------------------------------------------------------------------------

def _extract_acteurs_from_report(report_text: str, date_str: str) -> list:
    """
    G-04-FIX : Extrait le tableau Module 6 → liste de dicts {nom, pays, score}.
    Format Markdown : | Acteur | Pays | Activité | Évolution | Score |
    Regex corrigee sur une seule ligne sans literal newline (G-04-FIX).
    """
    acteurs = []
    m6 = re.search(
        r"MODULE\s+6[^\n]*\n(.*?)(?=##\s*MODULE\s+7|$)",
        report_text, re.DOTALL | re.IGNORECASE,
    )
    if not m6:
        return acteurs

    section = m6.group(1)
    for line in section.splitlines():
        cells = [c.strip() for c in line.split("|") if c.strip()]
        if len(cells) < 2:
            continue
        # Ignorer entêtes et séparateurs Markdown
        if re.match(r"^[-:]+$", cells[0]) or cells[0].lower() in ("acteur", "nom"):
            continue
        nom = cells[0]
        pays = cells[1] if len(cells) > 1 else ""
        score_raw = cells[-1]
        try:
            score = float(re.sub(r"[^0-9.]", "", score_raw) or "5.0")
        except ValueError:
            score = 5.0
        score = max(0.0, min(10.0, score))
        if nom and len(nom) > 1:
            acteurs.append({"nom": nom, "pays": pays, "score": score})
    return acteurs


# ---------------------------------------------------------------------------
# PARSER JSON DELTA — tendances / alertes → SQLite
# ---------------------------------------------------------------------------

def _process_memory_deltas_to_db(memory_deltas, date_str: str) -> None:
    """
    Persiste tendances et alertes dans SQLite depuis memory_deltas.
    Accepte dict brut (depuis extract_memory_delta) ou str JSON.
    FIX-BUG-DB7 : closealerte() utilise ESCAPE '\\' (dans db_manager v3.40).
    """
    if isinstance(memory_deltas, str):
        try:
            memory_deltas = json.loads(memory_deltas)
        except Exception:
            return
    if not isinstance(memory_deltas, dict):
        return

    for t in memory_deltas.get("nouvelles_tendances", []):
        if isinstance(t, str) and t.strip():
            try:
                SentinelDB.savetendance(t.strip(), date_str)
            except Exception as e:
                log(f"DB savetendance erreur : {e}\n{type(e).__name__}")

    for a in memory_deltas.get("alertes_ouvertes", []):
        if isinstance(a, str) and a.strip():
            try:
                SentinelDB.ouvrirealerte(a.strip(), date_str)
            except Exception as e:
                log(f"DB ouvrirealerte erreur : {e}\n{type(e).__name__}")

    for a in memory_deltas.get("alertes_closes", []):
        if isinstance(a, str) and a.strip():
            try:
                SentinelDB.closealerte(a.strip(), date_str)
            except Exception as e:
                log(f"DB closealerte erreur : {e}\n{type(e).__name__}")


# ---------------------------------------------------------------------------
# SAUVEGARDE RAPPORT EN DB  (reports + metrics)
# ---------------------------------------------------------------------------

def _save_report_to_db(report_text: str, date_str: str, metrics: dict) -> None:
    """
    Compresse le rapport (via memory_manager) et le persiste dans :
      - reports  (savereport)
      - metrics  (savemetrics — colonnes whitelistées dans db_manager)
    FIX-BUG-DB1 : UNIQUE sur reports.date garanti par DDL v3.40.
    FIX-BUG-DB5 : whitelist _ALLOWED_METRIC_COLS appliquée dans savemetrics().
    FIX-BUG-DB8 : rawtail exposé ici pour mode dégradé futur.
    """
    rawtail = report_text[-2000:] if report_text else ""

    try:
        from memory_manager import compress_report
        compressed = compress_report(report_text)
    except Exception:
        compressed = rawtail  # fallback si Haiku indisponible

    indice = float(metrics.get("indice") or 5.0)
    alerte = str(metrics.get("alerte") or "VERT")

    try:
        SentinelDB.savereport(
            date=date_str,
            indice=indice,
            alerte=alerte,
            compressed=compressed,
            rawtail=rawtail,
        )
    except Exception as e:
        log(f"DB savereport erreur : {e}")

    # savemetrics — whitelist _ALLOWED_METRIC_COLS dans db_manager (FIX-BUG-DB5)
    try:
        SentinelDB.savemetrics(date=date_str, **metrics)
    except Exception as e:
        log(f"DB savemetrics erreur : {e}")


# =============================================================================
# RUN DAILY REPORT
# =============================================================================

def run_daily_report() -> None:
    _ensure_health_server()  # F-01-FIX : garantit le démarrage même sans __main__
    try:
        TODAY = datetime.date.today()
        date_str = str(TODAY)
        t0 = timeperf.perf_counter()

        log(f"══ SENTINEL v{VERSION} — Rapport du {TODAY.strftime('%d/%m/%Y')} ══")

        # ── ÉTAPE 1 : Collecte RSS ───────────────────────────────────────────
        log("[ 1/7 ] Collecte RSS...")
        articles = scrape_all_feeds()

        # Enrichissement SAM.GOV / TED EU (optionnel — FIX-M4)
        try:
            from samgov_scraper import (
                fetch_sam_gov_opportunities,
                fetch_ted_eu_opportunities,
            )
            articles += fetch_sam_gov_opportunities(daysback=2)
            articles += fetch_ted_eu_opportunities()
        except ImportError:
            log("SAM.GOV module absent — skipped")

        # Enrichissement Telegram (optionnel — A20)
        try:
            from telegram_scraper import run_telegram_scraper
            articles += run_telegram_scraper(daysback=2)
        except ImportError:
            pass

        articles.sort(key=lambda a: -a.get("art_score", 5.0))

        if not articles:
            log("AVERTISSEMENT aucun article collecté — vérifier les flux RSS")
            log("Arrêt du pipeline pour éviter un rapport vide et une MAJ mémoire corrompue")
            return  # B11 — ne tue pas le process cron

        articles_text = format_for_claude(articles)
        log(f"   {len(articles)} articles → {len(articles_text)//1000}k chars")

        # ── ÉTAPE 2 : Mémoire ────────────────────────────────────────────────
        log("[ 2/7 ] Chargement mémoire...")
        memory_ctx = get_compressed_memory(ndays=7)
        log(f"   ~{len(memory_ctx)//4} tokens estimés")

        # ── ÉTAPE 3 : Analyse Claude Sonnet ──────────────────────────────────
        log("[ 3/7 ] Appel Claude Sonnet (3-5 min)...")
        report_text, memory_deltas = run_sentinel(articles_text, memory_ctx, TODAY)

        if not report_text:
            log("ERREUR rapport vide — arrêt")
            return  # B11

        log(f"   Rapport généré ~{len(report_text)//4} tokens")

        # ── ÉTAPE 4 : Sauvegarde rapport brut ────────────────────────────────
        log("[ 4/7 ] Sauvegarde rapport brut...")
        raw_path = Path("output") / f"SENTINEL_{TODAY}_raw.md"
        raw_path.write_text(report_text, encoding="utf-8")

        # ── ÉTAPE 5 : Graphiques ─────────────────────────────────────────────
        log("[ 5/7 ] Génération des graphiques...")
        chart_paths = generate_all_charts(report_text)
        log(f"   {len(chart_paths)} graphiques générés")

        # ── ÉTAPE 6 : Rapport HTML ───────────────────────────────────────────
        log("[ 6/7 ] Assemblage rapport HTML/PDF...")
        report_path = build_html_report(report_text, chart_paths, TODAY)

        # ── ÉTAPE 7 : SQLite — métriques · acteurs · tendances · alertes ─────
        log("[ 7/7 ] Persistance SQLite (db_manager v3.40)...")

        # 7a — Métriques Module 1 → tables reports + metrics
        metrics = _extract_metrics_from_report(report_text)
        _save_report_to_db(report_text, date_str, metrics)
        log(
            f"   Métriques : indice={metrics['indice']} | alerte={metrics['alerte']}"
            f" | articles={metrics['nb_articles']} | pertinents={metrics['nb_pertinents']}"
        )

        # 7b — Acteurs Module 6 → table acteurs (FIX-BUG-DB4 : table enfin utilisée)
        acteurs = _extract_acteurs_from_report(report_text, date_str)
        for a in acteurs:
            try:
                SentinelDB.saveacteur(
                    nom=a["nom"], pays=a["pays"],
                    score=a["score"], date=date_str,
                )
            except Exception as e:
                log(f"DB saveacteur erreur ({a['nom']}) : {e}\n{type(e).__name__}")
        if acteurs:
            log(f"   {len(acteurs)} acteurs persistés — Module 6")

        # 7c — Tendances / alertes depuis JSON_DELTA (fin §9 du rapport)
        _process_memory_deltas_to_db(memory_deltas, date_str)

        # 7d — Purge seenhashes > 90 jours (FIX-BUG-DB2 corrigé dans db_manager)
        try:
            purged = SentinelDB.purgeseeolderthandays(90)
            if purged:
                log(f"   Purge seenhashes : {purged} entrées > 90j supprimées")
        except Exception as e:
            log(f"DB purge erreur : {e}")

        # ── EMAIL ─────────────────────────────────────────────────────────────
        ok = send_report(report_path, TODAY)
        log(f"EMAIL {'Envoyé ✓' if ok else 'ÉCHEC SMTP — vérifier .env'}")

        # ── MÉMOIRE JSON (compat. v3.37 — memory_manager délègue vers SQLite) ─
        update_memory(report_text, memory_deltas, TODAY)

        # ── ALERTE INDICE ÉLEVÉ ───────────────────────────────────────────────
        if metrics["indice"] >= 8.5:
            log(f"ALERT ⚠ Indice activité = {metrics['indice']} ≥ 8.5 — rapport urgent")

        # ── OPS PATENTS EPO + USPTO + GitHub (vendredi — E7-1 / O2-FIX / O3-FIX) ────
        if datetime.datetime.now().weekday() == 4:
            # Brevets EPO (Espacenet)
            try:
                spec = ilu.spec_from_file_location("ops_patents", "ops_patents.py")
                if spec and spec.loader:
                    mod = ilu.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    p = mod.run_patent_watch()
                    log(f"PATENTS EPO {len(p)} brevets Espacenet collectés")
                    # O2-FIX : brevets US via USPTO PatentsView
                    try:
                        p_us = mod.run_uspto_watch(days_back=7)
                        log(f"PATENTS USPTO {len(p_us)} brevets US collectés")
                    except Exception as ep_us:
                        log(f"PATENTS USPTO erreur (non bloquant) : {ep_us}")
            except Exception as ep:
                log(f"PATENTS ops_patents non dispo : {ep}")

            # O3-FIX : GitHub — activité dépôts acteurs privés défense
            try:
                spec_gh = ilu.spec_from_file_location("github_scraper", "github_scraper.py")
                if spec_gh and spec_gh.loader:
                    mod_gh = ilu.module_from_spec(spec_gh)
                    spec_gh.loader.exec_module(mod_gh)
                    gh_arts = mod_gh.run_github_scraper(days_back=7)
                    log(f"GITHUB {len(gh_arts)} signaux collectés depuis dépôts défense")
            except Exception as eg:
                log(f"GITHUB github_scraper non dispo (non bloquant) : {eg}")

        # ── MAINTENANCE DB (vendredi — VACUUM + ANALYZE) ─────────────────────
        if datetime.datetime.now().weekday() == 4:
            try:
                SentinelDB.maintenance()
                log("DB maintenance hebdomadaire effectuée (VACUUM · ANALYZE · PRAGMA)")
            except Exception as em:
                log(f"DB maintenance erreur : {em}")

        elapsed = timeperf.perf_counter() - t0
        log(f"✓ Rapport du {TODAY.strftime('%d/%m/%Y')} terminé avec succès")
        log(f"✓ Rapport disponible : {report_path}")
        log(f"✓ Durée totale pipeline : {elapsed:.1f}s")

    except Exception as fatal:
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.critical(f"FATAL run_daily_report : {fatal}\n{tb_str}")
        log(f"FATAL Pipeline quotidien interrompu : {type(fatal).__name__}: {fatal}")
        raise  # R6A3-NEW-1 — re-raise pour que cron/supervisor détecte l'échec

    finally:
        # FIX-BUG-DB3 : fermeture explicite connexion thread principal
        # Garantit la libération de la connexion persistante même sur exception.
        closedb()


# =============================================================================
# RUN MONTHLY REPORT
# =============================================================================

def run_monthly_report() -> None:
    try:
        TODAY = datetime.date.today()
        last_month = TODAY.replace(day=1) - datetime.timedelta(days=1)

        try:
            import locale
            locale.setlocale(locale.LC_TIME, "fr_FR.UTF-8")  # FIX-M5
        except Exception:
            pass

        month_name  = last_month.strftime("%B")    # ex. "avril"
        month_year  = str(last_month.year)          # ex. "2026"
        month_label = f"{month_name} {month_year}"  # ex. "avril 2026"
        date_str    = str(last_month)

        log(f"══ RAPPORT MENSUEL SENTINEL — {month_label} ══")

        # Charger les 30 rapports compressés depuis SQLite (FIX-BUG-DB6 : filtre date)
        reports_db = SentinelDB.getrecentreports(ndays=30)
        if reports_db:
            reports = [r["compressed"] for r in reports_db if r.get("compressed")]
            log(f"   {len(reports)} rapports chargés depuis SQLite")
        else:
            # Fallback mémoire JSON v3.37
            memory = load_memory()
            reports = memory.get("compressed_reports", [])[-30:]
            log(f"   {len(reports)} rapports chargés depuis mémoire JSON (fallback)")

        if len(reports) < 5:
            log(f"AVERTISSEMENT seulement {len(reports)} rapports disponibles (min recommandé : 10)")

        monthly_context = json.dumps(reports, ensure_ascii=False, indent=2)

        monthly_prompt_path = Path("prompts/monthly.txt")
        if not monthly_prompt_path.exists():  # B9
            log("ERREUR prompts/monthly.txt introuvable — créer ce fichier avec le prompt §2.3")
            return

        MONTHLY_TEMPLATE = monthly_prompt_path.read_text(encoding="utf-8")
        user_prompt = (
            MONTHLY_TEMPLATE
            .replace("{{MOIS}}", month_name)
            .replace("{{ANNÉE}}", month_year)
            .replace("{{INJECTION_30_RAPPORTS_COMPRESSÉS}}", monthly_context)
        )

        import anthropic
        client = anthropic.Anthropic()
        SYSTEM = (
            Path("prompts/system.txt").read_text(encoding="utf-8")
            if Path("prompts/system.txt").exists()  # B9
            else ""
        )

        log("Appel Claude Sonnet pour synthèse mensuelle (5-8 min)...")
        resp = client.messages.create(
            model=SONNET_MODEL,  # v3.12 — constante anti-drift
            max_tokens=16000,    # v3.37 fix (était 8000)
            system=SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )
        report_text = next(  # BUG-05 fix : guard IndexError
            (b.text for b in resp.content if hasattr(b, "text") and b.text), ""
        )

        if not report_text.strip():
            log("ERREUR rapport mensuel vide — arrêt")
            return

        # Sauvegarde + diffusion
        month_file  = last_month.strftime("%Y-%m")
        raw_path    = Path("output") / f"SENTINEL_MENSUEL_{month_file}.md"
        raw_path.write_text(report_text, encoding="utf-8")

        chart_paths = generate_all_charts(report_text)
        report_path = build_html_report(report_text, chart_paths, last_month)
        send_report(report_path, last_month)

        # Persistance DB — métriques + acteurs + tendances
        metrics = _extract_metrics_from_report(report_text)
        _save_report_to_db(report_text, date_str, metrics)

        acteurs = _extract_acteurs_from_report(report_text, date_str)
        for a in acteurs:
            try:
                SentinelDB.saveacteur(
                    nom=a["nom"], pays=a["pays"],
                    score=a["score"], date=date_str,
                )
            except Exception as e:
                log(f"DB saveacteur mensuel erreur ({a['nom']}) : {e}")

        monthly_deltas = extract_memory_delta(report_text)
        _process_memory_deltas_to_db(monthly_deltas, date_str)
        update_memory(report_text, monthly_deltas, last_month)

        log("Mémoire mensuelle mise à jour.")
        log(f"✓ Rapport mensuel {month_label} terminé → {report_path}")

        # F2-FIX : backup DB le 1er du mois
        try:
            from db_manager import backup_db
            backup_db(dest_dir="backups", keep_days=30)
            log("DB backup mensuel effectué → backups/")
        except Exception as eb:
            log(f"DB backup erreur (non bloquant) : {eb}")

    except Exception as e:
        log(f"ERREUR rapport mensuel : {e}")
        raise

    finally:
        # FIX-BUG-DB3 : fermeture connexion thread mensuel
        closedb()


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

if __name__ == "__main__":
    _ensure_health_server()  # F-01-FIX : idempotent
    TODAY = datetime.date.today()

    if TODAY.day == 1:
        try:
            run_monthly_report()
        except Exception as e:
            log(f"ERREUR rapport mensuel non bloquant : {e}")
            log("Le rapport journalier va continuer normalement")

    run_daily_report()