#!/usr/bin/env python3
# samgov_scraper.py — SENTINEL v3.43 — Appels d'offres défense DoD/TED/BOAMP
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 (originales) :
#   FIX-SAM1    deptname="DEPT OF DEFENSE" + ptype="o,p,k"
#   FIX-SAM2    TED API v3 (POST JSON) + CPV défense
#   FIX-SAM3    seenids set global par run → zéro doublon multi-keyword
#   FIX-SAM4    noticeId + art_score=8.5 signal contractuel
#   FIX-OBS1    Logging structuré via logger nommé, zéro print()
#   FIX-M4      Tri par art_score en sortie
#   O5-FIX      BOAMP_URL déclaré avant scrape_boamp_fr()
#   E4-OSINT2   BOAMP.fr API publique gratuite
#   E4-OSINT3   SAMGOV_KW complète v3.40 (50 keywords)
#   A15-FIX     TED EU — appel officiel POST JSON v3
#   NEW-SG1     MIN_AMOUNT configurable via ENV
#   NEW-SG2     _save_to_db() — enregistrement optionnel SentinelDB
#   NEW-SG3     run_all_procurement() — point d'entrée unique
#   NEW-SG4     Résumé de collecte logué en fin de run
#   NEW-SG5     Timeout & User-Agent cohérents avec scraper_rss.py
#
# Corrections v3.41 :
#   SG-41-FIX1  Double appel scrape_boamp_fr() supprimé dans run_all_procurement()
#   SG-41-FIX2  User-Agent dédié SENTINEL/3.41 — SENTINEL_MODEL est le nom Claude
#   SG-41-FIX3  CPV codes inexistants supprimés : 35700000 et 38820000
#   SG-41-FIX4  datetime.utcnow() → datetime.now(timezone.utc) Python 3.12+
#   SG-41-FIX5  ThreadPoolExecutor(max_workers=8) pour les N requêtes SAM.gov
#   SG-41-FIX6  _save_to_db() : set transmis proprement à SentinelDB.saveseen()
#   SG-41-FIX7  CLI : print() → logger (cohérence FIX-OBS1)
#   SG-41-FIX8  MIN_AMOUNT=0 documenté (pre-awards sans valeur passent)
#
# Corrections v3.42 :
#   SG-42-FIX1  Retry tenacity sur _fetch_kw() — SAM.gov 504 Gateway Timeout.
#               3 tentatives, backoff exponentiel 2s→10s. reraise=False :
#               un keyword en échec retourne [] sans crasher le pool.
#   SG-42-FIX2  Filtre MIN_AMOUNT étendu à TED EU et BOAMP FR avec conversion
#               EUR/USD pivot. Taux configurable via SENTINEL_EUR_TO_USD.
#               Les contrats sans montant (0.0) passent intentionnellement.
#
# Corrections v3.43 — Audit inter-scripts (2026-04) :
#
#   SG-43-FIX1  BUG CRITIQUE : "artscore" → "art_score" dans _make_article()
#               et dans le paramètre de la fonction (artscore → art_score).
#               IMPACT : le champ "artscore" (sans underscore) n'est jamais lu
#               par format_for_claude() dans scraper_rss.py, ni par le tri de
#               sentinel_main.py — tous deux utilisent "art_score" (underscore).
#               Résultat v3.42 : tous les contrats DoD/TED/BOAMP étaient classés
#               art_score=0.0 → relégués en bas de liste → invisibles à Claude.
#               Tous les call sites _make_article(artscore=X) → art_score=X.
#               Tous les sorts -a["artscore"] → -a["art_score"].
#
#   SG-43-FIX2  Keywords chargés depuis config/keywords.toml section
#               [samgov_keywords] avec fallback sur SAMGOV_KW hardcodé.
#               Aligne avec scraper_rss.py v3.42 (SCR-42-FIX1) — source de
#               vérité unique pour les keywords partagés entre scrapers.
#               Format TOML : [samgov_keywords] / keywords = ["ugv", ...]
#
#   SG-43-FIX3  _article_hash() défini localement — supprime la dépendance
#               implicite à scraper_rss.py dans _save_to_db(). L'ancienne
#               implémentation utilisait SHA-256 sans séparateur → collisions
#               théoriques possibles. Correction alignée SCR-41-FIX2 :
#               SHA-256[:20] avec séparateur  entre titre et url.
#
#   SG-43-FIX4  Python ≥ 3.10 vérifié en tête — cohérence avec les autres
#               scripts du pipeline (health_check, watchdog, telegram_scraper).
#
#   SG-43-FIX5  BOAMP _source : tous les accès dict protégés par .get() avec
#               valeur par défaut explicite. L'ancienne implémentation levait
#               KeyError silencieux si un champ optionnel était absent de la
#               réponse → articles BOAMP perdus sans warning.
#
#   SG-43-FIX6  Chemins absolus via Path(__file__).resolve().parent.
#               Cohérence avec sentinel_main.py MAIN-60-FIX3,
#               sentinel_api.py API-60-FIX1, ops_patents.py V352-FIX1.
#
#   SG-43-FIX7  CLI enrichi avec argparse + --dry-run + --source.
#               L'ancien CLI (sys.argv[1]) ne permettait que le daysback.
#               Cohérence avec telegram_scraper.py TG-R11.
#
# ─────────────────────────────────────────────────────────────────────────────
# Dépendances : requests, tenacity, python-dotenv (opt.)
# Variables d'environnement :
#   SAM_GOV_API_KEY            Clé API SAM.gov (gratuite sur sam.gov/developers)
#   SENTINEL_MIN_CONTRACT_USD  Seuil filtrage montant USD (défaut : 1 000 000)
#   SENTINEL_EUR_TO_USD        Taux de change EUR→USD pivot (défaut : 1.08)
#   SENTINEL_SCRAPER_TIMEOUT   Timeout HTTP en secondes (défaut : 15)
#   Note : montant=0 (champ absent / pre-award) passe toujours (SG-41-FIX8)
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python samgov_scraper.py               Collecte complète (2 derniers jours)
#   python samgov_scraper.py --days 7      Fenêtre élargie
#   python samgov_scraper.py --source sam  SAM.gov uniquement
#   python samgov_scraper.py --source ted  TED EU uniquement
#   python samgov_scraper.py --source boamp BOAMP FR uniquement
#   python samgov_scraper.py --dry-run     Config + format article sans appel API
# ─────────────────────────────────────────────────────────────────────────────
# Intégration sentinel_main.py :
#   from samgov_scraper import run_all_procurement
#   contracts = run_all_procurement(daysback=2)
#   articles.extend(contracts)   # art_score float + score "A" label ✓
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from tenacity import (  # SG-42-FIX1
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Charger .env si disponible ────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Python ≥ 3.10 (SG-43-FIX4) ───────────────────────────────────────────────
if sys.version_info < (3, 10):
    sys.stderr.write(
        f"SAMGOV_SCRAPER CRITIQUE : Python "
        f"{sys.version_info.major}.{sys.version_info.minor} < 3.10
"
        "Requis : Python 3.10+ → sudo apt install python3.10
"
    )
    sys.exit(2)

# ── Logging structuré (FIX-OBS1) ─────────────────────────────────────────────
log = logging.getLogger("sentinel.samgov")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

# ── Version & User-Agent (SG-41-FIX2) ────────────────────────────────────────
_VERSION = "3.43"
_HEADERS  = {"User-Agent": f"SENTINEL/{_VERSION} (defence-osint-monitor)"}

# ── Chemins absolus (SG-43-FIX6) ─────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent
_CONFIG_DIR   = _PROJECT_ROOT / "config"

# ── Seuil montant minimum (NEW-SG1 / SG-42-FIX2) ─────────────────────────────
# MIN_AMOUNT en USD (référence SAM.gov).
# MIN_AMOUNT_EUR = pivot pour TED EU et BOAMP FR.
# SG-41-FIX8 : montant=0.0 (champ absent / pre-award) passe intentionnellement.
MIN_AMOUNT     = float(os.environ.get("SENTINEL_MIN_CONTRACT_USD", "1000000"))
EUR_TO_USD     = float(os.environ.get("SENTINEL_EUR_TO_USD",        "1.08"))
MIN_AMOUNT_EUR = MIN_AMOUNT / EUR_TO_USD   # ~926 k€ si MIN=1M USD

# ── Timeout HTTP configurable (NEW-SG5) ───────────────────────────────────────
_TIMEOUT = int(os.environ.get("SENTINEL_SCRAPER_TIMEOUT", "15"))

# ── Clé API SAM.gov ───────────────────────────────────────────────────────────
SAM_GOV_KEY = os.environ.get("SAM_GOV_API_KEY", "")

# ── API endpoints ─────────────────────────────────────────────────────────────
SAM_GOV_API = "https://api.sam.gov/opportunities/v2/search"
TED_API     = "https://api.ted.europa.eu/v3/notices/search"
TED_V2_API  = "https://ted.europa.eu/api/v2.0/notices/search"
BOAMP_URL   = "https://www.boamp.fr/api/search"   # O5-FIX

# =============================================================================
# KEYWORDS — chargement depuis config/keywords.toml (SG-43-FIX2)
# Section attendue : [samgov_keywords]  /  keywords = ["ugv", ...]
# Fallback sur la liste hardcodée si fichier absent / section manquante / erreur.
# =============================================================================

_SAMGOV_KW_FALLBACK: list[str] = [
    # ── Systèmes terrestres ──────────────────────────────────────────────────
    "unmanned ground vehicle",
    "unmanned surface vessel",
    "autonomous underwater vehicle",
    "drone defense",
    "loitering munition",
    "military robot",
    "UGV", "USV", "UUV",
    "autonomous weapon system",
    # ── Contre-systèmes ──────────────────────────────────────────────────────
    "counter-UAS",
    "C-UAS anti-drone",
    # ── Technologies transverses ─────────────────────────────────────────────
    "directed energy",
    "hypersonic",
    "autonomous combat",
    # ── Programmes CCA / AUKUS ───────────────────────────────────────────────
    "collaborative combat aircraft",
    "loyal wingman",
    "attritable uav",
    "ai-enabled warfare",
    "electronic warfare",
    # ── UGV programmes OTAN critiques ────────────────────────────────────────
    "mission master",
    "black knight ugv",
    "type x ugv",
    "rheinmetall ugv",
    "titan ugv",
    "camel ugv",
    "rex mk2",
    # ── USV/UUV programmes critiques ─────────────────────────────────────────
    "sea hunter",
    "ghost fleet overlord",
    "orca uuv",
    "manta ray darpa",
    "ghost shark uuv",
    "mine countermeasure autonomous",
    "extra large uuv",
    "xluuv",
    # ── Doctrines / concepts ─────────────────────────────────────────────────
    "manned unmanned teaming",
    "MUM-T",
    "lethal autonomous weapon",
    "mosaic warfare",
    "multi-domain operations",
    # ── IA militaire 2025-2026 ───────────────────────────────────────────────
    "machine learning targeting",
    "autonomous decision-making",
    "large language model intelligence",
    "golden horde darpa",
    # ── Acteurs industriels disruptifs ───────────────────────────────────────
    "anduril",
    "shield ai",
]


def _load_samgov_keywords() -> list[str]:
    """
    SG-43-FIX2 : Charge les keywords depuis config/keywords.toml si disponible.

    Section attendue : [samgov_keywords]  /  keywords = ["ugv", ...]
    Fallback silencieux sur _SAMGOV_KW_FALLBACK si :
      - tomllib/tomli absent (Python < 3.11 sans pip install tomli)
      - config/keywords.toml absent
      - section [samgov_keywords] absente ou vide
      - erreur de parsing TOML
    """
    try:
        try:
            import tomllib          # Python 3.11+ stdlib
        except ImportError:
            try:
                import tomli as tomllib  # type: ignore  # pip install tomli
            except ImportError:
                log.debug(
                    "tomllib/tomli absent — keywords SAM.gov depuis liste interne. "
                    "Python 3.11+ ou pip install tomli pour activer le chargement TOML."
                )
                return _SAMGOV_KW_FALLBACK

        toml_path = _CONFIG_DIR / "keywords.toml"
        if not toml_path.exists():
            log.debug(
                f"config/keywords.toml absent ({toml_path}) — "
                "keywords SAM.gov depuis liste interne"
            )
            return _SAMGOV_KW_FALLBACK

        with open(toml_path, "rb") as fh:
            data = tomllib.load(fh)

        kw = data.get("samgov_keywords", {}).get("keywords")
        if isinstance(kw, list) and kw:
            log.info(
                f"SAM.GOV Keywords chargés depuis {toml_path.name} "
                f"({len(kw)} entrées) — section [samgov_keywords]"
            )
            return kw

        log.debug(
            "config/keywords.toml : section [samgov_keywords] absente ou vide "
            "— fallback liste interne"
        )
        return _SAMGOV_KW_FALLBACK

    except Exception as exc:
        log.warning(
            f"SG-43-FIX2 : chargement keywords.toml échoué ({exc}) "
            "— fallback liste interne"
        )
        return _SAMGOV_KW_FALLBACK


# Initialisation au chargement du module
SAMGOV_KW: list[str] = _load_samgov_keywords()

# CPV défense TED EU — SG-41-FIX3 : 35700000 et 38820000 supprimés
# (codes inexistants dans la nomenclature CPV EU 2008)
TED_CPV_CODES: list[str] = [
    "35000000",  # Équipements de sécurité, défense, armements
    "35100000",  # Matériels et fournitures de survie et de sécurité
    "35120000",  # Systèmes et dispositifs de surveillance et de sécurité
    "35612000",  # Véhicules blindés de combat sans équipage
    "35613000",  # Drones militaires
]

# =============================================================================
# HELPERS
# =============================================================================

def _since_date(daysback: int) -> str:
    """Format mm/dd/yyyy pour SAM.gov. SG-41-FIX4 : timezone.utc."""
    return (datetime.now(timezone.utc) - timedelta(days=daysback)).strftime("%m/%d/%Y")


def _since_date_iso(daysback: int) -> str:
    """Format YYYY-MM-DD pour TED EU et BOAMP. SG-41-FIX4 : timezone.utc."""
    return (datetime.now(timezone.utc) - timedelta(days=daysback)).strftime("%Y-%m-%d")


def _safe_amount(val: object) -> float:
    """Convertit une valeur montant en float, 0.0 si invalide."""
    try:
        return float(
            str(val).replace(",", "").replace("$", "").replace("€", "").strip()
        )
    except (ValueError, TypeError):
        return 0.0


def _truncate(text: Optional[str], length: int = 300) -> str:
    """Tronque proprement à length caractères. Retourne '' si None."""
    if not text:
        return ""
    return str(text)[:length]


def _article_hash(titre: str, url: str) -> str:
    """
    SG-43-FIX3 : Hash SHA-256[:20] défini localement.

    Algorithme identique à scraper_rss.article_hash() (SCR-41-FIX2) :
    séparateur \\x00 pour éliminer les collisions théoriques titre+url.
    Déclaré localement pour éviter la dépendance implicite à scraper_rss.py.
    """
    return hashlib.sha256(f"{titre}{url}".encode()).hexdigest()[:20]


def _make_article(
    titre:     str,
    url:       str,
    date:      str,
    source:    str,
    resume:    str,
    art_score: float = 8.5,   # SG-43-FIX1 : renommé depuis artscore
    montant:   str   = "N/A",
) -> dict:
    """
    Construit un article au format pipeline SENTINEL unifié.

    Compatible avec :
      - scraper_rss.format_for_claude()  → lit "art_score" (float) + "score" (label)
      - sentinel_main.py                 → tri par -a["art_score"]
      - SentinelDB.saveseen()            → hash via _article_hash(titre, url)
      - telegram_scraper.py TG-R20       → même convention art_score / score

    SG-43-FIX1 BUG CRITIQUE corrigé :
      v3.42 retournait "artscore" (sans underscore) → non lu par le pipeline
      → score implicite 0.0 → contrats relégués en bas de liste → invisibles
      à Claude. Correction : "art_score" (avec underscore) aligné sur le reste
      du pipeline (scraper_rss.py, github_scraper.py, telegram_scraper.py).

    Champs retournés (format pipeline standard) :
      titre     str   — titre tronqué à 120 chars
      url       str   — URL complète de l'opportunité
      date      str   — date de publication (ISO ou format source)
      source    str   — nom de la source ("SAM.gov DoD", "TED EU", "BOAMP FR")
      score     str   — label fiabilité "A" (source institutionnelle officielle)
      resume    str   — résumé tronqué à 300 chars
      art_score float — score thématique défense [1.0, 10.0]
      montant   str   — montant formaté ("$45.0M USD", "€12.3M", "N/A")
      crossref  int   — indicateur de croisement (1 = signal contractuel confirmé)
    """
    return {
        "titre":     _truncate(titre, 120),
        "url":       url,
        "date":      date,
        "source":    source,
        "score":     "A",                                              # label fiabilité
        "resume":    _truncate(resume, 300),
        "art_score": round(min(10.0, max(1.0, art_score)), 1),        # SG-43-FIX1
        "montant":   montant,
        "crossref":  1,
    }


# =============================================================================
# SAM.GOV — APPELS D'OFFRES DoD
# =============================================================================

def fetch_sam_gov_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les appels d'offres DoD des N derniers jours via SAM.gov API v2.

    FIX-SAM1   : deptname="DEPT OF DEFENSE" + ptype="o,p,k"
    FIX-SAM4   : noticeId (champ v2 correct)
    NEW-SG1    : MIN_AMOUNT configurable via ENV
    SG-41-FIX5 : ThreadPoolExecutor(max_workers=8) — N keywords en parallèle
    SG-42-FIX1 : @retry tenacity sur _fetch_kw — résistance aux 504 SAM.gov
    SG-43-FIX1 : art_score (avec underscore) dans _make_article()
    """
    if not SAM_GOV_KEY:
        log.warning("SAM.GOV SAM_GOV_API_KEY manquant — module ignoré")
        return []

    since = _since_date(daysback)
    log.info(
        f"SAM.GOV démarrage collecte : {len(SAMGOV_KW)} keywords | "
        f"fenêtre {daysback}j ({since}) | MIN=${MIN_AMOUNT / 1e6:.0f}M USD"
    )

    # SG-42-FIX1 : @retry tenacity sur le worker.
    # 3 tentatives max, backoff 2s → 4s → 10s (cap).
    # reraise=False : keyword en échec retourne [] sans crasher le pool.
    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=False,
    )
    def _fetch_kw(kw: str) -> list[dict]:
        """Worker : requête SAM.gov pour un keyword unique, avec retry."""
        resp = requests.get(
            SAM_GOV_API,
            headers=_HEADERS,
            timeout=_TIMEOUT,
            params={
                "api_key":    SAM_GOV_KEY,
                "keyword":    kw,
                "postedFrom": since,
                "limit":      10,
                "deptname":   "DEPT OF DEFENSE",  # FIX-SAM1
                "ptype":      "o,p,k",             # FIX-SAM1
            },
        )
        resp.raise_for_status()
        items: list[dict] = []

        for opp in resp.json().get("opportunitiesData", []):
            nid = opp.get("noticeId", "")
            if not nid:
                continue

            raw_amount = _safe_amount(
                (opp.get("award") or {}).get("amount") or 0
            )
            # SG-41-FIX8 : 0.0 (champ absent / pre-award) passe intentionnellement
            if 0 < raw_amount < MIN_AMOUNT:
                continue

            montant = (
                f"${raw_amount / 1e6:.1f}M USD" if raw_amount >= 1e6
                else f"${raw_amount:,.0f} USD"   if raw_amount > 0
                else "N/A"
            )
            items.append(_make_article(
                titre     = f"CONTRAT DoD — {opp.get('title', '')[:100]}",
                url       = f"https://sam.gov/opp/{nid}/view",
                date      = opp.get("postedDate", "N/A"),
                source    = "SAM.gov DoD",
                resume    = _truncate(opp.get("description", ""), 300),
                art_score = 8.5,                                       # SG-43-FIX1
                montant   = montant,
            ))
        return items

    def _fetch_kw_safe(kw: str) -> list[dict]:
        """
        Wrapper sécurisé autour du worker retryable.
        tenacity avec reraise=False retourne None si toutes les tentatives
        échouent — on normalise en [] pour éviter TypeError dans extend().
        """
        try:
            result = _fetch_kw(kw)
            return result if result is not None else []
        except Exception as exc:
            log.warning(
                f"SAM.GOV Échec définitif keyword={kw!r} après retries : {exc}"
            )
            return []

    # SG-41-FIX5 : collecte parallèle — 8 workers pour N keywords
    raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for batch in executor.map(_fetch_kw_safe, SAMGOV_KW):
            raw.extend(batch)

    # Déduplication par URL après collecte parallèle (thread-safe, FIX-SAM3)
    seen_urls: set[str] = set()
    results:   list[dict] = []
    for art in raw:
        if art["url"] not in seen_urls:
            seen_urls.add(art["url"])
            results.append(art)

    # SG-43-FIX1 : tri par art_score (était -a["artscore"])
    results.sort(key=lambda a: -a["art_score"])
    log.info(
        f"SAM.GOV {len(results)} contrats DoD collectés "
        f"(MIN=${MIN_AMOUNT / 1e6:.0f}M USD, fenêtre {daysback}j, "
        f"{len(SAMGOV_KW)} keywords)"
    )
    return results


# =============================================================================
# TED EU — APPELS D'OFFRES DÉFENSE EUROPÉENS
# =============================================================================

def fetch_ted_eu_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les AO défense UE via TED API v3 (POST JSON).

    FIX-SAM2   : endpoint POST /v3/notices/search + CPV défense corrects
    SG-41-FIX3 : CPV codes inexistants supprimés (35700000, 38820000)
    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué (pivot EUR/USD configurable)
    SG-43-FIX1 : art_score dans _make_article()
    Fallback automatique vers API v2 si v3 indisponible (4xx/réseau).
    """
    since    = _since_date_iso(daysback)
    results: list[dict] = []
    seen_ids: set[str]  = set()

    payload = {
        "query": (
            "unmanned vehicle OR autonomous robot OR drone defense "
            "OR loitering munition OR UGV OR USV OR UUV"
        ),
        "cpvCodes":            TED_CPV_CODES,
        "publicationDateFrom": since,
        "fields": [
            "title", "publicationDate", "estimatedValue",
            "links", "contractorName", "cpvCodes",
        ],
        "limit": 25,
        "page":  1,
    }

    try:
        resp = requests.post(
            TED_API,
            headers={**_HEADERS, "Content-Type": "application/json"},
            json=payload,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()

        for notice in resp.json().get("results", []):
            nid = notice.get("id") or notice.get("noticeId", "")
            if nid and nid in seen_ids:
                continue
            if nid:
                seen_ids.add(nid)

            links   = notice.get("links") or []
            ted_url = (
                links[0].get("href", "https://ted.europa.eu")
                if links else "https://ted.europa.eu"
            )

            est_val = _safe_amount(notice.get("estimatedValue"))

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR — 0.0 passe (SG-41-FIX8)
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            montant = (
                f"€{est_val / 1e6:.1f}M" if est_val >= 1e6
                else f"€{est_val:,.0f}"  if est_val > 0
                else "N/A"
            )

            # Titre multilingue : TED v3 retourne parfois un dict {lang: str}
            title_raw = notice.get("title") or ""
            if isinstance(title_raw, dict):
                title_str = (
                    title_raw.get("en")
                    or title_raw.get("fr")
                    or next(iter(title_raw.values()), "N/A")
                )
            else:
                title_str = str(title_raw)

            cpv_list    = notice.get("cpvCodes") or []
            contractor  = notice.get("contractorName") or "N/A"

            results.append(_make_article(
                titre     = f"AO DÉFENSE EU — {title_str[:100]}",
                url       = ted_url,
                date      = notice.get("publicationDate", "N/A"),
                source    = "TED EU Defence",
                resume    = _truncate(
                    f"Valeur estimée : {montant}. "
                    f"CPV : {', '.join(cpv_list[:3])}. "
                    f"Contractant : {contractor}.",
                    300,
                ),
                art_score = 8.0,                                       # SG-43-FIX1
                montant   = montant,
            ))

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        log.warning(
            f"TED API v3 HTTP {status} — bascule fallback v2 : {exc}"
        )
        return _fetch_ted_eu_v2_fallback(daysback)
    except requests.RequestException as exc:
        log.warning(f"TED API v3 erreur réseau — bascule fallback v2 : {exc}")
        return _fetch_ted_eu_v2_fallback(daysback)
    except (KeyError, ValueError, TypeError) as exc:
        log.error(f"TED EU v3 parsing JSON : {exc}")

    # SG-43-FIX1 : tri par art_score
    results.sort(key=lambda a: -a["art_score"])
    log.info(
        f"TED EU {len(results)} AO défense collectés "
        f"(MIN=€{MIN_AMOUNT_EUR / 1e6:.2f}M, fenêtre {daysback}j)"
    )
    return results


def _fetch_ted_eu_v2_fallback(daysback: int = 2) -> list[dict]:
    """
    Fallback TED API v2 si v3 indisponible.

    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué ici aussi (cohérence).
    SG-43-FIX1 : art_score dans _make_article().
    """
    since   = _since_date_iso(daysback)
    results: list[dict] = []

    try:
        resp = requests.get(
            TED_V2_API,
            headers=_HEADERS,
            timeout=_TIMEOUT,
            params={
                "q":        f"cpv:35000000 AND publicationDate:{since}",
                "page":     1,
                "pageSize": 25,
                "fields":   "title,links,values,publicationDate",
            },
        )
        resp.raise_for_status()

        for notice in resp.json().get("notices", resp.json().get("results", [])):
            title_raw = notice.get("title") or [{}]
            title_str = (
                title_raw[0].get("value", "N/A")
                if isinstance(title_raw, list)
                else str(title_raw)
            )
            links   = notice.get("links") or [{"href": "https://ted.europa.eu"}]
            ted_url = links[0].get("href", "https://ted.europa.eu")
            est_val = _safe_amount(
                (notice.get("values") or {}).get("estimatedValue")
            )

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR sur fallback aussi
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            montant = (
                f"€{est_val / 1e6:.1f}M" if est_val >= 1e6
                else f"€{est_val:,.0f}"  if est_val > 0
                else "N/A"
            )
            results.append(_make_article(
                titre     = f"AO DÉFENSE EU — {title_str[:100]}",
                url       = ted_url,
                date      = notice.get("publicationDate", "N/A"),
                source    = "TED EU Defence",
                resume    = f"Valeur estimée : {montant}.",
                art_score = 7.5,                                       # SG-43-FIX1
                montant   = montant,
            ))

    except requests.RequestException as exc:
        log.warning(f"TED EU v2 fallback erreur : {exc}")

    log.info(f"TED EU v2 fallback : {len(results)} résultats")
    return results


# =============================================================================
# BOAMP FR — APPELS D'OFFRES DÉFENSE FRANCE
# =============================================================================

def scrape_boamp_fr(daysback: int = 2) -> list[dict]:
    """
    Collecte les AO défense français via l'API BOAMP (officielle, gratuite).

    E4-OSINT2  : BOAMP.fr — API publique Légifrance
    O5-FIX     : BOAMP_URL déclaré au niveau module
    SG-41-FIX1 : un seul appel depuis run_all_procurement()
    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué
    SG-43-FIX1 : art_score dans _make_article()
    SG-43-FIX5 : tous les accès dict protégés par .get() avec valeur défaut.
    """
    date_min = _since_date_iso(daysback)
    results:  list[dict] = []
    seen_ids: set[str]   = set()

    try:
        resp = requests.get(
            BOAMP_URL,
            headers=_HEADERS,
            timeout=_TIMEOUT,
            params={
                "q": (
                    "robotique defense OR UGV OR drone militaire "
                    "OR armement autonome OR système autonome "
                    "OR véhicule sans pilote OR surveillance"
                ),
                "dateparutionmin": date_min,
                "rows":            25,
            },
        )
        resp.raise_for_status()

        data = resp.json()

        # Support deux formats de réponse BOAMP (Elasticsearch et API directe)
        hits_raw = (
            data.get("hits", {}).get("hits", [])   # format Elasticsearch
            or data.get("results", [])              # format API directe
            or data.get("docs",    [])              # format Solr
        )

        for hit in hits_raw:
            # SG-43-FIX5 : protection complète de tous les accès dict
            src = hit.get("_source", hit)   # ES = _source, sinon l'objet directement

            ref    = str(src.get("idweb") or src.get("reference") or src.get("id") or "")
            if not ref or ref in seen_ids:
                continue
            seen_ids.add(ref)

            objet      = src.get("objet",     "")           or ""
            date_pub   = src.get("dateparution", "N/A")     or "N/A"
            pouvoir    = src.get("pouvoir_adjudicateur", "") or ""
            montant_raw = src.get("montant_estime") or src.get("montantEstime") or 0
            est_val    = _safe_amount(montant_raw)

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR (0.0 passe — SG-41-FIX8)
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            montant_str = (
                f"€{est_val / 1e6:.1f}M" if est_val >= 1e6
                else f"€{est_val:,.0f}"  if est_val > 0
                else "N/A"
            )

            url_avis = (
                src.get("urlAvis")
                or (f"https://www.boamp.fr/avis/detail/{ref}" if ref else BOAMP_URL)
            )

            results.append(_make_article(
                titre     = f"BOAMP — {_truncate(objet, 100) or 'AO Défense FR'}",
                url       = url_avis,
                date      = date_pub,
                source    = "BOAMP FR",
                resume    = _truncate(
                    f"Acheteur : {pouvoir[:80] or 'N/A'}. "
                    f"Objet : {objet[:150] or 'N/A'}. "
                    f"Montant : {montant_str}.",
                    300,
                ),
                art_score = 7.5,                                       # SG-43-FIX1
                montant   = montant_str,
            ))

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        log.warning(f"BOAMP FR HTTP {status} : {exc}")
    except requests.RequestException as exc:
        log.warning(f"BOAMP FR erreur réseau : {exc}")
    except (KeyError, ValueError, TypeError) as exc:
        log.warning(f"BOAMP FR parsing JSON : {exc}")

    # SG-43-FIX1 : tri par art_score
    results.sort(key=lambda a: -a["art_score"])
    log.info(
        f"BOAMP FR {len(results)} AO défense collectés "
        f"(MIN=€{MIN_AMOUNT_EUR / 1e6:.2f}M, fenêtre {daysback}j)"
    )
    return results


# =============================================================================
# PERSISTANCE SENTINELDB (NEW-SG2)
# =============================================================================

def _save_to_db(articles: list[dict]) -> None:
    """
    Persiste les hashes des articles collectés dans SentinelDB.seenhashes.
    Évite les doublons entre runs consécutifs.

    SG-41-FIX6  : set transmis proprement à SentinelDB.saveseen()
    SG-43-FIX3  : _article_hash() local — indépendance vis-à-vis scraper_rss.py.
                  SHA-256[:20] avec séparateur \\x00 (aligné SCR-41-FIX2).
    Non bloquant : toute exception est loggée en debug, jamais reraisée.
    """
    if not articles:
        return
    try:
        from db_manager import SentinelDB  # type: ignore
        hashes = {_article_hash(a["titre"], a["url"]) for a in articles}
        SentinelDB.saveseen(hashes, source="samgov_scraper")
        log.info(f"DB {len(hashes)} hashes contrats persistés (SentinelDB)")
    except ImportError:
        log.debug("db_manager absent — hashes contrats non persistés (mode standalone)")
    except Exception as exc:
        log.debug(f"_save_to_db non bloquant : {exc}")


# =============================================================================
# POINT D'ENTRÉE UNIQUE (NEW-SG3)
# =============================================================================

def run_all_procurement(daysback: int = 2) -> list[dict]:
    """
    Lance les trois collecteurs DoD / TED EU / BOAMP FR en séquence.
    Retourne la liste combinée dédupliquée triée par art_score décroissant.

    Point d'entrée unique pour sentinel_main.py (NEW-SG3).

    SG-41-FIX1  : un seul appel scrape_boamp_fr() (suppression du doublon)
    SG-43-FIX1  : tri final par art_score (était -a["artscore"])
    NEW-SG4     : résumé de collecte logué en fin de run

    Usage dans sentinel_main.py :
        from samgov_scraper import run_all_procurement
        contracts = run_all_procurement(daysback=2)
        articles.extend(contracts)
    """
    log.info(
        f"=== SAMGOV_SCRAPER v{_VERSION} — Collecte contrats défense ==="
    )
    log.info(
        f"Fenêtre : {daysback}j | "
        f"MIN=${MIN_AMOUNT / 1e6:.0f}M USD | "
        f"MIN=€{MIN_AMOUNT_EUR / 1e6:.2f}M EUR | "
        f"Keywords SAM.gov : {len(SAMGOV_KW)}"
    )

    # ── SAM.gov DoD ───────────────────────────────────────────────────────────
    sam_articles: list[dict] = fetch_sam_gov_opportunities(daysback=daysback)

    # ── TED EU ────────────────────────────────────────────────────────────────
    ted_articles: list[dict] = fetch_ted_eu_opportunities(daysback=daysback)

    # ── BOAMP FR (SG-41-FIX1 : un seul appel, protégé) ───────────────────────
    boamp_articles: list[dict] = []
    try:
        boamp_articles = scrape_boamp_fr(daysback=daysback)
    except Exception as exc:
        log.warning(f"BOAMP FR : erreur non bloquante ignorée : {exc}")

    # ── Fusion + déduplication globale par URL ────────────────────────────────
    all_raw: list[dict] = sam_articles + ted_articles + boamp_articles
    seen_urls: set[str] = set()
    deduped:   list[dict] = []
    for art in all_raw:
        if art["url"] not in seen_urls:
            seen_urls.add(art["url"])
            deduped.append(art)

    # SG-43-FIX1 : tri par art_score (était -a.get("artscore", 5.0))
    deduped.sort(key=lambda a: -a["art_score"])

    # Persistance SentinelDB (NEW-SG2)
    _save_to_db(deduped)

    # NEW-SG4 : résumé de collecte
    log.info(
        f"SAMGOV_SCRAPER collecte terminée : "
        f"{len(deduped)} contrats uniques "
        f"({len(sam_articles)} DoD | "
        f"{len(ted_articles)} TED EU | "
        f"{len(boamp_articles)} BOAMP FR)"
    )
    return deduped


# =============================================================================
# CLI — diagnostic & smoke test (SG-43-FIX7 : argparse)
# =============================================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description=f"SENTINEL SAM.gov Scraper v{_VERSION} — Contrats défense DoD/TED/BOAMP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :
"
            f"  python samgov_scraper.py                  # collecte complète 2j
"
            f"  python samgov_scraper.py --days 7         # fenêtre 7 jours
"
            f"  python samgov_scraper.py --source sam     # SAM.gov uniquement
"
            f"  python samgov_scraper.py --dry-run        # config sans appel API
"
        ),
    )
    parser.add_argument(
        "--days", type=int, default=2,
        help="Fenêtre temporelle en jours (défaut : 2)",
    )
    parser.add_argument(
        "--source", choices=["all", "sam", "ted", "boamp"], default="all",
        help="Source à interroger (défaut : all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Affiche la configuration et un article exemple sans appeler les APIs",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info(f"[DRY-RUN] SENTINEL SAM.gov Scraper v{_VERSION}")
        log.info(f"  SAM_GOV_KEY       : {'✓ définie' if SAM_GOV_KEY else '✗ MANQUANTE (.env)'}")
        log.info(f"  MIN_AMOUNT_USD    : ${MIN_AMOUNT / 1e6:.1f}M")
        log.info(f"  MIN_AMOUNT_EUR    : €{MIN_AMOUNT_EUR / 1e6:.2f}M")
        log.info(f"  EUR_TO_USD        : {EUR_TO_USD}")
        log.info(f"  Timeout HTTP      : {_TIMEOUT}s (SENTINEL_SCRAPER_TIMEOUT)")
        log.info(f"  Fenêtre           : {args.days} jours")
        log.info(f"  Keywords SAM.gov  : {len(SAMGOV_KW)} entrées")
        log.info(f"  Config TOML       : {_CONFIG_DIR / 'keywords.toml'}")
        log.info(f"  CPV TED défense   : {len(TED_CPV_CODES)} codes")
        log.info("")
        log.info("[DRY-RUN] Exemple d'article généré (format pipeline) :")
        sample = _make_article(
            titre     = "CONTRAT DoD — Autonomous Ground Vehicle System Integration",
            url       = "https://sam.gov/opp/DRYRUN-TEST-001/view",
            date      = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            source    = "SAM.gov DoD",
            resume    = "Contract for development and integration of autonomous UGV systems.",
            art_score = 8.5,
            montant   = "$45.0M USD",
        )
        for key, val in sample.items():
            log.info(f"    {key:12s} : {val!r}")
        log.info("")
        log.info("[DRY-RUN] Vérification clé art_score (BUG-1 corrigé) :")
        assert "art_score" in sample,     "ERREUR : champ art_score manquant !"
        assert "artscore"  not in sample, "ERREUR : champ artscore (sans underscore) présent !"
        assert sample["score"] == "A",    "ERREUR : label fiabilité 'A' manquant !"
        log.info("    ✓ art_score présent (clé correcte)")
        log.info("    ✓ artscore absent   (ancienne clé cassée supprimée)")
        log.info("    ✓ score='A'         (label fiabilité correct)")
        sys.exit(0)

    # ── Exécution réelle ──────────────────────────────────────────────────────
    if args.source == "sam":
        articles = fetch_sam_gov_opportunities(daysback=args.days)
    elif args.source == "ted":
        articles = fetch_ted_eu_opportunities(daysback=args.days)
    elif args.source == "boamp":
        articles = scrape_boamp_fr(daysback=args.days)
    else:
        articles = run_all_procurement(daysback=args.days)

    if not articles:
        log.warning("Aucun contrat collecté — vérifier la config .env (SAM_GOV_API_KEY)")
        sys.exit(1)

    log.info(f"
{'=' * 70}")
    log.info(f"TOP 20 contrats (sur {len(articles)}) :")
    for idx, art in enumerate(articles[:20], 1):
        log.info(
            f"  [{idx:02d}] [{art['art_score']:4.1f}] [{art['score']}] "
            f"{art['source']:<16} | {art['montant']:<15} | "
            f"{art['titre'][:55]}"
        )
    if len(articles) > 20:
        log.info(f"  … et {len(articles) - 20} autres résultats")
    log.info(f"{'=' * 70}")
    log.info(f"TOTAL : {len(articles)} contrats/AO collectés")
