#!/usr/bin/env python3
# samgov_scraper.py — SENTINEL v3.42 — Appels d'offres défense DoD/TED/BOAMP
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 (originales) :
#   FIX-SAM1    deptname="DEPT OF DEFENSE" + ptype="o,p,k"
#   FIX-SAM2    TED API v3 (POST JSON) + CPV défense
#   FIX-SAM3    seenids set global par run → zéro doublon multi-keyword
#   FIX-SAM4    noticeId + artscore=8.5 signal contractuel
#   FIX-OBS1    Logging structuré via logger nommé, zéro print()
#   FIX-M4      Tri par artscore en sortie
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
#   SG-41-FIX5  ThreadPoolExecutor(max_workers=8) pour les 50 requêtes SAM.gov
#   SG-41-FIX6  _save_to_db() : list(hashes) au lieu de set
#   SG-41-FIX7  CLI : print() → logger (cohérence FIX-OBS1)
#   SG-41-FIX8  MIN_AMOUNT=0 documenté (pre-awards sans valeur passent)
#
# Corrections v3.42 :
#   SG-42-FIX1  Retry tenacity sur _fetch_kw() — SAM.gov est connu pour ses
#               erreurs 504 Gateway Timeout aléatoires. 3 tentatives, backoff
#               exponentiel 2s→10s. tenacity est déjà dans requirements.txt.
#               reraise=False : un keyword en échec retourne [] sans crasher
#               le ThreadPoolExecutor.
#   SG-42-FIX2  Filtre MIN_AMOUNT étendu à TED EU et BOAMP FR avec conversion
#               EUR/USD pivot. Auparavant MIN_AMOUNT n'était appliqué qu'à
#               SAM.gov — TED et BOAMP ne filtraient pas par montant.
#               Taux configurable via ENV SENTINEL_EUR_TO_USD (défaut : 1.08).
#               Les contrats sans montant renseigné (0.0) passent toujours
#               intentionnellement (pre-awards / champ absent).
# ─────────────────────────────────────────────────────────────────────────────
# Dépendances : requests, tenacity, python-dotenv (opt.)
# Variables d'environnement :
#   SAM_GOV_API_KEY            Clé API SAM.gov (gratuite sur sam.gov/developers)
#   SENTINEL_MIN_CONTRACT_USD  Seuil filtrage montant USD (défaut : 1 000 000)
#   SENTINEL_EUR_TO_USD        Taux de change EUR→USD pivot (défaut : 1.08)
#                              Utilisé pour convertir MIN_AMOUNT en EUR pour
#                              le filtrage TED EU et BOAMP FR
#   Note : montant=0 (champ absent / pre-award) passe toujours (SG-41-FIX8)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from tenacity import (  # SG-42-FIX1
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Logger structuré (FIX-OBS1) ──────────────────────────────────────────────
log = logging.getLogger("sentinel.samgov")

# ── User-Agent dédié (SG-41-FIX2) ────────────────────────────────────────────
_VERSION = "3.42"
_HEADERS = {
    "User-Agent": f"SENTINEL/{_VERSION} (defence-osint-monitor)"
}

# ── Seuil montant minimum (NEW-SG1 / SG-42-FIX2) ─────────────────────────────
# MIN_AMOUNT est en USD (référence SAM.gov).
# MIN_AMOUNT_EUR est le pivot pour TED EU et BOAMP FR.
# SG-41-FIX8 : les contrats avec montant=0 (champ absent / pre-award)
# passent intentionnellement — seuls les montants >0 ET <seuil sont exclus.
MIN_AMOUNT     = float(os.environ.get("SENTINEL_MIN_CONTRACT_USD", "1000000"))
EUR_TO_USD     = float(os.environ.get("SENTINEL_EUR_TO_USD", "1.08"))  # SG-42-FIX2
MIN_AMOUNT_EUR = MIN_AMOUNT / EUR_TO_USD  # ~926k€ si MIN=1M USD

# ── Clé API SAM.gov ───────────────────────────────────────────────────────────
SAM_GOV_KEY = os.environ.get("SAM_GOV_API_KEY", "")

# ── API endpoints ─────────────────────────────────────────────────────────────
SAM_GOV_API = "https://api.sam.gov/opportunities/v2/search"
TED_API     = "https://api.ted.europa.eu/v3/notices/search"
BOAMP_URL   = "https://www.boamp.fr/api/search"  # O5-FIX


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORDS (E4-OSINT3 — liste complète v3.40)
# ─────────────────────────────────────────────────────────────────────────────

SAMGOV_KW = [
    # Systèmes terrestres
    "unmanned ground vehicle",
    "unmanned surface vessel",
    "autonomous underwater vehicle",
    "drone defense",
    "loitering munition",
    "military robot",
    "UGV", "USV", "UUV",
    "autonomous weapon system",
    # Contre-systèmes
    "counter-UAS",
    "C-UAS anti-drone",
    # Technologies transverses
    "directed energy",
    "hypersonic",
    "autonomous combat",
    # Programmes CCA / AUKUS
    "collaborative combat aircraft",
    "loyal wingman",
    "attritable uav",
    "ai-enabled warfare",
    "electronic warfare",
    # UGV programmes OTAN critiques
    "mission master",
    "black knight ugv",
    "type x ugv",
    "rheinmetall ugv",
    "titan ugv",
    "camel ugv",
    "rex mk2",
    # USV/UUV programmes critiques
    "sea hunter",
    "ghost fleet overlord",
    "orca uuv",
    "manta ray darpa",
    "ghost shark uuv",
    "mine countermeasure autonomous",
    "extra large uuv",
    "xluuv",
    # Doctrines / concepts
    "manned unmanned teaming",
    "MUM-T",
    "lethal autonomous weapon",
    "mosaic warfare",
    "multi-domain operations",
    # IA militaire 2025-2026
    "machine learning targeting",
    "autonomous decision-making",
    "large language model intelligence",
    "golden horde darpa",
    # Acteurs industriels disruptifs
    "anduril",
    "shield ai",
]

# CPV défense TED EU — SG-41-FIX3 : 35700000 et 38820000 supprimés
# (n'existent pas dans la nomenclature CPV EU 2008)
TED_CPV_CODES = [
    "35000000",  # Équipements de sécurité, défense, armements
    "35100000",  # Matériels et fournitures de survie et de sécurité
    "35120000",  # Systèmes et dispositifs de surveillance et de sécurité
    "35612000",  # Véhicules blindés de combat sans équipage
    "35613000",  # Drones militaires
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _since_date(daysback: int) -> str:
    """Format mm/dd/yyyy pour SAM.gov. SG-41-FIX4 : timezone.utc."""
    return (datetime.now(timezone.utc) - timedelta(days=daysback)).strftime("%m/%d/%Y")


def _since_date_iso(daysback: int) -> str:
    """Format YYYY-MM-DD pour TED EU et BOAMP. SG-41-FIX4 : timezone.utc."""
    return (datetime.now(timezone.utc) - timedelta(days=daysback)).strftime("%Y-%m-%d")


def _safe_amount(val) -> float:
    """Convertit une valeur montant en float, 0.0 si invalide."""
    try:
        return float(str(val).replace(",", "").replace("$", "").replace("€", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _truncate(text: Optional[str], length: int = 300) -> str:
    """Tronque proprement à length caractères, '' si None."""
    if not text:
        return ""
    return str(text)[:length]


def _make_article(
    titre: str,
    url: str,
    date: str,
    source: str,
    resume: str,
    artscore: float = 8.5,
    montant: str = "N/A",
) -> dict:
    """
    Construit un article au format pipeline SENTINEL.
    Compatible scraper_rss.py + format_for_claude() + SentinelDB.
    artscore=8.5 par défaut : signal contractuel = haute priorité (FIX-SAM4).
    """
    return {
        "titre":    _truncate(titre, 120),
        "url":      url,
        "date":     date,
        "source":   source,
        "score":    "A",
        "resume":   _truncate(resume, 300),
        "artscore": round(min(10.0, max(1.0, artscore)), 1),
        "montant":  montant,
        "crossref": 1,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SAM.GOV — APPELS D'OFFRES DoD
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sam_gov_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les appels d'offres DoD des N derniers jours via SAM.gov API v2.

    FIX-SAM1   : deptname="DEPT OF DEFENSE" + ptype="o,p,k"
    FIX-SAM4   : noticeId (champ v2 correct) + artscore=8.5
    NEW-SG1    : MIN_AMOUNT configurable via ENV
    SG-41-FIX5 : ThreadPoolExecutor(max_workers=8) — 50 keywords en parallèle
    SG-42-FIX1 : @retry tenacity sur _fetch_kw — résistance aux 504 SAM.gov
    """
    if not SAM_GOV_KEY:
        log.warning("SAM.GOV SAM_GOV_API_KEY manquant — module ignoré")
        return []

    since = _since_date(daysback)

    # SG-42-FIX1 : décorateur retry sur la fonction worker.
    # - 3 tentatives max par keyword
    # - Backoff exponentiel : 2s → 4s → 10s (cap)
    # - reraise=False : un keyword en échec retourne [] sans crasher le pool
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
            timeout=15,
            params={
                "api_key":    SAM_GOV_KEY,
                "keyword":    kw,
                "postedFrom": since,
                "limit":      10,
                "deptname":   "DEPT OF DEFENSE",  # FIX-SAM1
                "ptype":      "o,p,k",            # FIX-SAM1
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
                titre    = f"CONTRAT DoD — {opp.get('title', '')[:100]}",
                url      = f"https://sam.gov/opp/{nid}/view",
                date     = opp.get("postedDate", "N/A"),
                source   = "SAM.gov DoD",
                resume   = _truncate(opp.get("description", ""), 300),
                artscore = 8.5,
                montant  = montant,
            ))
        return items

    def _fetch_kw_safe(kw: str) -> list[dict]:
        """
        Wrapper sécurisé autour du worker retryable.
        tenacity avec reraise=False retourne None si toutes les tentatives
        échouent — on normalise en [] pour éviter un TypeError dans extend().
        """
        try:
            result = _fetch_kw(kw)
            return result if result is not None else []
        except Exception as e:
            log.warning(f"SAM.GOV Échec définitif keyword={kw!r} après retries : {e}")
            return []

    # SG-41-FIX5 : collecte parallèle — 8 workers pour 50 keywords
    raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for batch in ex.map(_fetch_kw_safe, SAMGOV_KW):
            raw.extend(batch)

    # Déduplication par URL après collecte parallèle (thread-safe)
    seen_urls: set[str] = set()
    results: list[dict] = []
    for art in raw:
        if art["url"] not in seen_urls:
            seen_urls.add(art["url"])
            results.append(art)

    results.sort(key=lambda a: -a["artscore"])  # FIX-M4
    log.info(
        f"SAM.GOV {len(results)} contrats DoD collectés "
        f"(MIN=${MIN_AMOUNT / 1e6:.0f}M USD, fenêtre {daysback}j)"
    )
    return results


# ─────────────────────────────────────────────────────────────────────────────
# TED EU — APPELS D'OFFRES DÉFENSE EUROPÉENS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ted_eu_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les AO défense UE via TED API v3 (POST JSON).

    FIX-SAM2   : endpoint POST /v3/notices/search + CPV défense corrects
    SG-41-FIX3 : CPV codes inexistants supprimés (35700000, 38820000)
    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué (pivot EUR/USD configurable)
    Fallback automatique vers API v2 si v3 indisponible.
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
            timeout=15,
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

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR — même logique que SAM.gov
            # 0.0 (champ absent) passe intentionnellement (SG-41-FIX8)
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            montant = (
                f"€{est_val / 1e6:.1f}M" if est_val >= 1e6
                else f"€{est_val:,.0f}"  if est_val > 0
                else "N/A"
            )

            # Titre multilingue (TED retourne parfois un dict)
            title_raw = notice.get("title") or ""
            if isinstance(title_raw, dict):
                title_str = (
                    title_raw.get("en")
                    or title_raw.get("fr")
                    or next(iter(title_raw.values()), "N/A")
                )
            else:
                title_str = str(title_raw)

            results.append(_make_article(
                titre    = f"APPEL OFFRE EU — {title_str[:100]}",
                url      = ted_url,
                date     = notice.get("publicationDate", "N/A"),
                source   = "TED EU Defence",
                resume   = (
                    f"Valeur estimée : {montant}. "
                    f"CPV : {', '.join(notice.get('cpvCodes', [])[:3])}."
                ),
                artscore = 8.0,
                montant  = montant,
            ))

    except requests.RequestException as e:
        log.warning(f"TED EU v3 Erreur : {e} — bascule fallback v2")
        return _fetch_ted_eu_v2_fallback(daysback)

    results.sort(key=lambda a: -a["artscore"])  # FIX-M4
    log.info(
        f"TED EU {len(results)} AO défense collectés "
        f"(MIN=€{MIN_AMOUNT_EUR / 1e6:.2f}M, fenêtre {daysback}j)"
    )
    return results


def _fetch_ted_eu_v2_fallback(daysback: int = 2) -> list[dict]:
    """
    Fallback TED API v2 si v3 indisponible.
    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué ici aussi.
    """
    TED_V2  = "https://ted.europa.eu/api/v2.0/notices/search"
    since   = _since_date_iso(daysback)
    results: list[dict] = []

    try:
        resp = requests.get(
            TED_V2,
            headers=_HEADERS,
            timeout=15,
            params={
                "q":        f"cpv:35000000 AND publicationDate:{since}",
                "page":     1,
                "pageSize": 25,
                "fields":   "title,links,values,publicationDate",
            },
        )
        resp.raise_for_status()

        for n in resp.json().get("notices", resp.json().get("results", [])):
            title_raw = n.get("title") or [{}]
            title_str = (
                title_raw[0].get("value", "N/A")
                if isinstance(title_raw, list)
                else str(title_raw)
            )
            links   = n.get("links") or [{"href": "https://ted.europa.eu"}]
            ted_url = links[0].get("href", "https://ted.europa.eu")
            est_val = _safe_amount((n.get("values") or {}).get("estimatedValue"))

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR sur le fallback aussi
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            montant = f"€{est_val / 1e6:.1f}M" if est_val >= 1e6 else "N/A"

            results.append(_make_article(
                titre    = f"APPEL OFFRE EU — {title_str[:100]}",
                url      = ted_url,
                date     = n.get("publicationDate", "N/A"),
                source   = "TED EU Defence",
                resume   = f"Valeur estimée : {montant}.",
                artscore = 7.5,
                montant  = montant,
            ))

    except requests.RequestException as e:
        log.warning(f"TED EU v2 fallback Erreur : {e}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# BOAMP.FR — APPELS D'OFFRES DÉFENSE FRANCE
# ─────────────────────────────────────────────────────────────────────────────

def scrape_boamp_fr(daysback: int = 2) -> list[dict]:
    """
    Collecte les AO défense FR via l'API BOAMP (gratuite, officielle).

    E4-OSINT2  : BOAMP.fr — API publique Légifrance, AO défense robotique
    O5-FIX     : BOAMP_URL déclaré au niveau module avant cet appel
    SG-42-FIX2 : filtre MIN_AMOUNT_EUR appliqué (cohérence avec TED/SAM.gov)
    """
    date_min = _since_date_iso(daysback)
    results: list[dict] = []

    try:
        resp = requests.get(
            BOAMP_URL,
            headers=_HEADERS,
            timeout=15,
            params={
                "q": (
                    "robotique defense OR UGV OR drone militaire "
                    "OR armement autonome OR système autonome"
                ),
                "dateparutionmin": date_min,
                "rows":            25,
            },
        )
        resp.raise_for_status()

        hits = resp.json().get("hits", {}).get("hits", [])
        for h in hits:
            src     = h.get("_source", {})
            objet   = src.get("objet", "N/A")
            date_p  = src.get("dateparution", "N/A")
            ref     = src.get("idweb") or src.get("reference", "")
            url     = (
                f"https://www.boamp.fr/avis/detail/{ref}"
                if ref else "https://www.boamp.fr"
            )
            montant_raw = src.get("montant_estime")
            est_val     = _safe_amount(montant_raw)
            montant_str = str(montant_raw) if montant_raw else "N/A"
            pouvoir     = src.get("pouvoir_adjudicateur", "")

            # SG-42-FIX2 : filtre MIN_AMOUNT_EUR — cohérence avec TED EU
            # 0.0 (champ absent) passe intentionnellement (SG-41-FIX8)
            if 0 < est_val < MIN_AMOUNT_EUR:
                continue

            results.append(_make_article(
                titre    = f"BOAMP — {objet[:100]}",
                url      = url,
                date     = date_p,
                source   = "BOAMP FR",
                resume   = f"Acheteur : {pouvoir[:80]}. Montant : {montant_str}.",
                artscore = 7.5,
                montant  = montant_str,
            ))

    except requests.RequestException as e:
        log.warning(f"BOAMP Erreur : {e}")

    results.sort(key=lambda a: -a["artscore"])  # FIX-M4
    log.info(
        f"BOAMP {len(results)} AO défense FR collectés "
        f"(MIN=€{MIN_AMOUNT_EUR / 1e6:.2f}M, fenêtre {daysback}j)"
    )
    return results


# ─────────────────────────────────────────────────────────────────────────────
# SAVE TO DB — DÉDUPLICATION INTER-RUNS (NEW-SG2)
# ─────────────────────────────────────────────────────────────────────────────

def _save_to_db(articles: list[dict]) -> None:
    """
    Enregistre les hash titre+url dans SentinelDB.seenhashes pour éviter
    les re-collectes lors des runs suivants.
    Non bloquant — ne lève jamais d'exception (NEW-SG2).
    SG-41-FIX6 : list(hashes) au lieu de set pour cohérence avec SentinelDB.
    """
    try:
        import hashlib
        from db_manager import SentinelDB

        hashes = [
            hashlib.sha256(
                (a["titre"] + a["url"]).encode()
            ).hexdigest()[:20]
            for a in articles
        ]
        SentinelDB.saveseen(hashes, source="procurement")
        log.debug(f"DB {len(hashes)} contrats enregistrés dans seenhashes")
    except Exception as e:
        log.debug(f"DB _save_to_db non bloquant : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE UNIQUE (NEW-SG3)
# ─────────────────────────────────────────────────────────────────────────────

def run_all_procurement(daysback: int = 2) -> list[dict]:
    """
    Lance les trois collecteurs DoD / TED EU / BOAMP FR en séquence.
    Retourne la liste combinée triée par artscore décroissant.

    Point d'entrée unique pour sentinel_main.py (NEW-SG3).

    Usage dans sentinel_main.py :
        from samgov_scraper import run_all_procurement
        contracts = run_all_procurement(daysback=2)
        articles.extend(contracts)
    """
    all_articles: list[dict] = []

    sam = fetch_sam_gov_opportunities(daysback=daysback)
    ted = fetch_ted_eu_opportunities(daysback=daysback)

    # SG-41-FIX1 : un seul appel protégé par try/except
    try:
        boamp = scrape_boamp_fr(daysback=daysback)
    except Exception as _boamp_err:
        boamp = []
        log.warning(f"BOAMP : erreur non bloquante : {_boamp_err}")

    all_articles.extend(sam)
    all_articles.extend(ted)
    all_articles.extend(boamp)

    # Tri global par artscore (FIX-M4)
    all_articles.sort(key=lambda a: -a.get("artscore", 5.0))

    # Enregistrement seenhashes inter-runs (NEW-SG2)
    _save_to_db(all_articles)

    # Résumé de collecte (NEW-SG4)
    log.info(
        f"PROCUREMENT TOTAL {len(all_articles)} contrats/AO "
        f"— DoD={len(sam)} TED_EU={len(ted)} BOAMP={len(boamp)} "
        f"(fenêtre {daysback}j | MIN=${MIN_AMOUNT / 1e6:.0f}M USD "
        f"/ €{MIN_AMOUNT_EUR / 1e6:.2f}M EUR)"
    )
    return all_articles


# ─────────────────────────────────────────────────────────────────────────────
# CLI — diagnostic & test
# SG-41-FIX7 : print() → logger (cohérence FIX-OBS1)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    daysback = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    log.info("── SENTINEL Procurement Scraper v3.42 ──────────────────────────")
    log.info(f"   Fenêtre      : {daysback} jours")
    log.info(f"   MIN USD      : ${MIN_AMOUNT / 1e6:.0f}M")
    log.info(f"   MIN EUR      : €{MIN_AMOUNT_EUR / 1e6:.2f}M (taux {EUR_TO_USD})")
    log.info(f"   SAM_KEY      : {'✓ défini' if SAM_GOV_KEY else '✗ MANQUANT (.env)'}")
    log.info(f"   Keywords     : {len(SAMGOV_KW)} termes SAM.gov")
    log.info(f"   CPV TED      : {len(TED_CPV_CODES)} codes défense")
    log.info("────────────────────────────────────────────────────────────────")

    arts = run_all_procurement(daysback=daysback)

    if not arts:
        log.warning("Aucun contrat collecté — vérifier la config .env")
        sys.exit(1)

    for i, a in enumerate(arts[:20], 1):
        log.info(
            f"  {i:02d}. [{a['source']:<16}] "
            f"score={a['artscore']:.1f}  "
            f"montant={a['montant']:<14}  "
            f"{a['titre'][:60]}"
        )

    if len(arts) > 20:
        log.info(f"  … et {len(arts) - 20} autres résultats")

    log.info(f"  TOTAL : {len(arts)} contrats/AO collectés")
