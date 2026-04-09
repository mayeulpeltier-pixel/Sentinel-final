# samgov_scraper.py — SENTINEL v3.40 — Appels d'offres défense DoD/TED/BOAMP
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   FIX-SAM1    deptname="DEPT OF DEFENSE" + ptype="o,p,k" (solicitations
#               + pre-award + awards) — champs corrects API SAM.gov v2
#   FIX-SAM2    TED API v3 (POST JSON) + CPV corrects 35612000,35613000
#   FIX-SAM3    seenids set global par run → zéro doublon multi-keyword
#   FIX-SAM4    noticeId (non opportunityId) + artscore=8.5 signal contractuel
#   FIX-OBS1    Logging structuré via logger nommé, zéro print()
#   FIX-M4      Tri par artscore en sortie de chaque collecteur
#   O5-FIX      BOAMP_URL déclaré avant scrape_boamp_fr()
#   E4-OSINT2   BOAMP.fr API publique gratuite, AO défense FR
#   E4-OSINT3   SAMGOV_KW complète v3.40 (50 keywords)
#   A15-FIX     TED EU — appel officiel, CPV défense, v3.40 POST JSON v3
#   NEW-SG1     MIN_AMOUNT configurable via ENV SENTINEL_MIN_CONTRACT_USD
#   NEW-SG2     save_to_db() — enregistrement optionnel SentinelDB
#   NEW-SG3     run_all_procurement() — point d'entrée unique pour
#               sentinelmain.py (remplace les 3 appels séparés)
#   NEW-SG4     Résumé de collecte logué en fin de run
#   NEW-SG5     Timeout & User-Agent cohérents avec scraperrss.py
# ─────────────────────────────────────────────────────────────────────────────
# Dépendances : requests, python-dotenv (opt.)
# Variables d'environnement :
#   SAM_GOV_API_KEY            Clé API SAM.gov (gratuite sur sam.gov/developers)
#   SENTINEL_MIN_CONTRACT_USD  Seuil filtrage montant (défaut : 1 000 000 USD)
#   SENTINEL_MODEL             Version SENTINEL (pour User-Agent)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import requests

# ── Logger structuré (FIX-OBS1) ──────────────────────────────────────────────
log = logging.getLogger("sentinel.samgov")

# ── User-Agent cohérent avec scraperrss.py (NEW-SG5) ─────────────────────────
SENTINEL_VERSION = os.environ.get("SENTINEL_MODEL", "SENTINEL/3.38")
_HEADERS = {"User-Agent": SENTINEL_VERSION}

# ── Seuil montant minimum (NEW-SG1) ──────────────────────────────────────────
MIN_AMOUNT = float(os.environ.get("SENTINEL_MIN_CONTRACT_USD", "1000000"))

# ── Clé API SAM.gov ───────────────────────────────────────────────────────────
SAM_GOV_KEY = os.environ.get("SAM_GOV_API_KEY", "")

# ── API endpoints ─────────────────────────────────────────────────────────────
SAM_GOV_API = "https://api.sam.gov/opportunities/v2/search"
TED_API     = "https://api.ted.europa.eu/v3/notices/search"  # FIX-SAM2 POST v3
BOAMP_URL   = "https://www.boamp.fr/api/search"              # O5-FIX — déclaré avant usage


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
    # UGV programmes OTAN critiques (v3.16)
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
    # IA militaire 2025-2026 (E4-OSINT6)
    "machine learning targeting",
    "autonomous decision-making",
    "large language model intelligence",
    "golden horde darpa",
    # Acteurs industriels disruptifs (v3.16)
    "anduril",
    "shield ai",
]

# CPV défense TED EU (FIX-SAM2)
TED_CPV_CODES = [
    "35000000",  # Équipements de sécurité, défense, armements
    "35100000",  # Matériels et fournitures de survie et de sécurité
    "35120000",  # Systèmes et dispositifs de surveillance
    "35612000",  # Véhicules blindés de combat sans équipage
    "35613000",  # Drones militaires
    "35700000",  # Drones et systèmes robotisés militaires
    "38820000",  # Télécommandes et systèmes autonomes
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _since_date(daysback: int) -> str:
    """Format mm/dd/yyyy pour SAM.gov."""
    return (datetime.utcnow() - timedelta(days=daysback)).strftime("%m/%d/%Y")


def _since_date_iso(daysback: int) -> str:
    """Format YYYY-MM-DD pour TED EU et BOAMP."""
    return (datetime.utcnow() - timedelta(days=daysback)).strftime("%Y-%m-%d")


def _safe_amount(val) -> float:
    """Convertit une valeur montant en float, 0.0 si invalide."""
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
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
    Compatible scraperrss.py + format_for_claude() + SentinelDB.
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
# SAM.GOV — APPELS D'OFFRES DoD (FIX-SAM1 / FIX-SAM3 / FIX-SAM4)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sam_gov_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les appels d'offres DoD des N derniers jours via SAM.gov API v2.

    FIX-SAM1 : deptname="DEPT OF DEFENSE" + ptype="o,p,k"
               (solicitations + pre-award + awards — tous types contractuels)
    FIX-SAM3 : seen_ids set global → zéro doublon sur les 50 keywords
    FIX-SAM4 : noticeId (champ v2 correct, non opportunityId) + artscore=8.5
    NEW-SG1  : MIN_AMOUNT configurable via ENV
    """
    if not SAM_GOV_KEY:
        log.warning("SAM.GOV SAM_GOV_API_KEY manquant — module ignoré")
        return []

    since     = _since_date(daysback)
    results:  list[dict] = []
    seen_ids: set[str]   = set()   # FIX-SAM3

    for kw in SAMGOV_KW:
        try:
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

            for opp in resp.json().get("opportunitiesData", []):
                nid = opp.get("noticeId", "")          # FIX-SAM4
                if not nid or nid in seen_ids:
                    continue                            # FIX-SAM3
                seen_ids.add(nid)

                raw_amount = _safe_amount(
                    (opp.get("award") or {}).get("amount") or 0
                )
                if 0 < raw_amount < MIN_AMOUNT:        # NEW-SG1
                    continue

                montant = (
                    f"${raw_amount / 1e6:.1f}M USD" if raw_amount >= 1e6
                    else f"${raw_amount:,.0f} USD"   if raw_amount > 0
                    else "N/A"
                )

                results.append(_make_article(
                    titre   = f"CONTRAT DoD — {opp.get('title', '')[:100]}",
                    url     = f"https://sam.gov/opp/{nid}/view",
                    date    = opp.get("postedDate", "N/A"),
                    source  = "SAM.gov DoD",
                    resume  = _truncate(opp.get("description", ""), 300),
                    artscore= 8.5,   # FIX-SAM4
                    montant = montant,
                ))

        except requests.RequestException as e:
            log.warning(f"SAM.GOV Erreur keyword={kw!r} : {e}")

    results.sort(key=lambda a: -a["artscore"])  # FIX-M4
    log.info(
        f"SAM.GOV {len(results)} contrats DoD collectés "
        f"(MIN={MIN_AMOUNT / 1e6:.0f}M USD, fenêtre {daysback}j)"
    )
    return results


# ─────────────────────────────────────────────────────────────────────────────
# TED EU — APPELS D'OFFRES DÉFENSE EUROPÉENS (FIX-SAM2 / A15-FIX)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ted_eu_opportunities(daysback: int = 2) -> list[dict]:
    """
    Récupère les AO défense UE via TED API v3 (POST JSON).

    FIX-SAM2 : endpoint POST /v3/notices/search + CPV défense corrects
    A15-FIX  : TED EU officiel, gratuit, équivalent SAM.gov pour l'UE
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
        "cpvCodes":            TED_CPV_CODES,   # FIX-SAM2
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
    log.info(f"TED EU {len(results)} AO défense collectés (fenêtre {daysback}j)")
    return results


def _fetch_ted_eu_v2_fallback(daysback: int = 2) -> list[dict]:
    """
    Fallback TED API v2 si v3 indisponible.
    Appelé automatiquement par fetch_ted_eu_opportunities().
    """
    TED_V2 = "https://ted.europa.eu/api/v2.0/notices/search"
    since  = _since_date_iso(daysback)
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
# BOAMP.FR — APPELS D'OFFRES DÉFENSE FRANCE (E4-OSINT2 / O5-FIX)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_boamp_fr(daysback: int = 2) -> list[dict]:
    """
    Collecte les AO défense FR via l'API BOAMP (gratuite, officielle).

    E4-OSINT2 : BOAMP.fr — API publique Légifrance, AO défense robotique
    O5-FIX    : BOAMP_URL déclaré au niveau module avant cet appel
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
            src    = h.get("_source", {})
            objet  = src.get("objet", "N/A")
            date_p = src.get("dateparution", "N/A")
            ref    = src.get("idweb") or src.get("reference", "")
            url    = (
                f"https://www.boamp.fr/avis/detail/{ref}"
                if ref else "https://www.boamp.fr"
            )
            montant = str(src.get("montant_estime", "N/A"))
            pouvoir = src.get("pouvoir_adjudicateur", "")

            results.append(_make_article(
                titre    = f"BOAMP — {objet[:100]}",
                url      = url,
                date     = date_p,
                source   = "BOAMP FR",
                resume   = f"Acheteur : {pouvoir[:80]}. Montant : {montant}.",
                artscore = 7.5,
                montant  = montant,
            ))

    except requests.RequestException as e:
        log.warning(f"BOAMP Erreur : {e}")

    results.sort(key=lambda a: -a["artscore"])  # FIX-M4
    log.info(f"BOAMP {len(results)} AO défense FR collectés (fenêtre {daysback}j)")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# SAVE TO DB — DÉDUPLICATION INTER-RUNS (NEW-SG2)
# ─────────────────────────────────────────────────────────────────────────────

def _save_to_db(articles: list[dict]) -> None:
    """
    Enregistre les hash titre+url dans SentinelDB.seenhashes pour éviter
    les re-collectes lors des runs suivants.
    Non bloquant — ne lève jamais d'exception (NEW-SG2).
    """
    try:
        import hashlib
        from db_manager import SentinelDB

        hashes = {
            hashlib.sha256(
                (a["titre"] + a["url"]).encode()
            ).hexdigest()[:20]
            for a in articles
        }
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

    Point d'entrée unique pour sentinelmain.py (NEW-SG3).
    Remplace les 3 appels séparés dans le pipeline.

    Usage dans sentinelmain.py :
        from samgov_scraper import run_all_procurement
        contracts = run_all_procurement(daysback=2)
        articles.extend(contracts)
    """
    all_articles: list[dict] = []

    sam   = fetch_sam_gov_opportunities(daysback=daysback)
    ted   = fetch_ted_eu_opportunities(daysback=daysback)
    # O-R3-FIX : scrape_boamp_fr() était définie mais jamais appelée
    try:
        boamp = scrape_boamp_fr(daysback=daysback)
        log.info(f"BOAMP : {len(boamp)} marchés publics français collectés")
    except Exception as _boamp_err:
        boamp = []
        log.warning(f"BOAMP : erreur non bloquante : {_boamp_err}")
    boamp = scrape_boamp_fr(daysback=daysback)

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
        f"(fenêtre {daysback}j)"
    )
    return all_articles


# ─────────────────────────────────────────────────────────────────────────────
# CLI — diagnostic & test
# ─────────────────────────────────────────────────────────────────────────────\n\nif __name__ == "__main__":\n    import sys\n\n    logging.basicConfig(\n        level   = logging.INFO,\n        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",\n        datefmt = "%Y-%m-%dT%H:%M:%S",\n    )\n\n    daysback = int(sys.argv[1]) if len(sys.argv) > 1 else 2\n\n    print(f"\n── SENTINEL Procurement Scraper v3.40 ──────────────────────────")\n    print(f"   Fenêtre    : {daysback} jours")\n    print(f"   MIN_AMOUNT : ${MIN_AMOUNT / 1e6:.0f}M USD")\n    print(f"   SAM_KEY    : {'✓ défini' if SAM_GOV_KEY else '✗ MANQUANT (.env)'}")\n    print(f"   Keywords   : {len(SAMGOV_KW)} termes SAM.gov")\n    print(f"   CPV TED    : {len(TED_CPV_CODES)} codes défense")\n    print(f"────────────────────────────────────────────────────────────────\n")\n\n    arts = run_all_procurement(daysback=daysback)\n\n    if not arts:\n        print("  Aucun contrat collecté — vérifier la config .env")\n        sys.exit(1)\n\n    for i, a in enumerate(arts[:20], 1):\n        print(\n            f"  {i:02d}. [{a['source']:<16}] "\n            f"score={a['artscore']:.1f}  "\n            f"montant={a['montant']:<12}  "\n            f"{a['titre'][:65]}"\n        )\n\n    if len(arts) > 20:\n        print(f"\n  … et {len(arts) - 20} autres résultats")\n\n    print(f"\n  TOTAL : {len(arts)} contrats/AO collectés\n")