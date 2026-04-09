# scraper_rss.py — SENTINEL v3.40
# Collecte, filtrage et scoring RSS — Robotique Défense
#
# CHANGELOG v3.40 vs v3.37 :
#   [SCR-FIX-1]  SQLite : load_seen/save_seen → SentinelDB (plus de JSON seen_hashes)
#   [SCR-FIX-2]  cross_reference : boost 1.5*ln(N) → 0.8*ln(N) additif (MATHS-M4)
#   [SCR-FIX-3]  AFRICA_BRICS_KEYWORDS fusionné dans KEYWORDS (était défini mais séparé)
#   [SCR-FIX-4]  NLP rerank optionnel : import nlp_scorer avec fallback gracieux
#   [SCR-FIX-5]  articles dict clés normalisées : titre/resume/url/date/source/score/art_score
#   [SCR-FIX-6]  Purge seen_hashes SQLite appelée à chaque run (remplace logique MAXSEEN JSON)
#   [SCR-FIX-7]  LATAM_KEYWORDS explicitement injecté dans KEYWORDS (double protection)
#   [SCR-FIX-8]  format_for_claude : sort stable par (-art_score, -score_source)
#   [SCR-FIX-9]  save_seen batchée une seule fois après scraping (non dans le thread)
#   [SCR-FIX-10] EUISS, CNAS, Lawfare, CASI, Jamestown ajoutés (5 feeds v3.37 manquants)
#   [SCR-FIX-11] User-Agent versionné R1-F1 centralisé en constante

from __future__ import annotations

import feedparser
import hashlib
import re
import socket
import math
import time
import threading
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ──────────────────────────────────────────────────────────────────────────────
_VERSION = "3.40"
USER_AGENT = f"SENTINEL/{_VERSION} RSS-Collector (+https://github.com/sentinel-osint)"

log = logging.getLogger("sentinel.scraper")

# ──────────────────────────────────────────────────────────────────────────────
# NLP RERANK OPTIONNEL — import nlp_scorer si disponible (PRIORITÉ 5 / A32)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from nlp_scorer import nlp_rerank as _nlp_rerank  # type: ignore
    _NLP_AVAILABLE = True
    log.info("NLP scorer chargé (TF-IDF bigrammes actif)")
except ImportError:
    _NLP_AVAILABLE = False
    log.debug("nlp_scorer absent — ranking NLP désactivé (pip install scikit-learn)")

# ──────────────────────────────────────────────────────────────────────────────
# SENTINEL DB — seen hashes via SQLite (SCR-FIX-1)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from db_manager import SentinelDB, getdb  # type: ignore
    _DB_AVAILABLE = True
    log.info("SentinelDB connecté — seen_hashes SQLite actif")
except ImportError:
    _DB_AVAILABLE = False
    log.warning("db_manager absent — fallback seen_hashes JSON")

# ──────────────────────────────────────────────────────────────────────────────
# SEEN HASHES — FALLBACK JSON si SentinelDB indisponible
# ──────────────────────────────────────────────────────────────────────────────
_SEEN_FILE = Path("data/seen_hashes.json")
_MAX_SEEN_JSON = 15_000  # ~75 jours à 200 art/jour

def _load_seen_json() -> list[str]:
    """Fallback : charge depuis seen_hashes.json (format liste ordonnée)."""
    if _SEEN_FILE.exists():
        import json
        data = json.loads(_SEEN_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else list(data)
    return []

def _save_seen_json(seen_list: list[str]) -> None:
    """Fallback : sauvegarde atomique seen_hashes.json avec purge chronologique."""
    import json
    if len(seen_list) > _MAX_SEEN_JSON:
        seen_list = seen_list[-_MAX_SEEN_JSON:]  # garder les plus récents
    tmp = _SEEN_FILE.with_suffix(".tmp")
    _SEEN_FILE.parent.mkdir(exist_ok=True)
    tmp.write_text(json.dumps(seen_list), encoding="utf-8")
    tmp.replace(_SEEN_FILE)  # atomique


def load_seen() -> tuple[list[str], set[str]]:
    """
    Charge les hashes vus depuis SQLite (ou JSON fallback).
    Retourne (seen_list, seen_set).
    """
    if _DB_AVAILABLE:
        seen_set = SentinelDB.loadseen()  # retourne un set
        return list(seen_set), seen_set
    else:
        seen_list = _load_seen_json()
        return seen_list, set(seen_list)


def save_seen(new_hashes: set[str]) -> None:
    """
    Persiste les nouveaux hashes uniquement.
    SQLite : INSERT OR IGNORE batchée. JSON : append + purge + rename atomique.
    """
    if not new_hashes:
        return
    if _DB_AVAILABLE:
        SentinelDB.saveseen(new_hashes, source="rss_scraper")
        # Purge chronologique : hashes >90 jours (corrige BUG-DB2 syntaxe SQLite)
        removed = SentinelDB.purgeseeolderthandays(90)
        if removed:
            log.info(f"SCRAPER Purge SQLite : {removed} hashes >90j supprimés")
    else:
        seen_list, _ = load_seen()
        seen_list.extend(new_hashes)
        _save_seen_json(seen_list)


def article_hash(title: str, url: str) -> str:
    """SHA-256 tronqué à 20 chars — identifiant article dédup."""
    return hashlib.sha256(f"{title}{url}".encode()).hexdigest()[:20]

# ──────────────────────────────────────────────────────────────────────────────
# FILTRES ANTI-PROPAGANDE
# ──────────────────────────────────────────────────────────────────────────────

SCMP_BLACKLIST = [
    "xi jinping", "pla shows", "taiwan province", "reunification",
    "wolf warrior", "splittist", "china firmly opposes", "sovereignty inviolable",
    "south china sea sovereignty", "military drills show strength",
    "taiwan independence splittist", "one china inviolable",
    "china warns consequences", "beijing condemns", "saber rattling pla",
    "comprehensive national power",
]

IRAN_BLACKLIST = [
    "zionist regime", "resistance forces", "liberation front",
    "islamic revolution", "death to israel", "great satan",
    "axis of resistance",
]

LATAM_BLACKLIST = [
    "bolivarian revolution", "imperial aggression", "yankee imperialism",
]

ISW_WHITELIST = [
    "drone", "ugv", "fpv", "loitering", "uav", "robot", "autonomous", "unmanned",
    "kamikaze", "shahed", "lancet", "orlan", "geran", "zala", "kub-bla", "ulan",
    "unmanned vehicle", "counter-drone", "c-uas", "swarm", "electronic warfare",
    # O-R1-FIX : termes terrain Ukraine/Russie
    "oryx", "equipment loss", "destroyed", "abandoned", "captured vehicle",
    "switchblade", "phoenix ghost", "warmate", "cube uav",
]

ACLED_KEYWORDS = [
    "drone strike", "loitering munition", "autonomous weapon", "ugv combat",
    "fpv drone", "kamikaze drone", "suicide drone", "armed drone",
    "uav strike", "shahed", "lancet strike", "uncrewed", "unmanned vehicle",
]

ARXIV_EXCLUDE = [
    "surgical", "rehabilitation", "agriculture", "warehouse", "medical",
    "social robot", "human-robot interaction", "bipedal", "prosthetic",
    "delivery robot", "domestic robot", "food delivery robot", "service robot",
    "cleaning robot", "elder care robot", "companion robot", "entertainment robot",
]

FARS_BIAS_NOTE = "⚠ FARS News Agency : biais éditorial IRGC connu — score=C, contenu à croiser"


def is_scmp_propaganda(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in SCMP_BLACKLIST)

def is_iran_propaganda(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in IRAN_BLACKLIST)

def is_latam_propaganda(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in LATAM_BLACKLIST)

def is_isw_relevant(title: str, summary: str) -> bool:
    """Filtre ISW — conserver uniquement articles robotique/drone/autonome/EW."""
    text = (title + " " + summary).lower()
    return any(kw in text for kw in ISW_WHITELIST)

def is_acled_relevant(title: str, summary: str) -> bool:
    """Filtre ACLED — incidents impliquant systèmes autonomes ou drones armés."""
    text = (title + " " + summary).lower()
    return any(kw in text for kw in ACLED_KEYWORDS)

# ──────────────────────────────────────────────────────────────────────────────
# MOTS-CLÉS (KEYWORDS fusionnés + LATAM + AFRICA/BRICS)
# ──────────────────────────────────────────────────────────────────────────────

KEYWORDS: list[str] = [
    # ── Systèmes terrestres ──────────────────────────────────────────────────
    "ugv", "unmanned ground vehicle", "military autonomous vehicle", "robot defense",
    "robot soldat", "armement autonome", "milrem", "boston dynamics",
    "ghost robotics", "arquus", "rheinmetall", "textron", "ripsaw",
    # ── Systèmes maritimes ───────────────────────────────────────────────────
    "usv", "uuv", "drone naval", "autonomous underwater vehicle",
    "naval robot", "saildrone", "unmanned surface vessel", "autonomous underwater",
    # ── Technologies transverses ─────────────────────────────────────────────
    "military drone", "drone swarm", "essaim", "loitering", "autonomous weapon",
    "darpa", "dga", "eda", "nato autonomy", "exail", "eca group",
    "edge ai military", "multi-domain autonomy",
    # ── Israël ───────────────────────────────────────────────────────────────
    "elbit systems", "iai autonomous", "rafael autonomous", "guardium ugv",
    "rex robot israel", "lanius drone", "torch-x elbit", "seagull usv",
    "mule usv", "heron ugv", "rotem-l",
    # ── Asie-Pacifique ───────────────────────────────────────────────────────
    "norinco ugv", "samsung techwin sgr", "pla autonomous", "china military robot",
    "atla japan", "south korea ugv", "hsu-001", "sharp claw norinco",
    "hyundai rotem robot", "k2 autonomous", "dapa korea",
    # ── Turquie ──────────────────────────────────────────────────────────────
    "bayraktar", "baykar", "stm kargu", "kargu", "aselsan", "roketsan",
    "togan", "akinci", "defence turkey", "turkish uav", "turkish drone",
    # ── Think tanks stratégiques ─────────────────────────────────────────────
    "iiss", "sipri", "rand corporation", "rusi", "think tank defense",
    # ── Norvège / Industrie ───────────────────────────────────────────────────
    "kongsberg", "mspc usv", "nemo mortar", "protector rws",
    # ── Ukraine retex ────────────────────────────────────────────────────────
    "ukraine drone", "brave1", "fpv drone combat", "zomak",
    "fpv combat", "lancet strike", "shahed drone", "orlan uav",
    # ── Industriels US disruptifs ─────────────────────────────────────────────
    "anduril", "shield ai", "l3harris", "palantir defense", "lockheed martin robot",
    "northrop grumman uuv", "qinetiq", "saab defense", "roboteam",
    # ── Programmes UGV NATO ───────────────────────────────────────────────────
    "mission master", "black knight ugv", "type x ugv", "rheinmetall ugv",
    "titan ugv", "camel ugv", "rex mk2", "smss robot",
    # ── Programmes USV/UUV ────────────────────────────────────────────────────
    "sea hunter", "ghost fleet overlord", "orca uuv", "manta ray darpa",
    "ghost shark uuv", "bluefin uuv", "remus uuv", "mine countermeasure autonomous",
    "extra large uuv", "xluuv", "autonomous submarine",
    # ── Doctrines et concepts ─────────────────────────────────────────────────
    "manned unmanned teaming", "mum-t", "lethal autonomous weapon", "laws drone",
    "human on the loop", "mosaic warfare", "collaborative combat aircraft",
    "loyal wingman", "autonomous system otan", "multi-domain operations",
    "human in the loop", "algorithmic warfare", "kill chain autonomy",
    # ── Russie ────────────────────────────────────────────────────────────────
    "uran-9", "marker ugv", "soratnik", "kalashnikov robot",
    # ── UK/FR/USN programmes ──────────────────────────────────────────────────
    "devil ray", "razorback uuv", "knifefish mcm", "snakehead uuv", "cormorant darpa",
    "malloy t150", "madfox ugv", "tars uk", "uk autonomous ground vehicle",
    "nereid exail", "sylena dcns", "souvim2", "minehunter autonomous france",
    # ── Contractuel ───────────────────────────────────────────────────────────
    "drone-on-drone", "counter-uas defense", "anti-drone swarm",
    "lrip defense", "milestone c defense", "jctd autonomous", "actd autonomous",
    # ── IA militaire & drones 2025-2026 ──────────────────────────────────────
    "ai-enabled warfare", "machine learning targeting", "autonomous decision-making",
    "large language model intelligence", "llm military", "foundation model defense",
    "cca program", "cca", "autonomous wingman",
    "xq-58 valkyrie", "mq-28 ghost bat",
    "attritable uav", "golden horde darpa", "low-cost attritable aircraft",
    "directed energy weapon", "high energy laser weapon", "hel system", "iron beam",
    "cognitive electronic warfare", "electronic warfare ai", "jamming autonomous",
    "orca xluuv", "autonomous minehunter", "xluuv program",
    "space domain awareness", "satellite autonomous defense", "orbital warfare",
    "autonomous cyber defense", "offensive cyber ai", "cyber weapon autonomous",
    "counter-uas", "c-uas", "anti-drone system", "droneshield", "dedrone",
    "shorad", "trophy aps", "iron fist aps", "active protection system",
    # ── AUKUS & Asie du Sud ───────────────────────────────────────────────────
    "aukus autonomous", "aukus pillar ii", "aukus uuv", "aukus drone",
    "pakistan drone", "nescom uav", "burraq ucav", "shahpar uav",
    "bangladesh drone defense", "bangladesh military robot",
    "ladakh drone", "lac surveillance", "india border autonomous",
    # ── LATAM (SCR-FIX-7 : fusionné depuis LATAM_KEYWORDS) ────────────────────
    "embraer defense", "kc-390 embraer", "avibras astros", "guarani vbtp",
    # ── Afrique subsaharienne + BRICS (SCR-FIX-3 : fusionné depuis AFRICA_BRICS_KEYWORDS)
    "wagner africa drone", "bayraktar sahel", "mali drone autonomous",
    "burkina faso drone", "african union defense robot", "south africa drone defense",
    "brics military autonomous", "india brazil defense", "russia africa weapon",
    "china africa military", "turkey africa defense",
]

# ──────────────────────────────────────────────────────────────────────────────
# FLUX RSS — 77 sources v3.40
# ──────────────────────────────────────────────────────────────────────────────

RSS_FEEDS: list[tuple[str, str, str]] = [
    # ── Zone USA / OTAN anglophone ────────────────────────────────────────────
    ("https://www.defensenews.com/rss/",                "Defense News",              "A"),
    ("https://breakingdefense.com/feed/",               "Breaking Defense",          "A"),
    ("https://www.csis.org/rss.xml",                    "CSIS Defense",              "A"),
    ("https://www.navalnews.com/feed/",                 "Naval News",                "A"),
    ("https://www.thedrive.com/the-war-zone/rss",       "The War Zone",              "A"),
    ("https://warontherocks.com/feed/",                 "War on the Rocks",          "A"),
    ("https://www.unmannedsystemstechnology.com/feed/", "Unmanned Systems Tech",     "A"),
    ("https://www.naval-technology.com/feed/",          "Naval Technology",          "A"),
    ("https://www.ausa.org/rss.xml",                    "AUSA Publications",         "A"),
    ("https://rusi.org/explore-our-research/rss.xml",   "RUSI",                      "A"),
    ("https://www.rand.org/rss/",                       "RAND",                      "A"),
    ("https://www.iiss.org/rss",                        "IISS Military Balance",     "A"),
    ("https://www.darpa.mil/rss.xml",                   "DARPA News",                "A"),
    ("https://www.aspistrategist.org.au/feed/",         "ASPI Strategist",           "A"),
    ("https://www.defenceconnect.com.au/feed",          "Defence Connect AU",        "A"),
    ("https://www.understandingwar.org/feed",           "ISW Conflict",              "A"),
    ("https://thediplomat.com/category/security/feed/", "The Diplomat",              "B"),
    ("https://taskandpurpose.com/feed/",                "Task & Purpose",            "B"),
    # ── USA spécialisés ───────────────────────────────────────────────────────
    ("https://news.usni.org/feed",                      "USNI News",                 "A"),
    ("https://ukdefencejournal.org.uk/feed/",           "UK Defence Journal",        "A"),
    ("https://www.c4isrnet.com/rss/news/",              "C4ISRNET",                  "A"),
    ("https://www.army-technology.com/feed/",           "Army Technology",           "A"),
    ("https://www.nationaldefensemagazine.org/rss/articles", "National Defense Mag", "A"),
    ("https://www.defensenews.com/land/rss/",           "Defense News Land",         "A"),
    # ── Think tanks v3.37-v3.40 (SCR-FIX-10) ─────────────────────────────────
    ("https://www.iss.europa.eu/content/rss.xml",       "EUISS",                     "A"),
    ("https://www.cnas.org/feed",                       "CNAS",                      "A"),
    ("https://www.lawfaremedia.org/feed",               "Lawfare",                   "A"),
    ("https://jamestown.org/program/china-brief/feed/", "CASI China Brief (Jamestown)","A"),  # O1-FIX: aerospacechinaorg invalide → China Brief
    ("https://jamestown.org/feed/",                     "Jamestown Foundation",      "A"),
    # ── Zone France / Europe ──────────────────────────────────────────────────
    ("https://www.opex360.com/feed/",                   "Opex360",                   "A"),
    ("https://www.meta-defense.fr/feed/",               "Meta-Defense",              "A"),
    ("https://lignesdedefense.blogs.ouest-france.fr/rss.xml", "Lignes de Defense",  "A"),
    ("https://www.air-cosmos.com/rss.xml",              "Air & Cosmos",              "A"),
    ("https://www.irsem.fr/rss.xml",                    "IRSEM",                     "A"),
    ("https://eda.europa.eu/rss/news.xml",              "EDA",                       "A"),
    ("https://www.sipri.org/rss.xml",                   "SIPRI",                     "A"),
    ("https://eudefence.net/feed/",                     "European Defence Review",   "B"),
    ("https://www.defense.gouv.fr/dga/rss.xml",        "DGA Actualités",             "A"),
    ("https://www.meretmarine.com/rss.xml",             "Mer et Marine",             "B+"),
    ("https://www.act.nato.int/rss",                    "NATO ACT Transformation",   "A"),
    ("https://defence-industry-space.ec.europa.eu/rss.xml", "European Defence Fund", "A"),
    # ── Zone Pologne ──────────────────────────────────────────────────────────
    ("https://www.defence24.pl/en/feed/",               "Defence24 Poland",          "A"),
    ("https://zbiam.pl/feed/",                          "Zbrojeniawka PL",           "A"),
    # ── Zone Allemagne ────────────────────────────────────────────────────────
    ("https://www.hartpunkt.de/feed/",                  "Hartpunkt Defense",         "A"),
    ("https://www.bundeswehr.de/rss",                   "Bundeswehr News",           "A"),
    # ── Zone Turquie ──────────────────────────────────────────────────────────
    ("https://www.defenceturkey.com/en/rss",            "Defence Turkey",            "A"),
    ("https://www.savunmasanayist.com/en/rss",          "Savunma Sanayist",          "B"),  # O-05-FIX: B (spécialisé défense TR)
    # ── Zone Israël ───────────────────────────────────────────────────────────
    ("https://www.israeldefense.co.il/en/rss.xml",      "Israel Defense",            "A"),
    ("https://www.shephardmedia.com/news/landwarfareint/rss/", "Shephard Media",     "A"),
    ("https://www.rafael.co.il/rss/",                   "Rafael Systems",            "B+"),
    ("https://elbitsystems.com/rss/news/",              "Elbit Systems",             "B+"),
    # ── Zone Japon / Corée du Sud ─────────────────────────────────────────────
    ("https://www.mod.go.jp/atla/en/news/rss.xml",      "ATLA Japan",                "A"),
    ("https://www.dapa.go.kr/en/rss",                   "DAPA Korea",                "A"),
    ("https://www.kida.re.kr/eng/rss",                  "KIDA Korea",                "A"),
    # ── Zone Moyen-Orient & analyse globale ───────────────────────────────────
    ("https://www.edgegroup.ae/en/media/news/rss",      "Edge Group UAE",            "B+"),
    ("https://adf-magazine.com/feed/",                  "Africa Defence Forum",      "B"),
    ("https://acleddata.com/feed/",                     "ACLED Conflict Data",       "A"),
    ("https://www.al-monitor.com/rss/security.xml",     "Al-Monitor Security",       "B+"),
    ("https://en.farsnews.ir/rss.xml",                  "FARS News Agency",          "C"),
    ("https://www.scmp.com/rss/5/feed",                 "SCMP Defense",              "B"),
    # ── Zone Asie-Pacifique ───────────────────────────────────────────────────
    ("https://www.38north.org/feed/",                   "38North Stimson",           "A"),
    # ── Zone Inde ─────────────────────────────────────────────────────────────
    ("https://pib.gov.in/RssMain.aspx?ModId=6",         "PIB Defence India",         "A"),
    ("https://www.indiastrategic.in/feed/",             "India Strategic",           "B+"),
    ("https://takshashila.org.in/feed/",                "Takshashila Institution",   "B"),
    ("https://www.indiadefencereview.com/feed/",        "India Defence Review",      "B+"),
    # ── Zone Ukraine retex ────────────────────────────────────────────────────
    ("https://mil.in.ua/en/feed/",                      "Defense of Ukraine",        "B"),
    ("https://www.kyivindependent.com/feed/",           "Kyiv Independent",          "B"),
    ("https://defense-ua.com/en/feed/",                 "Defense Ukraine Industry",  "B"),
    # ── Zone Russie/Ukraine terrain — O-R1-FIX (sources retex haute valeur) ─────
    ("https://www.oryxspioenkop.com/feeds/posts/default","Oryx Equipment Losses",    "A"),
    ("https://ukraine-weapons-tracker.com/feed/",       "UA Weapons Tracker",        "B+"),
    ("https://citeam.org/feed/",                        "CIT Conflict Intelligence", "A"),
    ("https://www.ukrinform.net/rss/block-ato",         "Ukrinform Defense",         "B"),
    ("https://en.defence-blog.com/feed/",               "Defence Blog (UA/RU)",      "B+"),
    # ── Zone Pakistan / Bangladesh ────────────────────────────────────────────
    ("https://www.defencejournal.com/feed/",            "Defence Journal Pakistan",  "B"),
    ("https://www.mod.gov.bd/site/page/rss.xml",        "MoD Bangladesh",            "B"),
    # ── Zone Afrique ──────────────────────────────────────────────────────────
    ("https://www.rfi.fr/fr/afrique/feed",              "RFI Afrique Défense",       "B"),
    ("https://www.jeuneafrique.com/rubrique/securite/feed/", "Jeune Afrique Sécu",  "B"),
    # ── Zone Russie (analyse secondaire) ──────────────────────────────────────
    ("https://www.voanews.com/z/4250",                  "VOA Russia Defense",        "B"),
    # ── Industrie & Académique ────────────────────────────────────────────────
    ("https://arxiv.org/rss/cs.RO",                     "arXiv Robotics",            "A"),
    ("https://milremrobotics.com/feed/",                "Milrem Robotics",           "B+"),
    ("https://www.ecagroup.com/en/news/feed",           "ECA Group",                 "B+"),
    ("https://gcaptain.com/feed/",                      "gCaptain Maritime",         "B"),
    ("https://www.kongsberg.com/rss",                   "Kongsberg Defence",         "A"),
    ("https://www.saabgroup.com/rss/",                  "Saab Group",                "B+"),
]

# ──────────────────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ──────────────────────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    """Supprime les balises HTML et tronque à 600 chars."""
    return (re.sub(r"<[^>]+>", "", text) or "").strip()[:600]


def is_relevant(title: str, summary: str, source: str = "") -> bool:
    """
    Filtre de pertinence multi-couche :
    1. Exclusion arXiv hors-scope
    2. Match keyword KEYWORDS fusionné (LATAM + AFRICA/BRICS inclus)
    """
    if "arxiv" in source.lower():
        text = (title + " " + summary).lower()
        if any(x in text for x in ARXIV_EXCLUDE):
            return False
    text = (title + " " + summary).lower()
    return any(kw in text for kw in KEYWORDS)

# ──────────────────────────────────────────────────────────────────────────────
# SCORING TEMPOREL — décroissance exponentielle (demi-vie ~4.6 jours)
# ──────────────────────────────────────────────────────────────────────────────

def _weight_temporal(date_str: str, lam: float = 0.15) -> float:
    """
    w = exp(-λ·δ). λ=0.15 → demi-vie ≈ 4.62 jours.
    FIX CODE-R7 : datetime.now(UTC) partout — pas de mélange timezone.
    """
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(date_str).replace(tzinfo=timezone.utc)
        delta = (datetime.now(timezone.utc) - d).total_seconds() / 86400.0
        return math.exp(-lam * max(0.0, delta))
    except Exception:
        return 1.0  # pas de pénalité si date inconnue/mal formée

# ──────────────────────────────────────────────────────────────────────────────
# SCORING ARTICLE — sigmoïde calibrée
# ──────────────────────────────────────────────────────────────────────────────

def score_article(title: str, summary: str, source_score: str) -> float:
    """
    Score 1–10 via sigmoïde s = 1 + 9/(1+exp(-0.5*(r-5))).
    Plage r : [3.5, ~15].
    Bonus source A: +1.0. Malus source C: -1.5. B/B+ = neutre (E2-FIX).
    """
    text = (title + " " + summary).lower()

    HIGH = [
        "contract", "awarded", "contrat", "test", "trial", "deployment", "unveiled",
        "acquisition", "breakthrough", "revealed", "successfully", "first", "operational",
    ]
    MED = ["program", "programme", "development", "prototype", "upgrade", "integration"]

    r = 5.0
    r += sum(0.6 for kw in HIGH if kw in text)
    r += sum(0.3 for kw in MED if kw in text)

    # M-01-FIX : plafond appliqué AVANT les bonus/malus source
    # Préserve la différentiation source A/C même sur articles hyper-pertinents
    r = min(r, 11.0)  # cap keywords à 11 (laisse 1pt de marge pour bonus A)

    if source_score == "A":
        r += 1.0   # max atteint : 12.0
    elif source_score == "C":
        r -= 1.5   # malus préservé même au plafond
    # B / B+ = neutre (0)

    r = min(r, 12.0)  # cap final absolu
    score = 1.0 + 9.0 / (1.0 + math.exp(-0.5 * (r - 5.0)))
    return round(min(10.0, max(1.0, score)), 1)

# ──────────────────────────────────────────────────────────────────────────────
# CROSS-REFERENCE — boost multi-sources (SCR-FIX-2 : 0.8*ln(N))
# ──────────────────────────────────────────────────────────────────────────────

_SYNONYMES: dict[str, str] = {
    "autonomous": "autonome", "contract": "contrat", "awarded": "attribue",
    "unmanned": "autonome", "weapon": "armement", "system": "systeme",
    "naval": "naval", "drone": "drone", "test": "test", "ground": "terrestre",
    "loitering": "loitering", "robot": "robot", "missile": "missile",
    "hypersonic": "hypersonique", "laser": "laser",
    "electronic": "electronique", "ai": "ia",
}

def _norm(text: str) -> str:
    s = text.lower()
    for en, fr in _SYNONYMES.items():
        s = s.replace(en, fr)
    return s

def cross_reference(articles: list[dict]) -> list[dict]:
    """
    Détecte les événements rapportés par N≥2 sources → boost signal.
    SCR-FIX-2 : boost additif 0.8*ln(N) (MATHS-M4, était 1.5*ln(N)).
    Fusion Jaccard ≥65% pour clusters quasi-identiques FR/EN.
    """
    import copy
    articles = copy.deepcopy(articles)

    clusters: defaultdict[str, list[dict]] = defaultdict(list)
    for a in articles:
        words = sorted([w for w in _norm(a["titre"]).split() if len(w) > 4])[:6]
        key = " ".join(words)
        clusters[key].append(a)

    # Fusion Jaccard
    ck = list(clusters.keys())
    for i in range(len(ck)):
        for j in range(i + 1, len(ck)):
            if ck[i] not in clusters or ck[j] not in clusters:
                continue
            s1, s2 = set(ck[i].split()), set(ck[j].split())
            if not s1 | s2:
                continue
            jaccard = len(s1 & s2) / len(s1 | s2)
            if jaccard >= 0.70:  # O4-FIX : seuil 0.70 réduit les faux regroupements
                clusters[ck[i]].extend(clusters.pop(ck[j]))

    for arts in clusters.values():
        if len(arts) >= 2:
            for a in arts:
                a["cross_ref"] = len(arts)
                # SCR-FIX-2 : 0.8*ln(N) additif
                a["art_score"] = min(10.0, a.get("art_score", 5.0) + 0.8 * math.log(len(arts)))

    return articles

# ──────────────────────────────────────────────────────────────────────────────
# WORKER THREAD — scrape d'un seul flux RSS
# ──────────────────────────────────────────────────────────────────────────────

def _scrape_one(
    feed_tuple: tuple[str, str, str],
    seen_set: set[str],
    lock: threading.Lock,
) -> list[dict]:
    """
    Worker par flux. Thread-safe via lock sur seen_set.
    Retourne les articles pertinents et nouveaux du flux.
    """
    url, src, sc = feed_tuple
    arts: list[dict] = []

    try:
        f = feedparser.parse(url, request_headers={"User-Agent": USER_AGENT})

        for e in f.entries:
            t = (e.get("title") or "").strip()
            s = strip_html(e.get("summary") or e.get("description") or "")

            # ── Filtres anti-propagande ──────────────────────────────────────
            if src == "SCMP Defense" and is_scmp_propaganda(t, s):
                continue
            if src in ("ISW Conflict", "Understanding War") and not is_isw_relevant(t, s):
                continue
            if src == "ACLED Conflict Data" and not is_acled_relevant(t, s):
                continue
            if src == "FARS News Agency" and is_iran_propaganda(t, s):
                continue
            if is_latam_propaganda(t, s):
                continue

            # ── Filtre pertinence thématique ─────────────────────────────────
            if not is_relevant(t, s, src):
                continue

            # ── Déduplication thread-safe ─────────────────────────────────────
            h = article_hash(t, e.get("link") or "")
            with lock:
                if h in seen_set:
                    continue
                seen_set.add(h)

            # ── Score combiné : sigmoïde × pondération temporelle ─────────────\n            base_score = score_article(t, s, sc)\n            wt = _weight_temporal(e.get("published") or "")\n            art_score = round(max(1.0, base_score * wt), 1)\n\n            resume = s\n            if src == "FARS News Agency":\n                resume = f"{FARS_BIAS_NOTE}\n{s}"

            arts.append({
                "titre":     t,
                "url":       e.get("link") or "",
                "date":      e.get("published") or "NA",
                "source":    src,
                "score":     sc,
                "resume":    resume,
                "art_score": art_score,
                "cross_ref": 1,
                "_hash":     h,   # clé interne, supprimée avant export
            })

    except Exception as ex:
        log.warning(f"RSS {src} : {ex}")
        time.sleep(0.1)

    return arts

# ──────────────────────────────────────────────────────────────────────────────
# SCRAPE PRINCIPAL — ThreadPoolExecutor + SQLite
# ──────────────────────────────────────────────────────────────────────────────

def scrape_all_feeds() -> list[dict]:
    """
    Scraping parallèle de tous les flux RSS.
    - Charge seen_hashes depuis SQLite (ou JSON fallback)
    - 10 threads max
    - Persiste les nouveaux hashes en batch après scraping (SCR-FIX-9)
    - Purge SQLite des hashes >90 jours
    """
    socket.setdefaulttimeout(10)  # timeout global feedparser

    _, seen_set = load_seen()
    lock = threading.Lock()
    articles: list[dict] = []

    max_workers = min(10, len(RSS_FEEDS))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_scrape_one, ft, seen_set, lock): ft
            for ft in RSS_FEEDS
        }
        for fut in as_completed(futures):
            try:
                arts = fut.result()
                articles.extend(arts)
            except Exception as e:
                src = futures[fut][1]
                log.error(f"RSS Worker {src} : {e}")

    # Persistance batch — 1 seule transaction après fin du pool (SCR-FIX-9)
    new_hashes = {a["_hash"] for a in articles}
    save_seen(new_hashes)

    # Suppression clé interne avant export
    for a in articles:
        a.pop("_hash", None)

    log.info(f"[SCRAPER] {len(articles)} nouveaux articles pertinents collectés")
    return articles

# ──────────────────────────────────────────────────────────────────────────────
# FORMAT_FOR_CLAUDE — mise en forme du contexte pour l'API
# ──────────────────────────────────────────────────────────────────────────────

def format_for_claude(articles: list[dict], max_chars: int = 88_000) -> str:
    """
    Prépare le bloc texte injecté dans le prompt Claude.
    1. Cross-reference boost 0.8*ln(N)
    2. NLP rerank optionnel (TF-IDF bigrammes)
    3. Tri stable par (-art_score, -src_priority) (SCR-FIX-8)
    4. Circuit-breaker budget SENTINEL_MAX_TOKENS
    5. Header TRUNCATED si dépassement
    """
    # Étape 1 : cross-reference
    articles = cross_reference(articles)

    # Étape 2 : NLP rerank si nlp_scorer.py disponible
    if _NLP_AVAILABLE:
        try:
            articles = _nlp_rerank(articles, weight=0.3)
        except Exception as e:
            log.warning(f"NLP rerank échoué : {e} — fallback score simple")

    # Étape 3 : tri stable (SCR-FIX-8)
    _src_prio = {"A": 3, "B+": 2, "B": 1, "C": 0}
    articles = sorted(
        articles,
        key=lambda a: (-a.get("art_score", 5.0), -_src_prio.get(a.get("score", "B"), 1))
    )

    # Étape 4 : circuit-breaker budget
    _max_tok = int(os.environ.get("SENTINEL_MAX_TOKENS", "30000"))
    max_chars = min(max_chars, _max_tok * 4)  # ~4 chars/token\n\n    lines: list[str] = []\n    total = 0\n\n    for i, a in enumerate(articles):\n        cross_tag = f" ⚡{a['cross_ref']} src" if a.get("cross_ref", 1) >= 2 else ""\n        block = (\n            f"[{i+1}] {a['titre']}\n"\n            f"Source: {a['source']} (Fiabilité {a['score']}) | Date: {a['date']}\n"\n            f"URL: {a['url']}\n"\n            f"Résumé: {a['resume']} [Score:{a.get('art_score', 5.0):.1f}/10]{cross_tag}\n\n"\n        )\n        if total + len(block) > max_chars:\n            n_skip = len(articles) - i\n            lines.insert(\n                0,\n                f"[⚠ SENTINEL-TRUNCATED : {n_skip}/{len(articles)} articles — résultats PARTIELS]\n\n"\n            )\n            break\n        lines.append(block)\n        total += len(block)

    return "".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE STANDALONE
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    arts = scrape_all_feeds()
    formatted = format_for_claude(arts)

    out = Path("data/articles_du_jour.txt")
    out.parent.mkdir(exist_ok=True)
    out.write_text(formatted, encoding="utf-8")

    log.info(f"[SCRAPER] Fichier généré : {len(formatted)/1000:.1f}k chars")
    log.info("[SCRAPER] Top 5 articles :")
    for a in sorted(arts, key=lambda x: -x.get("art_score", 5.0))[:5]:
        print(f"  [{a.get('art_score',5.0):.1f}] {a['source']:<30} {a['titre'][:80]}")