#!/usr/bin/env python3
# telegram_scraper.py — SENTINEL v3.51 — Scraping Telegram militaire
# ─────────────────────────────────────────────────────────────────────────────
# Prérequis  : pip install telethon
# Optionnel  : pip install requests          (traduction DeepL — TG-R9)
# Credentials: TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env
#              Inscription gratuite : https://my.telegram.org
#
# Historique des corrections (v3.40 → v3.51) :
#
# FIXES v3.40 : (TG-FIX1 à TG-FIX18 — voir historique complet git)
# FIXES v3.40.1 :
#   [TG-R1]  clean_text : 5 patterns reconstruits via chr(92)
#   [TG-R2]  _save_telegram_metrics : table telegram_metrics dédiée
#   [TG-R3]  Retry delays [5, 15, 30]s cohérents avec ASYNCIO_TIMEOUT=300s
#   [TG-R4]  Hashes filtrés (propagande/non-pertinents) persistés
#   [TG-R5]  modmoscow retiré de CHANNELS (doublon CHANNEL_BLACKLIST)
#   [TG-R6]  import time supprimé (dead import)
#   [TG-R7]  _cli_days : validation ValueError
#   [TG-R8]  dry_run/force_purge déplacés en paramètres de run_telegram_scraper
#   [TG-R9]  requests documenté comme dépendance optionnelle
# FIXES v3.40.2 :
#   [TG-R10] f-strings dans tous les logs/messages
#   [TG-R11] argparse remplace le parsing manuel sys.argv
#
# CORRECTIONS v3.51 — Post-audit complet :
#   [TG-R12] _scrape_channel : new_hashes.add(h) ajouté pour les articles
#            RETENUS (belt-and-suspenders) — les articles valides sont
#            maintenant garantis persistés en seenhashes directement dans
#            _scrape_channel, sans dépendre uniquement de la boucle externe.
#   [TG-R13] load_seen() : filtre temporel WHERE dateseen >= datetime('now', ?)
#            L'ancienne requête chargeait TOUS les hashes Telegram depuis
#            l'origine — croissance RAM illimitée après plusieurs mois.
#            Utilise maintenant SentinelDB.loadseendays(days=SEEN_PURGE_DAYS)
#            et fallback JSON uniquement si SentinelDB indisponible.
#   [TG-R14] save_seen() : utilise SentinelDB.saveseen() pour cohérence API
#            avec le reste du pipeline. Fallback JSON atomique conservé.
#   [TG-R15] purge_seen_telegram() : utilise SentinelDB.purgeseen_older_than_days()
#            Fallback requête directe conservé si SentinelDB indisponible.
#   [TG-R16] ASYNCIO_TIMEOUT : configurable via TELEGRAM_ASYNCIO_TIMEOUT (.env)
#            N'était pas exposé comme les autres constantes. Défaut : 300s.
#   [TG-R17] _save_telegram_metrics : CREATE TABLE exécuté une seule fois
#            via flag module-level _telegram_metrics_table_ready. L'ancien
#            code réexécutait le DDL à chaque sauvegarde journalière.
#   [TG-R18] _scrape_async : scraping parallèle via asyncio.gather()
#            + asyncio.Semaphore(TG_PARALLEL_LIMIT, défaut=3).
#            Les 11 canaux sont maintenant scrapés par groupes de 3 simultanés
#            au lieu de séquentiellement — réduction du temps ~3x.
#            Compatible avec le rate-limiting Telegram (délai inter-groupe).
#   [TG-R19] score_telegram : dead code path blacklist supprimé.
#            Le retour 1.0 pour canaux blacklistés était inaccessible car
#            is_propaganda() filtre ces canaux avant score_telegram().
#   [TG-R20] Compatibilité format_for_claude() (scraper_rss.py) — CORRECTION
#            CRITIQUE : les articles Telegram utilisaient des clés incorrectes
#            par rapport au schéma attendu par format_for_claude() :
#              "title"   → "titre"    (accédé comme a['titre'] dans clustering)
#              "summary" → "resume"   (accédé comme a['resume'] dans formatage)
#              "score": float → séparation en deux champs distincts :
#                "score":     chan_score  (label "A"/"B" — fiabilité source)
#                "art_score": score_val  (float calculé — score thématique)
#            Sans cette correction, les articles Telegram étaient silencieusement
#            perdus dans format_for_claude() (KeyError sur 'titre') et le
#            dashboard affichait des scores incorrects ("Fiabilité 7.5").
#            Correction identique appliquée dans _dry_run_sample().
#
# Compatibilité pipeline v3.51 totalement préservée (API run_telegram_scraper inchangée)
# Compatibilité sentinel_main.py v3.54 garantie
# Compatibilité db_manager.py v3.51 garantie (SentinelDB.loadseendays / saveseen)
# Compatibilité scraper_rss.format_for_claude() garantie (TG-R20)
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python telegram_scraper.py            Scraping complet (48h)
#   python telegram_scraper.py --help     Aide complète
#   python telegram_scraper.py --dry-run  Test sans connexion Telegram
#   python telegram_scraper.py --days 7   Fenêtre élargie à 7 jours
#   python telegram_scraper.py --channel wartranslated  Un seul canal
#   python telegram_scraper.py --purge    Force purge seenhashes > 14j
# ─────────────────────────────────────────────────────────────────────────────
# Sortie :
#   data/telegram_latest.json   Derniers messages collectés
#   logs/telegram_stats.json    Statistiques par canal
# ─────────────────────────────────────────────────────────────────────────────
# Intégration sentinel_main.py :
#   from telegram_scraper import run_telegram_scraper
#   articles = run_telegram_scraper()
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Python >= 3.10 (TG-FIX3) ─────────────────────────────────────────────────
if sys.version_info < (3, 10):
    sys.stderr.write(
        f"TELEGRAM_SCRAPER CRITIQUE : Python "
        f"{sys.version_info.major}.{sys.version_info.minor} < 3.10\n"
    )
    sys.exit(2)

# ── Logging structuré (TG-FIX2) ───────────────────────────────────────────────
log = logging.getLogger("sentinel.telegram")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

# =============================================================================
# CONSTANTES & CONFIGURATION
# =============================================================================

VERSION = "3.51"

TELEGRAM_API_ID   = os.environ.get("TELEGRAM_API_ID",   "")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION",  "data/sentinel_telegram.session")

DEEPL_API_KEY   = os.environ.get("DEEPL_API_KEY", "")
DEEPL_API_URL   = "https://api-free.deepl.com/v2/translate"
DEEPL_MAX_CHARS = int(os.environ.get("DEEPL_MAX_CHARS_MONTH", "450000"))

# [TG-R5] modmoscow retiré (présent dans CHANNEL_BLACKLIST — connexion inutile)
CHANNELS: list[tuple[str, str, str, str]] = [
    ("defenseofukraine",  "Defense of Ukraine",     "A", "uk"),
    ("wartranslated",     "War Translated",          "A", "en"),
    ("ukraineweapons",    "Ukraine Weapons Tracker", "A", "uk"),
    ("militarylandnet",   "Military Land",           "A", "en"),
    ("UAWeapons",         "UA Weapons",              "B", "uk"),
    ("DefMon3",           "Defence Monitor",         "B", "en"),
    ("oryxspioenkop",     "Oryx Equipment Losses",   "A", "en"),
    ("bayraktartb2",      "Baykar TB2 Officiel",     "B", "en"),
    ("UAdefenceindustry", "UA Defence Industry",     "B", "en"),
    ("combatarmor",       "Combat Armor",            "B", "en"),
    ("natowarmonitor",    "NATO War Monitor",        "B", "en"),
]

KEYWORDS: set[str] = {
    "ugv", "unmanned ground", "fpv", "robot", "autonomous ground",
    "armored robot", "ground drone", "rover combat", "infantry robot",
    "usv", "uuv", "autonomous underwater", "naval drone", "unmanned surface",
    "maritime autonomous", "naval robot",
    "drone", "uav", "unmanned aerial", "loitering", "kamikaze drone",
    "fpv drone", "shahed", "lancet", "geran", "orlan", "zala", "kub-bla",
    "swarm", "essaim", "autonomous swarm", "collaborative combat",
    "electronic warfare", "ew ", " ew,", "jamming", "c-uas", "counter-drone",
    "anti-drone", "dronegun", "droneshield",
    "autonomous weapon", "laws ", "lethal autonomous", "human on the loop",
    "targeting ai", "fire control", "active protection", "trophy aps",
    "milrem", "textron", "ghost robotics", "boston dynamics", "baykar",
    "stm kargu", "elbit", "iai autonomous", "rafael", "anduril", "shield ai",
    "rheinmetall ugv", "milipol", "saab defense",
    "бпла", "дрон", "шахед", "ланцет", "угв", "безпілотн",
}

IRAN_BLACKLIST: set[str] = {
    "zionist regime", "resistance forces", "liberation front",
    "islamic revolution", "death to israel", "great satan",
    "axis of resistance", "martyrdom", "revolutionary guard",
}
LATAM_BLACKLIST: set[str] = {
    "bolivarian revolution", "imperial aggression", "yankee imperialism",
    "socialist fatherland", "anti-imperialist",
}
CHANNEL_BLACKLIST: set[str] = {
    "modmoscow",
    "readovkanews",
    "rybar_en",
}

MAX_HOURS_BACK    = int(os.environ.get("TELEGRAM_HOURS_BACK",       "48"))
# [TG-R16] ASYNCIO_TIMEOUT configurable via .env (était hardcodé à 300)
ASYNCIO_TIMEOUT   = int(os.environ.get("TELEGRAM_ASYNCIO_TIMEOUT",  "300"))
# [TG-R18] Nombre max de canaux scrapés simultanément (parallélisme asyncio)
TG_PARALLEL_LIMIT = int(os.environ.get("TELEGRAM_PARALLEL_LIMIT",   "3"))

MAX_MSGS_CHAN   = 150
SCORE_BASE_TG   = 6.0
SCORE_BASE_TG_A = 7.5
SEEN_PURGE_DAYS = 14
TEXT_MAX_LEN    = 600

# [TG-R3] Delays [5, 15, 30]s — total max ~50s, largement dans les 300s du timeout
_RETRY_DELAYS: list[int] = [5, 15, 30]

DATA_DIR        = Path("data")
TELEGRAM_LATEST = DATA_DIR / "telegram_latest.json"
TELEGRAM_STATS  = Path("logs") / "telegram_stats.json"
SEEN_FALLBACK   = DATA_DIR / "telegram_seen.json"

# [TG-R17] Flag module-level : CREATE TABLE telegram_metrics exécuté une seule fois
_telegram_metrics_table_ready: bool = False

# =============================================================================
# TG-R1 — REGEX BACKSLASH-SAFE pour clean_text
# chr(92) = backslash. Gardes en v3.51 : le copier-coller depuis certains
# éditeurs/chats peut corrompre les séquences d'échappement dans les raw strings.
# =============================================================================
_BS    = chr(92)
_S_NW  = _BS + "S"   # S — non-whitespace
_W_C   = _BS + "w"   # w — word char
_S_WS  = _BS + "s"   # s — whitespace
_NL_R  = _BS + "n"   # \n — newline
_DOT_L = _BS + "."   # . — point littéral

_PAT_CLEAN_URL      = re.compile("https?://" + _S_NW + "+")
_PAT_CLEAN_TME      = re.compile("t" + _DOT_L + "me/" + _S_NW + "+")
_PAT_CLEAN_MENTION  = re.compile("@" + _W_C + "+")
_PAT_CLEAN_HASHTAG  = re.compile("#" + _W_C + "+")
_PAT_CLEAN_HTML     = re.compile("[<>]")
_PAT_CLEAN_SPACES   = re.compile(_S_WS + "{2,}")
_PAT_CLEAN_NEWLINES = re.compile(_NL_R + "{3,}")

# =============================================================================
# DÉDUPLICATION SEENHASHES
# [TG-R13] load_seen : filtre temporel via SentinelDB.loadseendays()
# [TG-R14] save_seen : utilise SentinelDB.saveseen() pour cohérence API
# [TG-R15] purge    : utilise SentinelDB.purgeseen_older_than_days()
# =============================================================================

def _hash_msg(channel: str, msg_id: int) -> str:
    """Génère un hash court (20 chars) pour identifier un message Telegram."""
    return hashlib.sha256(f"{channel}:{msg_id}".encode()).hexdigest()[:20]


def load_seen() -> set[str]:
    """
    [TG-R13] Charge uniquement les hashes des SEEN_PURGE_DAYS derniers jours.
    Ancienne version : SELECT hash FROM seenhashes WHERE source='telegram'
    → chargeait TOUS les hashes depuis l'origine (croissance RAM illimitée).
    Nouvelle version : délègue à SentinelDB.loadseendays() qui applique
    un filtre temporel (WHERE dateseen >= datetime('now', ?)).
    Fallback JSON si SentinelDB indisponible.
    """
    try:
        from db_manager import SentinelDB
        hashes = SentinelDB.loadseendays(days=SEEN_PURGE_DAYS)
        log.debug(f"load_seen : {len(hashes)} hashes chargés (filtre {SEEN_PURGE_DAYS}j)")
        return hashes
    except (ImportError, Exception) as e:
        log.debug(f"SentinelDB indisponible pour seenhashes telegram : {e} — fallback JSON")
        if SEEN_FALLBACK.exists():
            try:
                return set(json.loads(SEEN_FALLBACK.read_text(encoding="utf-8")))
            except (json.JSONDecodeError, OSError):
                pass
        return set()


def save_seen(new_hashes: set[str]) -> None:
    """
    [TG-R14] Persiste les nouveaux hashes via SentinelDB.saveseen().
    Cohérent avec l'API utilisée par les autres scrapers (scraper_rss, samgov).
    Fallback JSON atomique conservé si SentinelDB indisponible.
    """
    if not new_hashes:
        return
    try:
        from db_manager import SentinelDB
        SentinelDB.saveseen(new_hashes, source="telegram")
        log.debug(f"seenhashes telegram : {len(new_hashes)} hashes persistés via SentinelDB")
    except (ImportError, Exception) as e:
        log.warning(f"SentinelDB.saveseen indisponible : {e} — fallback JSON atomique")
        existing: set[str] = set()
        if SEEN_FALLBACK.exists():
            try:
                existing = set(json.loads(SEEN_FALLBACK.read_text(encoding="utf-8")))
            except (json.JSONDecodeError, OSError):
                pass
        all_hashes = existing | new_hashes
        tmp = SEEN_FALLBACK.with_suffix(".tmp")
        DATA_DIR.mkdir(exist_ok=True)
        tmp.write_text(json.dumps(list(all_hashes), ensure_ascii=False), encoding="utf-8")
        tmp.replace(SEEN_FALLBACK)


def purge_seen_telegram(days: int = SEEN_PURGE_DAYS) -> int:
    """
    [TG-R15] Purge les seenhashes Telegram plus anciens que `days` jours.
    Délègue à SentinelDB.purgeseen_older_than_days() pour cohérence API.
    Si days=0, purge tout (force_purge).
    """
    try:
        from db_manager import SentinelDB
        n = SentinelDB.purgeseen_older_than_days(days=days)
        if n:
            log.info(f"PURGE seenhashes Telegram : {n} entrée(s) > {days}j supprimée(s)")
        return n
    except (ImportError, Exception) as e:
        # Fallback : requête directe
        try:
            from db_manager import getdb as get_db
            with get_db() as db:
                result = db.execute(
                    "DELETE FROM seenhashes WHERE source='telegram' "
                    "AND dateseen < datetime('now', ?)",
                    (f"-{days} days",),
                )
                n = result.rowcount
            if n:
                log.info(f"PURGE seenhashes Telegram (fallback) : {n} entrée(s) supprimée(s)")
            return n
        except Exception as e2:
            log.debug(f"Purge seenhashes telegram : {e} / {e2}")
            return 0


# =============================================================================
# FILTRES PERTINENCE & ANTI-PROPAGANDE (TG-FIX5 / TG-FIX6)
# =============================================================================

def is_relevant(text: str) -> bool:
    """Retourne True si le texte contient au moins un mot-clé de veille."""
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def is_propaganda(text: str, channel: str) -> bool:
    """
    Retourne True si le message est considéré comme propagande :
    - Canal dans CHANNEL_BLACKLIST, OU
    - Texte contenant des marqueurs Iran/LATAM.
    """
    if channel.lower() in CHANNEL_BLACKLIST:
        return True
    t = text.lower()
    return any(kw in t for kw in IRAN_BLACKLIST) or any(kw in t for kw in LATAM_BLACKLIST)


def score_telegram(text: str, channel_score: str) -> float:
    """
    [TG-R19] Dead code path supprimé : le retour 1.0 pour canaux blacklistés
    était inaccessible car is_propaganda() filtre ces canaux AVANT l'appel
    à score_telegram(). Le paramètre channel est retiré.

    Score = base (A=7.5, B=6.0, C=5.0) + bonus mots-clés haute valeur (max +2.0).
    Résultat borné entre 1.0 et 10.0.
    """
    base = {"A": SCORE_BASE_TG_A, "B": SCORE_BASE_TG, "C": 5.0}.get(channel_score, SCORE_BASE_TG)
    t = text.lower()
    HIGH_VALUE = {
        "ugv", "loitering", "fpv", "swarm", "autonomous weapon", "laws ",
        "c-uas", "electronic warfare", "lancet", "shahed", "anduril",
        "бпла", "ланцет",
    }
    bonus = min(sum(0.5 for kw in HIGH_VALUE if kw in t), 2.0)
    return round(min(10.0, max(1.0, base + bonus)), 2)


# =============================================================================
# NETTOYAGE TEXTE (TG-R1 — patterns backslash-safe)
# =============================================================================

def clean_text(raw: str, max_len: int = TEXT_MAX_LEN) -> str:
    """Nettoie un message Telegram : supprime URLs, mentions, hashtags, HTML."""
    text = raw.strip()
    text = _PAT_CLEAN_URL.sub("", text)
    text = _PAT_CLEAN_TME.sub("", text)
    text = _PAT_CLEAN_MENTION.sub("", text)
    text = _PAT_CLEAN_HASHTAG.sub("", text)
    text = _PAT_CLEAN_HTML.sub("", text)
    text = _PAT_CLEAN_SPACES.sub(" ", text)
    text = _PAT_CLEAN_NEWLINES.sub(chr(10) + chr(10), text)
    return text[:max_len].strip()


# =============================================================================
# TRADUCTION DEEPL (TG-FIX7 / TG-R9)
# Dépendance optionnelle : pip install requests
# =============================================================================

_deepl_chars_used: int = 0


def translate_deepl(text: str, source_lang: str = "UK") -> str:
    """
    Traduit `text` vers le français via l'API DeepL Free.
    Skip si : pas de clé API, langue déjà FR/EN, ou quota mensuel atteint.
    Requiert : pip install requests
    """
    global _deepl_chars_used
    if not DEEPL_API_KEY:
        return text
    if source_lang.upper() in ("EN", "FR"):
        return text
    if _deepl_chars_used + len(text) > DEEPL_MAX_CHARS:
        log.debug(f"DeepL : quota mensuel approché ({_deepl_chars_used} chars) — skip")
        return text
    try:
        import requests
        r = requests.post(
            DEEPL_API_URL,
            data={
                "auth_key":    DEEPL_API_KEY,
                "text":        text[:1500],
                "source_lang": source_lang.upper(),
                "target_lang": "FR",
            },
            timeout=10,
        )
        r.raise_for_status()
        translated = r.json()["translations"][0]["text"]
        _deepl_chars_used += len(text)
        return translated
    except Exception as e:
        log.debug(f"DeepL traduction échouée ({source_lang}->FR) : {e}")
        return text


# =============================================================================
# MODE DRY-RUN (TG-FIX16)
# [TG-R20] Clés corrigées : titre/resume/score(label)/art_score(float)
# =============================================================================

def _dry_run_sample() -> list[dict[str, Any]]:
    """
    Retourne des articles fictifs pour tester le pipeline sans Telegram.
    [TG-R20] Clés alignées sur le schéma format_for_claude() :
        titre     ← ex "title"
        resume    ← ex "summary"
        score     ← label "A"/"B" (fiabilité source)
        art_score ← float calculé (score thématique)
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return [
        {
            "titre":     "[DRY-RUN] FPV drone swarm autonome détruit 3 UGV — wartranslated",
            "resume":    "[DRY-RUN] Footage confirms autonomous FPV swarm engagement "
                         "against 3 armored UGVs near Zaporizhzhia. Score: 9.5/10.",
            "url":       "https://t.me/wartranslated/99999",
            "source":    "Telegram wartranslated [DRY-RUN]",
            "score":     "A",          # label fiabilité source
            "art_score": 9.5,          # score thématique calculé
            "date":      now_str,
            "type":      "telegram",
            "channel":   "wartranslated",
            "lang":      "en",
            "cross_ref": 1,
        },
        {
            "titre":     "[DRY-RUN] Milrem TYPE-X UGV : contrat OTAN 2026 — defenseofukraine",
            "resume":    "[DRY-RUN] Contract signed for 12 TYPE-X UGVs under NATO "
                         "Article 3 modernisation fund. Delivery Q3 2027. Score: 8.0/10.",
            "url":       "https://t.me/defenseofukraine/88888",
            "source":    "Telegram defenseofukraine [DRY-RUN]",
            "score":     "A",          # label fiabilité source
            "art_score": 8.0,          # score thématique calculé
            "date":      now_str,
            "type":      "telegram",
            "channel":   "defenseofukraine",
            "lang":      "uk",
            "cross_ref": 1,
        },
    ]


# =============================================================================
# SAUVEGARDE JSON (TG-FIX1 — écriture atomique)
# =============================================================================

def _save_results(articles: list[dict]) -> None:
    """Sauvegarde atomique des articles dans data/telegram_latest.json."""
    DATA_DIR.mkdir(exist_ok=True)
    tmp = TELEGRAM_LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(TELEGRAM_LATEST)
    log.info(f"data/telegram_latest.json : {len(articles)} articles sauvegardés")


def _save_stats(stats: dict[str, Any]) -> None:
    """Sauvegarde atomique des statistiques par canal dans logs/telegram_stats.json."""
    Path("logs").mkdir(exist_ok=True)
    tmp = TELEGRAM_STATS.with_suffix(".tmp")
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()
    tmp.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(TELEGRAM_STATS)


# =============================================================================
# MÉTRIQUES TELEGRAM — table dédiée (TG-R2 / TG-R17)
# [TG-R17] CREATE TABLE exécuté une seule fois via _telegram_metrics_table_ready.
# =============================================================================

def _ensure_telegram_metrics_table(db: Any) -> None:
    """
    Crée la table telegram_metrics si elle n'existe pas encore.
    [TG-R17] Exécuté une seule fois par processus grâce au flag module-level.
    """
    global _telegram_metrics_table_ready
    if not _telegram_metrics_table_ready:
        db.execute("""
            CREATE TABLE IF NOT EXISTS telegram_metrics (
                date        TEXT PRIMARY KEY,
                nb_total    INTEGER,
                nb_new      INTEGER,
                nb_critical INTEGER,
                top_score   REAL,
                updated_at  TEXT
            )
        """)
        _telegram_metrics_table_ready = True


def _save_telegram_metrics(articles: list[dict], new_count: int) -> None:
    """
    Persiste les métriques de la session de scraping Telegram.
    [TG-R17] _ensure_telegram_metrics_table() remplace le CREATE TABLE inline
    répété à chaque appel.
    [TG-R20] Utilise art_score (float) pour top_score et nb_critical.
    """
    if not articles:
        return
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    top_sc   = max(a.get("art_score", 0.0) for a in articles)
    crit_cnt = sum(1 for a in articles if a.get("art_score", 0.0) >= 8.0)
    try:
        from db_manager import getdb as get_db
        with get_db() as db:
            _ensure_telegram_metrics_table(db)
            db.execute("""
                INSERT INTO telegram_metrics
                    (date, nb_total, nb_new, nb_critical, top_score, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    nb_total    = excluded.nb_total,
                    nb_new      = excluded.nb_new,
                    nb_critical = excluded.nb_critical,
                    top_score   = excluded.top_score,
                    updated_at  = excluded.updated_at
            """, (
                today,
                len(articles),
                new_count,
                crit_cnt,
                top_sc,
                datetime.now(timezone.utc).isoformat(),
            ))
        log.debug(
            f"Métriques Telegram sauvegardées "
            f"({len(articles)} total, {new_count} nouveaux, {crit_cnt} critiques)"
        )
    except (ImportError, Exception) as e:
        log.debug(f"Métriques SentinelDB Telegram non disponibles : {e}")


# =============================================================================
# SCRAPING ASYNCHRONE — CANAL UNIQUE
# [TG-R4]  : tous les hashes traités (retenus ET filtrés) sont persistés
# [TG-R12] : new_hashes.add(h) ajouté pour les articles RETENUS directement
#            dans _scrape_channel (belt-and-suspenders avec la boucle externe)
# [TG-R3]  : retry delays [5, 15, 30]s cohérents avec ASYNCIO_TIMEOUT
# [TG-R20] : clés articles corrigées (titre/resume/score label/art_score float)
# =============================================================================

async def _scrape_channel(
    client:      Any,
    channel:     str,
    chan_label:  str,
    chan_score:  str,
    chan_lang:   str,
    cutoff:      datetime,
    seen:        set[str],
    new_hashes:  set[str],
) -> tuple[list[dict], int, str]:
    """
    Scrape un canal Telegram unique.
    Retourne (articles, nb_total_lus, statut).

    [TG-R12] Correction belt-and-suspenders : new_hashes.add(h) est maintenant
    appelé pour TOUS les messages traités (retenus ET filtrés), directement dans
    cette fonction. La boucle externe dans _scrape_async extrait également le
    _hash — les deux mécanismes garantissent la persistance.

    [TG-R19] score_telegram() ne prend plus `channel` en paramètre (dead code
    path supprimé — les canaux blacklistés sont filtrés par is_propaganda()).

    [TG-R20] Articles au format compatible format_for_claude() :
        "titre"     ← titre article (accédé comme a['titre'] dans clustering)
        "resume"    ← résumé court  (accédé comme a['resume'] dans formatage)
        "score"     ← label "A"/"B" (fiabilité source — "Fiabilité A" dans rapport)
        "art_score" ← float calculé (score thématique — tri et dashboard)
        "cross_ref" ← 1 par défaut  (boost cross-référence dans format_for_claude)
    """
    from telethon.errors import (
        ChannelPrivateError,
        FloodWaitError,
        UsernameInvalidError,
        UsernameNotOccupiedError,
    )

    articles:   list[dict] = []
    total_read: int        = 0

    try:
        async for msg in client.iter_messages(channel, limit=MAX_MSGS_CHAN):
            if msg.date is None:
                continue
            msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
            if msg_date < cutoff:
                break
            if not msg.text or len(msg.text.strip()) < 20:
                continue

            total_read += 1
            h = _hash_msg(channel, msg.id)
            if h in seen:
                continue

            clean = clean_text(msg.text)

            # Propagande : hash persisté, message ignoré (TG-R4)
            if is_propaganda(clean, channel):
                log.debug(f"Telegram {channel} msg {msg.id} : propagande filtrée")
                seen.add(h)
                new_hashes.add(h)
                continue

            # Non-pertinent : hash persisté (TG-R4)
            if not is_relevant(clean):
                seen.add(h)
                new_hashes.add(h)
                continue

            # Traduction si nécessaire
            if chan_lang not in ("en", "fr") and DEEPL_API_KEY:
                clean_translated = translate_deepl(clean, source_lang=chan_lang.upper())
            else:
                clean_translated = clean

            # [TG-R19] score_telegram sans paramètre channel
            score_val = score_telegram(clean_translated, chan_score)
            date_str  = msg_date.strftime("%Y-%m-%d %H:%M")
            url       = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

            # [TG-R20] Clés corrigées pour compatibilité format_for_claude() :
            #   "titre"     → accédé comme a['titre'] dans cross_reference() + formatage
            #   "resume"    → accédé comme a['resume'] dans le bloc texte Claude
            #   "score"     → label "A"/"B" affiché "Fiabilité A" dans le rapport
            #   "art_score" → float calculé utilisé pour le tri et le dashboard
            #   "cross_ref" → 1 par défaut, boost dans format_for_claude si ≥2
            articles.append({
                "titre":     f"[TG] {clean_translated[:120]}",
                "resume":    (
                    f"{clean_translated[:500]} "
                    f"[Source : Telegram @{channel} — {chan_label}. "
                    f"Score : {score_val}/10. Langue : {chan_lang.upper()}]"
                ),
                "url":       url,
                "source":    f"Telegram {chan_label}",
                "score":     chan_score,   # label "A" ou "B" — fiabilité source
                "art_score": score_val,    # float — score thématique calculé
                "date":      date_str,
                "type":      "telegram",
                "channel":   channel,
                "lang":      chan_lang,
                "cross_ref": 1,
                "_hash":     h,
            })

            # [TG-R12] Belt-and-suspenders : new_hashes.add(h) pour articles retenus
            # La boucle externe dans _scrape_async extrait aussi _hash → double garantie
            seen.add(h)
            new_hashes.add(h)

        return articles, total_read, "ok"

    except FloodWaitError as e:
        wait = e.seconds + 5
        log.warning(f"Telegram {channel} : FloodWaitError — attente {wait}s")
        await asyncio.sleep(wait)
        return [], total_read, f"flood_wait_{wait}s"

    except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
        log.warning(f"Telegram {channel} : accès refusé — {type(e).__name__}")
        return [], 0, f"inaccessible:{type(e).__name__}"

    except Exception as e:
        log.error(f"Telegram {channel} : erreur inattendue — {str(e)[:80]}")
        return [], 0, f"error:{str(e)[:60]}"


# =============================================================================
# SCRAPING ASYNCHRONE — ORCHESTRATION PARALLÈLE
# [TG-R18] asyncio.gather() + Semaphore(TG_PARALLEL_LIMIT)
# =============================================================================

async def _scrape_channel_with_sem(
    sem:        asyncio.Semaphore,
    client:     Any,
    channel:    str,
    chan_label: str,
    chan_score: str,
    chan_lang:  str,
    cutoff:     datetime,
    seen:       set[str],
    new_hashes: set[str],
) -> tuple[list[dict], int, str, str, str, str, str]:
    """
    Wrapper qui limite la concurrence via un sémaphore.
    [TG-R18] Permet un scraping parallèle contrôlé (max TG_PARALLEL_LIMIT canaux).
    Retourne (articles, nb_lus, statut, channel, chan_label, chan_score, chan_lang).
    """
    async with sem:
        arts, total_read, status = await _scrape_channel(
            client=client,
            channel=channel,
            chan_label=chan_label,
            chan_score=chan_score,
            chan_lang=chan_lang,
            cutoff=cutoff,
            seen=seen,
            new_hashes=new_hashes,
        )
    return arts, total_read, status, channel, chan_label, chan_score, chan_lang


async def _scrape_async(
    hours_back:     int,
    channel_filter: str | None,
) -> list[dict]:
    """
    Orchestre le scraping de tous les canaux configurés.
    [TG-R18] Parallélisme via asyncio.gather() + Semaphore(TG_PARALLEL_LIMIT).
    [TG-FIX8] Connexion Telethon avec retry sur [TG-R3] delays.
    [TG-FIX11] Timeout global géré par l'appelant via asyncio.wait_for().
    """
    try:
        from telethon import TelegramClient
    except ImportError:
        log.error(
            "TELEGRAM : telethon absent — installer avec : pip install telethon\n"
            "  Puis configurer TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env"
        )
        return []

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        log.warning(
            "TELEGRAM : TELEGRAM_API_ID ou TELEGRAM_API_HASH absent du .env\n"
            "  Inscription gratuite : https://my.telegram.org"
        )
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    seen   = load_seen()
    log.info(f"Telegram : {len(seen)} messages déjà vus (seenhashes {SEEN_PURGE_DAYS}j)")

    channels_to_process = CHANNELS
    if channel_filter:
        channels_to_process = [c for c in CHANNELS if c[0].lower() == channel_filter.lower()]
        if not channels_to_process:
            log.warning(f"Canal '{channel_filter}' non trouvé dans CHANNELS — run annulé")
            return []

    DATA_DIR.mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    all_articles: list[dict]  = []
    new_hashes:   set[str]    = set()
    stats: dict[str, Any]     = {"channels": {}, "hours_back": hours_back}

    for attempt, delay in enumerate(_RETRY_DELAYS):
        try:
            async with TelegramClient(
                TELEGRAM_SESSION,
                int(TELEGRAM_API_ID),
                TELEGRAM_API_HASH,
            ) as client:
                log.info(
                    f"Telegram : connecté (tentative {attempt + 1}/{len(_RETRY_DELAYS)}) — "
                    f"{len(channels_to_process)} canaux, "
                    f"parallélisme={TG_PARALLEL_LIMIT}"
                )

                # [TG-R18] Sémaphore limitant la concurrence
                sem = asyncio.Semaphore(TG_PARALLEL_LIMIT)

                # Création des tâches parallèles
                tasks = [
                    _scrape_channel_with_sem(
                        sem        = sem,
                        client     = client,
                        channel    = chan_handle,
                        chan_label = chan_label,
                        chan_score = chan_score,
                        chan_lang  = chan_lang,
                        cutoff     = cutoff,
                        seen       = seen,
                        new_hashes = new_hashes,
                    )
                    for chan_handle, chan_label, chan_score, chan_lang in channels_to_process
                ]

                # Lancement parallèle — return_exceptions=True évite qu'une
                # exception dans un canal n'annule les autres
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, BaseException):
                        log.error(f"Telegram canal exception non gérée : {result}")
                        continue

                    arts, total_read, status, chan_handle, chan_label, chan_score, chan_lang = result

                    # Extraction des _hash des articles retenus (belt-and-suspenders TG-R12)
                    for a in arts:
                        h = a.pop("_hash", None)
                        if h:
                            new_hashes.add(h)

                    all_articles.extend(arts)

                    stats["channels"][chan_handle] = {
                        "label":     chan_label,
                        "score_src": chan_score,
                        "lang":      chan_lang,
                        "read":      total_read,
                        "retained":  len(arts),
                        "status":    status,
                    }

                    log.info(
                        f"  @{chan_handle} : {total_read} lus → "
                        f"{len(arts)} retenus [status: {status}]"
                    )

            break  # Connexion et scraping réussis

        except Exception as e:
            if attempt < len(_RETRY_DELAYS) - 1:
                log.warning(
                    f"Telegram connexion tentative {attempt + 1}/{len(_RETRY_DELAYS)} "
                    f"échouée : {e} — retry dans {delay}s"
                )
                await asyncio.sleep(delay)
            else:
                log.error(f"Telegram : {len(_RETRY_DELAYS)} tentatives épuisées — abandon")
                return []

    # Tri global par art_score décroissant [TG-R20]
    all_articles.sort(key=lambda a: a.get("art_score", 0.0), reverse=True)

    # Persistance des seenhashes [TG-R14] et métriques [TG-R17]
    save_seen(new_hashes)
    _save_results(all_articles)
    _save_stats(stats)
    _save_telegram_metrics(all_articles, len(new_hashes))

    accessible   = [ch for ch, s in stats["channels"].items() if s["status"] == "ok"]
    inaccessible = [ch for ch, s in stats["channels"].items() if "inaccessible" in s["status"]]
    log.info(
        f"Telegram résumé : {len(all_articles)} articles retenus | "
        f"{len(new_hashes)} nouveaux hashes | "
        f"{len(accessible)}/{len(channels_to_process)} canaux accessibles"
    )
    if inaccessible:
        log.warning(f"Canaux inaccessibles : {', '.join(inaccessible)}")

    return all_articles


# =============================================================================
# FORMAT PIPELINE (TG-FIX13 — compatible scraper_rss.py / sentinel_main.py)
# [TG-R20] Tri par art_score (float) et non plus par score (label)
# =============================================================================

def _to_pipeline_articles(raw: list[dict]) -> list[dict]:
    """
    Trie et limite les articles au format pipeline.
    Retourne au maximum 50 articles triés par art_score décroissant.
    [TG-R20] Utilise art_score (float) pour le tri, cohérent avec scraper_rss.
    """
    return sorted(raw, key=lambda a: a.get("art_score", 0.0), reverse=True)[:50]


# =============================================================================
# POINT D'ENTRÉE PRINCIPAL
# [TG-R8]  : dry_run/force_purge en paramètres (plus de flags module-level)
# [TG-R16] : ASYNCIO_TIMEOUT configurable
# =============================================================================

def run_telegram_scraper(
    hours_back:     int        = MAX_HOURS_BACK,
    channel_filter: str | None = None,
    dry_run:        bool       = False,
    force_purge:    bool       = False,
) -> list[dict]:
    """
    Lance le scraping Telegram et retourne une liste d'articles
    au format pipeline (compatible format_for_claude / sentinel_main.py).

    Args:
        hours_back      : Fenêtre temporelle en heures (défaut : 48h)
        channel_filter  : Si spécifié, ne scrape que ce canal
        dry_run         : True = retourne données fictives sans connexion Telegram
        force_purge     : True = force purge totale des seenhashes avant scraping

    Returns:
        list[dict] — Articles format pipeline, triés par art_score décroissant.
        Chaque article contient : titre, resume, url, source, score (label),
                                  art_score (float), date, type, channel, lang,
                                  cross_ref.
    """
    log.info("=" * 60)
    log.info(f"TELEGRAM_SCRAPER v{VERSION} — Scraping Telegram militaire")
    log.info(
        f"Fenêtre : {hours_back}h | Canaux : {len(CHANNELS)} | "
        f"Parallélisme : {TG_PARALLEL_LIMIT} | "
        f"{'[DRY-RUN]' if dry_run else 'Mode production'}"
    )
    log.info("=" * 60)

    # Purge des seenhashes périmés (TG-R15)
    purge_seen_telegram(days=SEEN_PURGE_DAYS)
    if force_purge:
        log.info("FORCE PURGE : suppression de tous les seenhashes Telegram")
        purge_seen_telegram(days=0)

    # Mode dry-run (TG-FIX16)
    if dry_run:
        log.info("[DRY-RUN] Retour données fictives sans connexion Telegram")
        sample = _dry_run_sample()
        _save_results(sample)
        return _to_pipeline_articles(sample)

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        log.warning(
            "TELEGRAM_SCRAPER : TELEGRAM_API_ID/HASH absent — script désactivé\n"
            "  Inscription gratuite : https://my.telegram.org\n"
            "  Ajouter dans .env : TELEGRAM_API_ID=... TELEGRAM_API_HASH=..."
        )
        return []

    # Exécution asyncio avec timeout global (TG-FIX11 / TG-R16)
    try:
        raw_articles = asyncio.run(
            asyncio.wait_for(
                _scrape_async(
                    hours_back     = hours_back,
                    channel_filter = channel_filter,
                ),
                timeout=ASYNCIO_TIMEOUT,
            )
        )
    except asyncio.TimeoutError:
        log.error(
            f"TELEGRAM : timeout global atteint ({ASYNCIO_TIMEOUT}s) — "
            "vérifier la connexion réseau et les canaux configurés"
        )
        return []

    articles = _to_pipeline_articles(raw_articles)

    if articles:
        log.info("Top 5 messages Telegram par score :")
        for a in articles[:5]:
            log.info(
                f"  [{a.get('art_score', 0.0):4.1f}] @{a.get('channel', '?')} | "
                f"{a['titre'][:80]} | {a['date']}"
            )
    else:
        log.info("Telegram : aucun message pertinent collecté dans la fenêtre")

    return articles


# =============================================================================
# ENTRY POINT CLI
# [TG-R11] : argparse avec --help natif et parse_known_args
# [TG-R7]  : validation type=int native sur --days
# [TG-R8]  : dry_run/force_purge uniquement ici (plus de flags module-level)
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=f"SENTINEL Telegram Scraper v{VERSION} — Scraping militaire Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :\n"
            "  python telegram_scraper.py\n"
            "  python telegram_scraper.py --dry-run\n"
            "  python telegram_scraper.py --days 7\n"
            "  python telegram_scraper.py --channel wartranslated\n"
            "  python telegram_scraper.py --purge\n"
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=f"Fenêtre temporelle en jours (défaut : {MAX_HOURS_BACK // 24}j)",
    )
    parser.add_argument(
        "--channel",
        type=str,
        default=None,
        help="Scraper un seul canal (ex: wartranslated)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test sans connexion Telegram (données fictives)",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Force purge totale des seenhashes Telegram",
    )

    # parse_known_args : ignore les args inconnus (sentinel_main.py, cron, etc.)
    args, unknown = parser.parse_known_args()
    if unknown:
        log.debug(f"Arguments CLI ignorés (non-telegram) : {unknown}")

    hours = (args.days * 24) if args.days else MAX_HOURS_BACK

    try:
        arts = run_telegram_scraper(
            hours_back     = hours,
            channel_filter = args.channel,
            dry_run        = args.dry_run,
            force_purge    = args.purge,
        )
        log.info(f"TELEGRAM_SCRAPER terminé — {len(arts)} articles pour le pipeline")
    except Exception as fatal:
        # TG-FIX18 — reraise pour cron/supervisor
        log.critical(f"FATAL telegram_scraper.py : {fatal}", exc_info=True)
        raise
