#!/usr/bin/env python3
# telegram_scraper.py — SENTINEL v3.40.2 — Scraping Telegram militaire
# ─────────────────────────────────────────────────────────────────────────────
# Prérequis  : pip install telethon
# Optionnel  : pip install requests          (traduction DeepL — TG-R9)
# Credentials: TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env
#              Inscription gratuite : https://my.telegram.org
#
# Corrections v3.40 : (TG-FIX1 à TG-FIX18 — voir historique)
# FIXES v3.40.1 :
#   [TG-R1]  clean_text : 5 patterns reconstruits via chr(92)
#   [TG-R2]  _save_telegram_metrics : table telegram_metrics dediee
#   [TG-R3]  Retry delays [5, 15, 30]s coherents avec ASYNCIO_TIMEOUT=300s
#   [TG-R4]  Hashes filtres (propagande/non-pertinents) persistes
#   [TG-R5]  modmoscow retire de CHANNELS (doublon CHANNEL_BLACKLIST)
#   [TG-R6]  import time supprime (dead import)
#   [TG-R7]  _cli_days : validation ValueError
#   [TG-R8]  dry_run/force_purge deplaces en parametres de run_telegram_scraper
#   [TG-R9]  requests documente comme dependance optionnelle
# FIXES v3.40.2 :
#   [TG-R10] f-strings dans tous les logs/messages (plus de str() + str())
#   [TG-R11] argparse remplace le parsing manuel sys.argv (--help natif,
#             validation de type, parse_known_args pour coexistence sentinel_main)
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python telegram_scraper.py            Scraping complet (48h)
#   python telegram_scraper.py --help     Aide complete
#   python telegram_scraper.py --dry-run  Test sans connexion Telegram
#   python telegram_scraper.py --days 7   Fenetre elargie a 7 jours
#   python telegram_scraper.py --channel wartranslated  Un seul canal
#   python telegram_scraper.py --purge    Force purge seenhashes > 14j
# ─────────────────────────────────────────────────────────────────────────────
# Sortie :
#   data/telegram_latest.json   Derniers messages collectes
#   logs/telegram_stats.json    Statistiques par canal
# ─────────────────────────────────────────────────────────────────────────────
# Integration sentinel_main.py :
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
        f"{sys.version_info.major}.{sys.version_info.minor} < 3.10
"
    )
    sys.exit(2)

# ── Logging structure (TG-FIX2) ───────────────────────────────────────────────
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

VERSION = "3.40.2"

TELEGRAM_API_ID   = os.environ.get("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION", "data/sentinel_telegram.session")

DEEPL_API_KEY   = os.environ.get("DEEPL_API_KEY", "")
DEEPL_API_URL   = "https://api-free.deepl.com/v2/translate"
DEEPL_MAX_CHARS = int(os.environ.get("DEEPL_MAX_CHARS_MONTH", "450000"))

# TG-R5 : modmoscow retire (present dans CHANNEL_BLACKLIST — connexion inutile)
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

MAX_HOURS_BACK   = int(os.environ.get("TELEGRAM_HOURS_BACK", "48"))
MAX_MSGS_CHAN    = 150
SCORE_BASE_TG    = 6.0
SCORE_BASE_TG_A  = 7.5
INTER_CHAN_DELAY  = 0.5
ASYNCIO_TIMEOUT  = 300
SEEN_PURGE_DAYS  = 14
TEXT_MAX_LEN     = 600

# TG-R3 : delays [5, 15, 30]s — total max ~50s de sleep avant abandon,
# largement dans les 300s du timeout global (etait 30/90/270s = 390s)
_RETRY_DELAYS: list[int] = [5, 15, 30]

DATA_DIR        = Path("data")
TELEGRAM_LATEST = DATA_DIR / "telegram_latest.json"
TELEGRAM_STATS  = Path("logs") / "telegram_stats.json"
SEEN_FALLBACK   = DATA_DIR / "telegram_seen.json"


# =============================================================================
# TG-R1 — REGEX BACKSLASH-SAFE pour clean_text
# chr(92)=  S=non-whitespace  w=word  s=whitespace  n=newline  .=point litteral
# Gardes meme en v3.40.2 : le workflow copier-coller depuis chat casse les \
# Les f-strings (TG-R10) sont sures car elles ne contiennent pas de backslash
# =============================================================================
_BS    = chr(92)
_S_NW  = _BS + "S"
_W_C   = _BS + "w"
_S_WS  = _BS + "s"
_NL_R  = _BS + "n"
_DOT_L = _BS + "."

_PAT_CLEAN_URL      = re.compile("https?://" + _S_NW + "+")
_PAT_CLEAN_TME      = re.compile("t" + _DOT_L + "me/" + _S_NW + "+")
_PAT_CLEAN_MENTION  = re.compile("@" + _W_C + "+")
_PAT_CLEAN_HASHTAG  = re.compile("#" + _W_C + "+")
_PAT_CLEAN_HTML     = re.compile("[<>]")
_PAT_CLEAN_SPACES   = re.compile(_S_WS + "{2,}")
_PAT_CLEAN_NEWLINES = re.compile(_NL_R + "{3,}")


# =============================================================================
# DEDUPLICATION SEENHASHES (TG-FIX4 + TG-FIX14)
# =============================================================================

def _hash_msg(channel: str, msg_id: int) -> str:
    return hashlib.sha256(f"{channel}:{msg_id}".encode()).hexdigest()[:20]


def load_seen() -> set[str]:
    try:
        from db_manager import getdb as get_db
        with get_db() as db:
            rows = db.execute(
                "SELECT hash FROM seenhashes WHERE source='telegram'"
            ).fetchall()
            return {r["hash"] for r in rows}
    except (ImportError, Exception) as e:
        log.debug(f"db_manager indisponible pour seenhashes telegram : {e} — fallback JSON")
        if SEEN_FALLBACK.exists():
            try:
                return set(json.loads(SEEN_FALLBACK.read_text(encoding="utf-8")))
            except (json.JSONDecodeError, OSError):
                pass
        return set()


def save_seen(new_hashes: set[str]) -> None:
    if not new_hashes:
        return
    today = datetime.now(timezone.utc).isoformat()
    try:
        from db_manager import getdb as get_db
        with get_db() as db:
            db.executemany(
                "INSERT OR IGNORE INTO seenhashes(hash, dateseen, source) VALUES (?, ?, ?)",
                [(h, today, "telegram") for h in new_hashes],
            )
        log.debug(f"seenhashes telegram : {len(new_hashes)} hashes ajoutes")
    except (ImportError, Exception) as e:
        log.warning(f"seenhashes SQLite indisponible : {e} — fallback JSON atomique")
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
            log.info(f"TG-FIX14 : {n} seenhashes Telegram > {days}j purges")
        return n
    except (ImportError, Exception) as e:
        log.debug(f"Purge seenhashes telegram (SQLite) : {e}")
        return 0


# =============================================================================
# FILTRES PERTINENCE & ANTI-PROPAGANDE (TG-FIX5 / TG-FIX6)
# =============================================================================

def is_relevant(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def is_propaganda(text: str, channel: str) -> bool:
    if channel.lower() in CHANNEL_BLACKLIST:
        return True
    t = text.lower()
    return any(kw in t for kw in IRAN_BLACKLIST) or any(kw in t for kw in LATAM_BLACKLIST)


def score_telegram(text: str, channel_score: str, channel: str) -> float:
    if channel.lower() in CHANNEL_BLACKLIST:
        return 1.0
    base = {"A": SCORE_BASE_TG_A, "B": SCORE_BASE_TG, "C": 5.0}.get(channel_score, SCORE_BASE_TG)
    t    = text.lower()
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
# Dependance optionnelle : pip install requests
# =============================================================================

_deepl_chars_used: int = 0


def translate_deepl(text: str, source_lang: str = "UK") -> str:
    global _deepl_chars_used
    if not DEEPL_API_KEY:
        return text
    if source_lang.upper() in ("EN", "FR"):
        return text
    if _deepl_chars_used + len(text) > DEEPL_MAX_CHARS:
        log.debug(f"DeepL : quota mensuel approche ({_deepl_chars_used} chars) — skip")
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
        log.debug(f"DeepL traduction echouee ({source_lang}->FR) : {e}")
        return text


# =============================================================================
# MODE DRY-RUN (TG-FIX16)
# =============================================================================

def _dry_run_sample() -> list[dict[str, Any]]:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return [
        {
            "title":   "[DRY-RUN] FPV drone swarm autonome detruit 3 UGV — wartranslated",
            "summary": "[DRY-RUN] Footage confirms autonomous FPV swarm engagement "
                       "against 3 armored UGVs near Zaporizhzhia. Score: 9.5/10.",
            "url":     "https://t.me/wartranslated/99999",
            "source":  "Telegram wartranslated [DRY-RUN]",
            "score":   9.5,
            "date":    now_str,
            "type":    "telegram",
            "channel": "wartranslated",
            "lang":    "en",
        },
        {
            "title":   "[DRY-RUN] Milrem TYPE-X UGV : contrat OTAN 2026 — defenseofukraine",
            "summary": "[DRY-RUN] Contract signed for 12 TYPE-X UGVs under NATO "
                       "Article 3 modernisation fund. Delivery Q3 2027. Score: 8.0/10.",
            "url":     "https://t.me/defenseofukraine/88888",
            "source":  "Telegram defenseofukraine [DRY-RUN]",
            "score":   8.0,
            "date":    now_str,
            "type":    "telegram",
            "channel": "defenseofukraine",
            "lang":    "uk",
        },
    ]


# =============================================================================
# SAUVEGARDE JSON (TG-FIX1 — atomique)
# =============================================================================

def _save_results(articles: list[dict]) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    tmp = TELEGRAM_LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(TELEGRAM_LATEST)
    log.info(f"data/telegram_latest.json : {len(articles)} articles sauvegardes")


def _save_stats(stats: dict[str, Any]) -> None:
    Path("logs").mkdir(exist_ok=True)
    tmp = TELEGRAM_STATS.with_suffix(".tmp")
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()
    tmp.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(TELEGRAM_STATS)


# =============================================================================
# TG-R2 — METRIQUES : table telegram_metrics dediee
# CREATE TABLE IF NOT EXISTS — independant du schema de la table metrics principale
# Corrige : INSERT echouait silencieusement (colonnes nb_telegram* absentes de metrics)
# =============================================================================

def _save_telegram_metrics(articles: list[dict], new_count: int) -> None:
    if not articles:
        return
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    top_sc   = max(a["score"] for a in articles)
    crit_cnt = sum(1 for a in articles if a["score"] >= 8.0)
    try:
        from db_manager import getdb as get_db
        with get_db() as db:
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
        log.debug(f"Metriques Telegram sauvegardes ({len(articles)} total, {crit_cnt} critiques)")
    except (ImportError, Exception) as e:
        log.debug(f"Metriques SentinelDB Telegram non disponibles : {e}")


# =============================================================================
# SCRAPING ASYNCHRONE TELETHON
# TG-R4 : new_hashes passe par reference — hashes filtres aussi persistes
# TG-R3 : delays [5, 15, 30]s coherents avec ASYNCIO_TIMEOUT=300s
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
    TG-R4 : tous les hashes traites (retenus ET filtres) sont ajoutes
            a new_hashes pour etre persistes — evite le re-traitement
            des messages de propagande a chaque run.
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

            # Propagande : hash persiste mais message ignore (TG-R4)
            if is_propaganda(clean, channel):
                log.debug(f"Telegram {channel} msg {msg.id} : propagande filtree")
                seen.add(h)
                new_hashes.add(h)
                continue

            # Non-pertinent : hash persiste (TG-R4)
            if not is_relevant(clean):
                seen.add(h)
                new_hashes.add(h)
                continue

            if chan_lang not in ("en", "fr") and DEEPL_API_KEY:
                clean_translated = translate_deepl(clean, source_lang=chan_lang.upper())
            else:
                clean_translated = clean

            score    = score_telegram(clean, chan_score, channel)
            date_str = msg_date.strftime("%Y-%m-%d %H:%M")
            url      = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

            articles.append({
                "title":   f"[TG] {clean_translated[:120]}",
                "summary": (
                    f"{clean_translated[:500]} "
                    f"[Source : Telegram @{channel} — {chan_label}. "
                    f"Score : {score}/10. Langue : {chan_lang.upper()}]"
                ),
                "url":     url,
                "source":  f"Telegram {chan_label}",
                "score":   score,
                "date":    date_str,
                "type":    "telegram",
                "channel": channel,
                "lang":    chan_lang,
                "_hash":   h,
            })
            seen.add(h)

        return articles, total_read, "ok"

    except FloodWaitError as e:
        wait = e.seconds + 5
        log.warning(f"Telegram {channel} : FloodWaitError — attente {wait}s")
        await asyncio.sleep(wait)
        return [], total_read, f"flood_wait_{wait}s"

    except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
        log.warning(f"Telegram {channel} : acces refuse — {type(e).__name__}")
        return [], 0, f"inaccessible:{type(e).__name__}"

    except Exception as e:
        log.error(f"Telegram {channel} : erreur inattendue — {str(e)[:80]}")
        return [], 0, f"error:{str(e)[:60]}"


async def _scrape_async(
    hours_back:     int,
    channel_filter: str | None,
) -> list[dict]:
    """
    TG-FIX8  — Connexion Telethon avec retry.
    TG-R3    — Delays [5, 15, 30]s coherents avec ASYNCIO_TIMEOUT=300s.
    TG-FIX11 — Timeout global gere par l'appelant (asyncio.wait_for).
    """
    try:
        from telethon import TelegramClient
    except ImportError:
        log.error(
            "TELEGRAM : telethon absent — installer avec : pip install telethon
"
            "  Puis configurer TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env"
        )
        return []

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        log.warning(
            "TELEGRAM : TELEGRAM_API_ID ou TELEGRAM_API_HASH absent du .env
"
            "  Inscription gratuite : https://my.telegram.org"
        )
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    seen   = load_seen()
    log.info(f"Telegram : {len(seen)} messages deja vus (seenhashes)")

    channels_to_process = CHANNELS
    if channel_filter:
        channels_to_process = [c for c in CHANNELS if c[0].lower() == channel_filter.lower()]
        if not channels_to_process:
            log.warning(f"Canal '{channel_filter}' non trouve dans CHANNELS — run annule")
            return []

    DATA_DIR.mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    all_articles: list[dict] = []
    new_hashes:   set[str]   = set()
    stats: dict[str, Any]    = {"channels": {}, "hours_back": hours_back}

    for attempt, delay in enumerate(_RETRY_DELAYS):
        try:
            async with TelegramClient(
                TELEGRAM_SESSION,
                int(TELEGRAM_API_ID),
                TELEGRAM_API_HASH,
            ) as client:
                log.info(
                    f"Telegram : connecte (tentative {attempt + 1}/{len(_RETRY_DELAYS)}) — "
                    f"{len(channels_to_process)} canaux a scraper"
                )

                for chan_handle, chan_label, chan_score, chan_lang in channels_to_process:
                    log.info(f"Telegram : scraping @{chan_handle} ({chan_label}, score {chan_score})...")

                    chan_articles, total_read, status = await _scrape_channel(
                        client     = client,
                        channel    = chan_handle,
                        chan_label = chan_label,
                        chan_score = chan_score,
                        chan_lang  = chan_lang,
                        cutoff     = cutoff,
                        seen       = seen,
                        new_hashes = new_hashes,
                    )

                    for a in chan_articles:
                        h = a.pop("_hash", None)
                        if h:
                            new_hashes.add(h)

                    all_articles.extend(chan_articles)

                    stats["channels"][chan_handle] = {
                        "label":     chan_label,
                        "score_src": chan_score,
                        "lang":      chan_lang,
                        "read":      total_read,
                        "retained":  len(chan_articles),
                        "status":    status,
                    }

                    log.info(
                        f"  @{chan_handle} : {total_read} lus -> "
                        f"{len(chan_articles)} retenus [status: {status}]"
                    )
                    await asyncio.sleep(INTER_CHAN_DELAY)

            break  # Connexion reussie

        except Exception as e:
            if attempt < len(_RETRY_DELAYS) - 1:
                log.warning(
                    f"Telegram connexion tentative {attempt + 1}/{len(_RETRY_DELAYS)} "
                    f"echouee : {e} — retry dans {delay}s"
                )
                await asyncio.sleep(delay)
            else:
                log.error(f"Telegram : {len(_RETRY_DELAYS)} tentatives epuisees — abandon")
                return []

    all_articles.sort(key=lambda a: a["score"], reverse=True)

    save_seen(new_hashes)
    _save_results(all_articles)
    _save_stats(stats)
    _save_telegram_metrics(all_articles, len(new_hashes))

    accessible   = [ch for ch, s in stats["channels"].items() if s["status"] == "ok"]
    inaccessible = [ch for ch, s in stats["channels"].items() if "inaccessible" in s["status"]]
    log.info(
        f"Telegram resume : {len(all_articles)} articles retenus | "
        f"{len(new_hashes)} nouveaux | "
        f"{len(accessible)}/{len(channels_to_process)} canaux accessibles"
    )
    if inaccessible:
        log.warning(f"Canaux inaccessibles : {', '.join(inaccessible)}")

    return all_articles


# =============================================================================
# FORMAT PIPELINE (TG-FIX13 — compatible scraper_rss.py)
# =============================================================================

def _to_pipeline_articles(raw: list[dict]) -> list[dict]:
    return sorted(raw, key=lambda a: a["score"], reverse=True)[:50]


# =============================================================================
# POINT D'ENTREE PRINCIPAL
# TG-R8  : dry_run/force_purge en parametres (plus de flags module-level)
# TG-R10 : f-strings dans tous les logs
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
        hours_back      : Fenetre temporelle en heures (defaut : 48h)
        channel_filter  : Si specifie, ne scrape que ce canal
        dry_run         : True = retourne donnees fictives sans connexion
        force_purge     : True = force purge totale seenhashes avant scraping

    Returns:
        list[dict] — Articles format pipeline, tries par score decroissant.
    """
    log.info("=" * 60)
    log.info(f"TELEGRAM_SCRAPER v{VERSION} — Scraping Telegram militaire")
    log.info(
        f"Fenetre : {hours_back}h | Canaux : {len(CHANNELS)} | "
        f"{'[DRY-RUN]' if dry_run else 'Mode production'}"
    )
    log.info("=" * 60)

    # Purge seenhashes (CDC-4)
    purge_seen_telegram(days=SEEN_PURGE_DAYS)
    if force_purge:
        purge_seen_telegram(days=0)

    # Mode dry-run (TG-FIX16)
    if dry_run:
        log.info("[DRY-RUN] Retour donnees fictives sans connexion Telegram")
        sample = _dry_run_sample()
        _save_results(sample)
        return _to_pipeline_articles(sample)

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        log.warning(
            "TELEGRAM_SCRAPER : TELEGRAM_API_ID/HASH absent — script desactive
"
            "  Inscription gratuite : https://my.telegram.org
"
            "  Ajouter dans .env : TELEGRAM_API_ID=... TELEGRAM_API_HASH=..."
        )
        return []

    # Execution asyncio avec timeout global (TG-FIX11)
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
            "verifier la connexion reseau et les canaux configures"
        )
        return []

    articles = _to_pipeline_articles(raw_articles)

    if articles:
        log.info("Top 5 messages Telegram par score :")
        for a in articles[:5]:
            log.info(
                f"  [{a['score']:4.1f}] @{a.get('channel', '?')} | "
                f"{a['title'][:80]} | {a['date']}"
            )
    else:
        log.info("Telegram : aucun message pertinent collecte dans la fenetre")

    return articles


# =============================================================================
# ENTRY POINT CLI
# TG-R11 : argparse remplace le parsing manuel sys.argv
#   - --help natif genere automatiquement
#   - type=int sur --days : validation native (plus de try/except manuel TG-R7)
#   - parse_known_args() : coexiste avec les args de sentinel_main.py si importe
#   - dry_run/force_purge lus ici uniquement (TG-R8 : plus de flags module-level)
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="SENTINEL Telegram Scraper — Scraping militaire Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :
"
            "  python telegram_scraper.py
"
            "  python telegram_scraper.py --dry-run
"
            "  python telegram_scraper.py --days 7
"
            "  python telegram_scraper.py --channel wartranslated
"
            "  python telegram_scraper.py --purge
"
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=f"Fenetre temporelle en jours (defaut : {MAX_HOURS_BACK // 24}j)",
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
        help="Test sans connexion Telegram (donnees fictives)",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Force purge totale des seenhashes Telegram",
    )

    # parse_known_args : ignore les args inconnus (sentinel_main.py, cron, etc.)
    args, unknown = parser.parse_known_args()
    if unknown:
        log.debug(f"Arguments CLI ignores (non-telegram) : {unknown}")

    hours = (args.days * 24) if args.days else MAX_HOURS_BACK

    try:
        arts = run_telegram_scraper(
            hours_back     = hours,
            channel_filter = args.channel,
            dry_run        = args.dry_run,
            force_purge    = args.purge,
        )
        log.info(f"TELEGRAM_SCRAPER termine — {len(arts)} articles pour le pipeline")
    except Exception as fatal:
        # TG-FIX18 / R6A3-NEW-1 — reraised pour cron/supervisor
        log.critical(f"FATAL telegram_scraper.py : {fatal}", exc_info=True)
        raise
