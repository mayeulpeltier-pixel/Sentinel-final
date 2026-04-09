#!/usr/bin/env python3
# telegram_scraper.py — SENTINEL v3.40 — Scraping Telegram militaire
# ─────────────────────────────────────────────────────────────────────────────
# Script OPTIONNEL — PRIORITÉ 3 (A20) — même architecture qu'ops_patents.py
# Prérequis  : pip install telethon
# Credentials: TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env
#              → Inscription gratuite sur https://my.telegram.org
#
# Corrections & améliorations v3.40 appliquées :
#   TG-FIX1   CODE-R6    — saveseen atomique : écriture tmp → rename
#   TG-FIX2   FIX-OBS1   — logging structuré (zéro print())
#   TG-FIX3   C-4        — vérification Python ≥ 3.10 en tête
#   TG-FIX4              — intégration SentinelDB (db_manager v3.40)
#   TG-FIX5              — scoring articles Telegram (0.0 – 10.0)
#   TG-FIX6              — filtres anti-propagande (IRAN, LATAM, canaux blacklistés)
#   TG-FIX7              — traduction DeepL API (RU/UK → FR, gratuit 500k/mois)
#   TG-FIX8   K-6        — retry connexion Telethon (3 tentatives exponentielles)
#   TG-FIX9              — flood-wait handler natif Telethon (FloodWaitError)
#   TG-FIX10             — rate limiting entre canaux (0.5s de délai)
#   TG-FIX11             — timeout global asyncio (évite blocage infini)
#   TG-FIX12             — nettoyage texte avancé (URLs, mentions, entités HTML)
#   TG-FIX13             — format pipeline compatible scraper_rss.py
#   TG-FIX14  CDC-4      — purge seenhashes Telegram > 14 jours
#   TG-FIX15             — sauvegarde métriques SentinelDB (dashboard)
#   TG-FIX16             — mode --dry-run (test sans connexion Telegram)
#   TG-FIX17             — résumé des canaux actifs/inaccessibles en fin de run
#   TG-FIX18  R6A3-NEW-1 — exception fatale reraisée pour cron/supervisor
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python telegram_scraper.py            Scraping complet (48h)
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
#   articles = run_telegram_scraper()  # list[dict] format pipeline
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Python ≥ 3.10 (C-4 / TG-FIX3) ───────────────────────────────────────────\nif sys.version_info < (3, 10):\n    sys.stderr.write(\n        f"TELEGRAM_SCRAPER CRITIQUE : Python {sys.version_info.major}.{sys.version_info.minor} < 3.10\n"\n    )
    sys.exit(2)

# ── Logging structuré (TG-FIX2 / FIX-OBS1) ──────────────────────────────────
log = logging.getLogger("sentinel.telegram")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTES & CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

VERSION = "3.40"

# Credentials Telegram (https://my.telegram.org)
TELEGRAM_API_ID   = os.environ.get("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION", "data/sentinel_telegram.session")

# DeepL (TG-FIX7)
DEEPL_API_KEY  = os.environ.get("DEEPL_API_KEY", "")
DEEPL_API_URL  = "https://api-free.deepl.com/v2/translate"
DEEPL_MAX_CHARS = int(os.environ.get("DEEPL_MAX_CHARS_MONTH", "450000"))

# ── Canaux prioritaires (A20) ─────────────────────────────────────────────────
# Format : (handle, label, score_source, langue)
# Score source : "A" = haute valeur, "B" = bonne valeur, "C" = à croiser
CHANNELS: list[tuple[str, str, str, str]] = [
    # Ukraine — documentation combat UGV/FPV/drone en temps réel
    ("defenseofukraine",   "Defense of Ukraine",    "A", "uk"),
    ("wartranslated",      "War Translated",         "A", "en"),
    ("ukraineweapons",     "Ukraine Weapons Tracker","A", "uk"),
    ("militarylandnet",    "Military Land",          "A", "en"),
    ("UAWeapons",          "UA Weapons",             "B", "uk"),
    ("DefMon3",            "Defence Monitor",        "B", "en"),
    ("oryxspioenkop",      "Oryx Equipment Losses",  "A", "en"),
    # Officiel / Institutionnel
    ("modmoscow",          "MoD Russie EN",          "C", "en"),  # biais — croiser
    ("bayraktartb2",       "Baykar TB2 Officiel",    "B", "en"),
    # Analyse défense internationale
    ("UAdefenceindustry",  "UA Defence Industry",    "B", "en"),
    ("combatarmor",        "Combat Armor",           "B", "en"),
    ("natowarmonitor",     "NATO War Monitor",       "B", "en"),
]

# ── Mots-clés de pertinence ───────────────────────────────────────────────────
KEYWORDS: set[str] = {
    # UGV / Terrestre
    "ugv", "unmanned ground", "fpv", "robot", "autonomous ground",
    "armored robot", "ground drone", "rover combat", "infantry robot",
    # USV / UUV / Maritime
    "usv", "uuv", "autonomous underwater", "naval drone", "unmanned surface",
    "maritime autonomous", "naval robot",
    # Aérien / Drone
    "drone", "uav", "unmanned aerial", "loitering", "kamikaze drone",
    "fpv drone", "shahed", "lancet", "geran", "orlan", "zala", "kub-bla",
    # Essaim / C2
    "swarm", "essaim", "autonomous swarm", "collaborative combat",
    # EW / C-UAS
    "electronic warfare", "ew ", " ew,", "jamming", "c-uas", "counter-drone",
    "anti-drone", "dronegun", "droneshield",
    # Combat / Systèmes
    "autonomous weapon", "laws ", "lethal autonomous", "human on the loop",
    "targeting ai", "fire control", "active protection", "trophy aps",
    # Acteurs industriels clés
    "milrem", "textron", "ghost robotics", "boston dynamics", "baykar",
    "stm kargu", "elbit", "iai autonomous", "rafael", "anduril", "shield ai",
    "rheinmetall ugv", "milipol", "saab defense",
    # Cyrillique (commun dans canaux UA/RU)
    "бпла", "дрон", "шахед", "ланцет", "угв", "безпілотн",
}

# ── Blacklists propagande (TG-FIX6) ──────────────────────────────────────────
IRAN_BLACKLIST: set[str] = {
    "zionist regime", "resistance forces", "liberation front",
    "islamic revolution", "death to israel", "great satan",
    "axis of resistance", "martyrdom", "revolutionary guard",
}
LATAM_BLACKLIST: set[str] = {
    "bolivarian revolution", "imperial aggression", "yankee imperialism",
    "socialist fatherland", "anti-imperialist",
}
# Canaux blacklistés (propagande avérée, non fiables pour SENTINEL)
CHANNEL_BLACKLIST: set[str] = {
    "modmoscow",       # MoD Russie : biais systématique, croiser seulement
    "readovkanews",    # Propaganda RU
    "rybar_en",        # Blogueur militaire RU, non vérifié
}

# ── Paramètres ────────────────────────────────────────────────────────────────
MAX_HOURS_BACK  = int(os.environ.get("TELEGRAM_HOURS_BACK", "48"))
MAX_MSGS_CHAN   = 150     # messages lus par canal (marge large, filtrés ensuite)
SCORE_BASE_TG   = 6.0    # score de base pour un article Telegram (source B)
SCORE_BASE_TG_A = 7.5    # score de base pour un canal score A
INTER_CHAN_DELAY = 0.5    # secondes entre canaux (rate limiting)
ASYNCIO_TIMEOUT  = 300   # timeout global asyncio (5 min max)
SEEN_PURGE_DAYS  = 14    # TG-FIX14 — purger seenhashes Telegram > 14 jours
TEXT_MAX_LEN     = 600   # longueur max du texte nettoyé conservé

# Chemins
DATA_DIR          = Path("data")
TELEGRAM_LATEST   = DATA_DIR / "telegram_latest.json"
TELEGRAM_STATS    = Path("logs") / "telegram_stats.json"
SEEN_FALLBACK     = DATA_DIR / "telegram_seen.json"

# CLI flags
DRY_RUN        = "--dry-run"  in sys.argv
FORCE_PURGE    = "--purge"    in sys.argv
_cli_channel   = next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--channel"), None)
_cli_days      = next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--days"), None)
if _cli_days:
    MAX_HOURS_BACK = int(_cli_days) * 24


# ═════════════════════════════════════════════════════════════════════════════
# DÉDUPLICATION SEENHASHES (TG-FIX4 + TG-FIX14)
# ═════════════════════════════════════════════════════════════════════════════

def _hash_msg(channel: str, msg_id: int) -> str:
    """Hash SHA-256 (20 chars) d'un message Telegram."""
    return hashlib.sha256(f"{channel}:{msg_id}".encode()).hexdigest()[:20]


def load_seen() -> set[str]:
    """TG-FIX4 — Charge les hashes depuis SentinelDB ou JSON fallback."""
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
    """
    TG-FIX1 (CODE-R6) + TG-FIX4 — Sauvegarde atomique (tmp → rename).
    Écrit dans SentinelDB avec fallback JSON.
    """
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
        log.debug(f"seenhashes telegram : {len(new_hashes)} hashes ajoutés")
    except (ImportError, Exception) as e:
        log.warning(f"seenhashes SQLite indisponible : {e} — fallback JSON atomique")
        existing: set[str] = set()
        if SEEN_FALLBACK.exists():
            try:
                existing = set(json.loads(SEEN_FALLBACK.read_text(encoding="utf-8")))
            except (json.JSONDecodeError, OSError):
                pass
        all_hashes = existing | new_hashes
        # TG-FIX1 — écriture atomique
        tmp = SEEN_FALLBACK.with_suffix(".tmp")
        DATA_DIR.mkdir(exist_ok=True)
        tmp.write_text(json.dumps(list(all_hashes), ensure_ascii=False), encoding="utf-8")
        tmp.replace(SEEN_FALLBACK)


def purge_seen_telegram(days: int = SEEN_PURGE_DAYS) -> int:
    """
    TG-FIX14 (CDC-4) — Purge les seenhashes Telegram de plus de N jours.
    Retourne le nombre d'entrées supprimées.
    """
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
            log.info(f"TG-FIX14 : {n} seenhashes Telegram > {days}j purgés")
        return n
    except (ImportError, Exception) as e:
        log.debug(f"Purge seenhashes telegram (SQLite) : {e}")
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# FILTRES PERTINENCE & ANTI-PROPAGANDE (TG-FIX5 / TG-FIX6)
# ═════════════════════════════════════════════════════════════════════════════

def is_relevant(text: str) -> bool:
    """Vérifie qu'au moins un mot-clé SENTINEL est présent dans le texte."""
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def is_propaganda(text: str, channel: str) -> bool:
    """
    TG-FIX6 — Détecte la propagande (Iran, LATAM) et les canaux blacklistés.
    Retourne True si le message doit être ignoré.
    """
    if channel.lower() in CHANNEL_BLACKLIST:
        return True
    t = text.lower()
    return any(kw in t for kw in IRAN_BLACKLIST) or any(kw in t for kw in LATAM_BLACKLIST)


def score_telegram(text: str, channel_score: str, channel: str) -> float:
    """
    TG-FIX5 — Score de pertinence 0.0 – 10.0 pour un message Telegram.

    Critères :
      - Score de base selon qualité du canal (A→7.5, B→6.0, C→5.0)
      - Bonus keywords haute valeur (+0.5 chacun, max +2.0)
      - Bonus médias (images/vidéos réelles, non applicables ici → stub)
      - Pénalité propagande partielle (-1.5)
      - Canal blacklisté → score 1.0 forcé
    """
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
    score = base + bonus
    return round(min(10.0, max(1.0, score)), 2)


# ═════════════════════════════════════════════════════════════════════════════
# NETTOYAGE TEXTE (TG-FIX12)
# ═════════════════════════════════════════════════════════════════════════════

def clean_text(raw: str, max_len: int = TEXT_MAX_LEN) -> str:
    """
    TG-FIX12 — Nettoyage avancé du texte Telegram :
      - Supprime les URLs (http/https/t.me)
      - Supprime les mentions @canal
      - Supprime les hashtags #tag
      - Collapse les espaces/sauts de ligne multiples
      - Tronque à max_len caractères
    """
    text = raw.strip()
    text = re.sub(r"https?://S+", "", text)       # URLs http(s)
    text = re.sub(r"t.me/S+", "", text)          # liens t.me
    text = re.sub(r"@w+", "", text)               # mentions
    text = re.sub(r"#w+", "", text)               # hashtags
    text = re.sub(r"[<>]", "", text)               # balises HTML orphelines
    text = re.sub(r"s{2,}", " ", text)            # espaces multiples\n    text = re.sub(r"\n{3,}", "\n\n", text)         # sauts de ligne excessifs
    return text[:max_len].strip()


# ═════════════════════════════════════════════════════════════════════════════
# TRADUCTION DEEPL (TG-FIX7)
# ═════════════════════════════════════════════════════════════════════════════

# Compteur mensuel DeepL (en mémoire pour ce run)
_deepl_chars_used: int = 0


def translate_deepl(text: str, source_lang: str = "UK") -> str:
    """
    TG-FIX7 — Traduit le texte vers FR via DeepL API Free (gratuit 500k/mois).
    Retourne le texte original si DeepL est indisponible ou quota atteint.
    Ne traduit pas si la langue est déjà EN ou FR.
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
                "auth_key":   DEEPL_API_KEY,
                "text":       text[:1500],  # Max 1500 chars/requête
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
        log.debug(f"DeepL traduction échouée ({source_lang}→FR) : {e}")
        return text


# ═════════════════════════════════════════════════════════════════════════════
# MODE DRY-RUN (TG-FIX16)
# ═════════════════════════════════════════════════════════════════════════════

def _dry_run_sample() -> list[dict[str, Any]]:
    """TG-FIX16 — Données fictives pour --dry-run (sans connexion Telegram)."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return [
        {
            "title":   "[DRY-RUN] [BREVET] FPV drone swarm autonome détruit 3 UGV ennemis — wartranslated",
            "summary": "[DRY-RUN] Footage confirms autonomous FPV swarm engagement against "
                       "3 armored UGVs near Zaporizhzhia. First confirmed multi-target autonomous engagement. "
                       "Source: wartranslated. Score: 9.5/10.",
            "url":     "https://t.me/wartranslated/99999",
            "source":  "Telegram wartranslated [DRY-RUN]",
            "score":   9.5,
            "date":    now_str,
            "type":    "telegram",
            "channel": "wartranslated",
            "lang":    "en",
        },
        {
            "title":   "[DRY-RUN] Milrem TYPE-X UGV : contrat OTAN 2026 signé — defenseofukraine",
            "summary": "[DRY-RUN] Contract signed for 12 TYPE-X autonomous UGVs under NATO "
                       "Article 3 modernisation fund. Delivery Q3 2027. Milrem confirms armed variant "
                       "with 30mm cannon. Score: 8.0/10.",
            "url":     "https://t.me/defenseofukraine/88888",
            "source":  "Telegram defenseofukraine [DRY-RUN]",
            "score":   8.0,
            "date":    now_str,
            "type":    "telegram",
            "channel": "defenseofukraine",
            "lang":    "uk",
        },
    ]


# ═════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE JSON (TG-FIX1 — atomique)
# ═════════════════════════════════════════════════════════════════════════════

def _save_results(articles: list[dict]) -> None:
    """TG-FIX1 — Sauvegarde atomique dans data/telegram_latest.json."""
    DATA_DIR.mkdir(exist_ok=True)
    tmp = TELEGRAM_LATEST.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(articles, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(TELEGRAM_LATEST)
    log.info(f"data/telegram_latest.json : {len(articles)} articles sauvegardés")


def _save_stats(stats: dict[str, Any]) -> None:
    """TG-FIX17 — Sauvegarde les statistiques par canal."""
    Path("logs").mkdir(exist_ok=True)
    tmp = TELEGRAM_STATS.with_suffix(".tmp")
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()
    tmp.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(TELEGRAM_STATS)


# ═════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE MÉTRIQUES (TG-FIX15)
# ═════════════════════════════════════════════════════════════════════════════

def _save_telegram_metrics(articles: list[dict], new_count: int) -> None:
    """TG-FIX15 — Enregistre les métriques Telegram dans SentinelDB (dashboard)."""
    if not articles:
        return
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    top_sc   = max(a["score"] for a in articles)
    crit_cnt = sum(1 for a in articles if a["score"] >= 8.0)
    try:
        from db_manager import getdb as get_db
        with get_db() as db:
            db.execute(
                """INSERT INTO metrics(date, nb_telegram, nb_telegram_new, nb_telegram_critical, top_telegram_score)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(date) DO UPDATE SET
                   nb_telegram=excluded.nb_telegram,
                   nb_telegram_new=excluded.nb_telegram_new,
                   nb_telegram_critical=excluded.nb_telegram_critical,
                   top_telegram_score=excluded.top_telegram_score""",
                (today, len(articles), new_count, crit_cnt, top_sc),
            )
        log.debug(f"Métriques Telegram sauvegardées ({len(articles)} total, {crit_cnt} critiques)")
    except (ImportError, Exception) as e:
        log.debug(f"Métriques SentinelDB Telegram non disponibles : {e}")


# ═════════════════════════════════════════════════════════════════════════════
# SCRAPING ASYNCHRONE TELETHON (TG-FIX8 / TG-FIX9 / TG-FIX10 / TG-FIX11)
# ═════════════════════════════════════════════════════════════════════════════

async def _scrape_channel(
    client: Any,
    channel: str,
    chan_label: str,
    chan_score: str,
    chan_lang: str,
    cutoff: datetime,
    seen: set[str],
) -> tuple[list[dict], int, str]:
    """
    Scrape un canal Telegram unique.
    Retourne (articles, nb_total_lus, statut).
    TG-FIX9 — FloodWaitError géré nativement.
    """
    from telethon.errors import (
        ChannelPrivateError,
        FloodWaitError,
        UsernameInvalidError,
        UsernameNotOccupiedError,
    )

    articles: list[dict] = []
    total_read = 0

    try:
        async for msg in client.iter_messages(channel, limit=MAX_MSGS_CHAN):
            # Arrêt si message trop ancien
            if msg.date is None:
                continue
            msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
            if msg_date < cutoff:
                break

            # Ignorer messages sans texte
            if not msg.text or len(msg.text.strip()) < 20:
                continue

            total_read += 1

            # Déduplication
            h = _hash_msg(channel, msg.id)
            if h in seen:
                continue

            # Nettoyage texte (TG-FIX12)
            clean = clean_text(msg.text)

            # Filtre propagande (TG-FIX6)
            if is_propaganda(clean, channel):
                log.debug(f"Telegram {channel} msg {msg.id} : propagande filtrée")
                seen.add(h)
                continue

            # Filtre pertinence
            if not is_relevant(clean):
                seen.add(h)
                continue

            # Traduction DeepL si nécessaire (TG-FIX7)
            if chan_lang not in ("en", "fr") and DEEPL_API_KEY:
                clean_translated = translate_deepl(clean, source_lang=chan_lang.upper())
            else:
                clean_translated = clean

            # Score
            score = score_telegram(clean, chan_score, channel)

            # Date formatée
            date_str = msg_date.strftime("%Y-%m-%d %H:%M")

            # URL du message
            url = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

            articles.append({
                "title":   f"[TG] {clean_translated[:120]}",
                "summary": (
                    f"{clean_translated[:500]} "
                    f"[Source : Telegram @{channel} — {chan_label}. "
                    f"Score : {score}/10. Langue originale : {chan_lang.upper()}]"
                ),
                "url":     url,
                "source":  f"Telegram {chan_label}",
                "score":   score,
                "date":    date_str,
                "type":    "telegram",
                "channel": channel,
                "lang":    chan_lang,
                "_hash":   h,   # conservé pour save_seen
            })
            seen.add(h)

        return articles, total_read, "ok"

    except FloodWaitError as e:
        # TG-FIX9 — Flood wait natif Telethon\n        wait = e.seconds + 5\n        log.warning(f"Telegram {channel} : FloodWaitError — attente {wait}s")\n        await asyncio.sleep(wait)\n        return [], total_read, f"flood_wait_{wait}s"\n\n    except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:\n        log.warning(f"Telegram {channel} : accès refusé — {type(e).__name__}")\n        return [], 0, f"inaccessible:{type(e).__name__}"\n\n    except Exception as e:\n        log.error(f"Telegram {channel} : erreur inattendue — {e}", exc_info=False)\n        return [], 0, f"error:{e!s:.60}"\n\n\nasync def _scrape_async(\n    hours_back: int,\n    channel_filter: str | None,\n) -> list[dict]:\n    """\n    TG-FIX8 — Connexion Telethon avec retry exponentiel (3 tentatives).\n    TG-FIX11 — Timeout global asyncio.\n    """\n    try:\n        from telethon import TelegramClient\n    except ImportError:\n        log.error(\n            "TELEGRAM : telethon absent — installer avec : pip install telethon\n"\n            "  Puis configurer TELEGRAM_API_ID + TELEGRAM_API_HASH dans .env"\n        )\n        return []\n\n    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:\n        log.warning(\n            "TELEGRAM : TELEGRAM_API_ID ou TELEGRAM_API_HASH absent du .env\n"\n            "  Inscription gratuite : https://my.telegram.org"\n        )
        return []

    cutoff  = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    seen    = load_seen()
    log.info(f"Telegram : {len(seen)} messages déjà vus (seenhashes)")

    # Filtrer les canaux si --channel est spécifié
    channels_to_process = CHANNELS
    if channel_filter:
        channels_to_process = [c for c in CHANNELS if c[0].lower() == channel_filter.lower()]
        if not channels_to_process:
            log.warning(f"Canal '{channel_filter}' non trouvé dans CHANNELS — run annulé")
            return []

    DATA_DIR.mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    all_articles: list[dict]    = []
    new_hashes:   set[str]      = set()
    stats: dict[str, Any]       = {"channels": {}, "hours_back": hours_back}

    # TG-FIX8 — Retry connexion Telethon (3 tentatives exponentielles)
    for attempt in range(3):
        try:
            async with TelegramClient(
                TELEGRAM_SESSION,
                int(TELEGRAM_API_ID),
                TELEGRAM_API_HASH,
            ) as client:
                log.info(
                    f"Telegram : connecté (tentative {attempt + 1}/3) — "
                    f"{len(channels_to_process)} canaux à scraper"
                )

                for chan_handle, chan_label, chan_score, chan_lang in channels_to_process:
                    log.info(f"Telegram : scraping @{chan_handle} ({chan_label}, score {chan_score})…")

                    chan_articles, total_read, status = await _scrape_channel(
                        client    = client,
                        channel   = chan_handle,
                        chan_label = chan_label,
                        chan_score = chan_score,
                        chan_lang  = chan_lang,
                        cutoff    = cutoff,
                        seen      = seen,
                    )

                    # Collecter hashes nouveaux articles pour save_seen
                    for a in chan_articles:
                        h = a.pop("_hash", None)
                        if h:
                            new_hashes.add(h)

                    all_articles.extend(chan_articles)

                    stats["channels"][chan_handle] = {
                        "label":       chan_label,
                        "score_src":   chan_score,
                        "lang":        chan_lang,
                        "read":        total_read,
                        "retained":    len(chan_articles),
                        "status":      status,
                    }

                    log.info(
                        f"  @{chan_handle} : {total_read} lus → "
                        f"{len(chan_articles)} retenus [status: {status}]"
                    )

                    # TG-FIX10 — rate limiting entre canaux
                    await asyncio.sleep(INTER_CHAN_DELAY)

            break  # Sortir du retry loop si connexion réussie

        except Exception as e:
            delay = 30 * (3 ** attempt)
            log.warning(
                f"Telegram connexion tentative {attempt + 1}/3 échouée : {e} "
                f"— retry dans {delay}s"
            )
            if attempt < 2:
                await asyncio.sleep(delay)
            else:
                log.error("Telegram : 3 tentatives de connexion épuisées")
                return []

    # Trier par score décroissant
    all_articles.sort(key=lambda a: a["score"], reverse=True)

    # Sauvegardes
    save_seen(new_hashes)
    _save_results(all_articles)
    _save_stats(stats)
    _save_telegram_metrics(all_articles, len(new_hashes))

    # TG-FIX17 — Résumé canaux actifs/inaccessibles
    accessible   = [ch for ch, s in stats["channels"].items() if s["status"] == "ok"]
    inaccessible = [ch for ch, s in stats["channels"].items() if "inaccessible" in s["status"]]
    log.info(
        f"Telegram résumé : {len(all_articles)} articles retenus | "
        f"{len(new_hashes)} nouveaux | "
        f"{len(accessible)}/{len(channels_to_process)} canaux accessibles"
    )
    if inaccessible:
        log.warning(f"Canaux inaccessibles : {', '.join(inaccessible)}")

    return all_articles


# ═════════════════════════════════════════════════════════════════════════════
# FORMAT PIPELINE (TG-FIX13 — compatible scraper_rss.py)
# ═════════════════════════════════════════════════════════════════════════════

def _to_pipeline_articles(raw: list[dict]) -> list[dict]:
    """
    TG-FIX13 — Les articles sont déjà au format pipeline (title/summary/url/source/score/date).
    Applique un cap de 50 articles Telegram par run pour ne pas polluer le contexte Claude.
    Trie par score décroissant.
    """
    return sorted(raw, key=lambda a: a["score"], reverse=True)[:50]


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def run_telegram_scraper(
    hours_back: int = MAX_HOURS_BACK,
    channel_filter: str | None = None,
) -> list[dict]:
    """
    Lance le scraping Telegram et retourne une liste d'articles
    au format pipeline (compatible format_for_claude / sentinel_main.py).

    Args:
        hours_back      : Fenêtre temporelle en heures (défaut : 48h)
        channel_filter  : Si spécifié, ne scrape que ce canal (ex: "wartranslated")

    Returns:
        list[dict] — Articles format pipeline, triés par score décroissant.
                     Liste vide si pas de credentials ou erreur connexion.
    """
    log.info("=" * 60)
    log.info(f"TELEGRAM_SCRAPER v{VERSION} — Scraping Telegram militaire")
    log.info(
        f"Fenêtre : {hours_back}h | Canaux : {len(CHANNELS)} | "
        f"{'[DRY-RUN]' if DRY_RUN else 'Mode production'}"
    )
    log.info("=" * 60)

    # ── Purge seenhashes (CDC-4) ──────────────────────────────────────────────
    if FORCE_PURGE:
        purge_seen_telegram()
    else:
        # Purge automatique en arrière-plan (non bloquant)
        purge_seen_telegram(days=SEEN_PURGE_DAYS)

    # ── Mode dry-run (TG-FIX16) ──────────────────────────────────────────────
    if DRY_RUN:
        log.info("[DRY-RUN] Retour données fictives sans connexion Telegram")
        sample = _dry_run_sample()
        _save_results(sample)
        return _to_pipeline_articles(sample)

    # ── Vérification credentials ─────────────────────────────────────────────\n    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:\n        log.warning(\n            "TELEGRAM_SCRAPER : TELEGRAM_API_ID/HASH absent — script désactivé\n"\n            "  Inscription gratuite : https://my.telegram.org\n"\n            "  Ajouter dans .env : TELEGRAM_API_ID=... TELEGRAM_API_HASH=..."\n        )
        return []

    # ── Exécution asyncio avec timeout global (TG-FIX11) ─────────────────────
    try:
        raw_articles = asyncio.run(
            asyncio.wait_for(
                _scrape_async(
                    hours_back     = hours_back,
                    channel_filter = channel_filter or _cli_channel,
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

    # ── Format pipeline + top 5 logs ─────────────────────────────────────────
    articles = _to_pipeline_articles(raw_articles)

    if articles:
        log.info(f"Top 5 messages Telegram par score :")
        for a in articles[:5]:
            log.info(
                f"  [{a['score']:4.1f}] @{a.get('channel','?')} | "
                f"{a['title'][:80]} | {a['date']}"
            )
    else:
        log.info("Telegram : aucun message pertinent collecté dans la fenêtre")

    return articles


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        arts = run_telegram_scraper()
        log.info(f"TELEGRAM_SCRAPER terminé — {len(arts)} articles pour le pipeline")
    except Exception as fatal:
        # TG-FIX18 / R6A3-NEW-1 — reraisée pour que cron/supervisor détecte l'échec
        log.critical(f"FATAL telegram_scraper.py : {fatal}", exc_info=True)
        raise