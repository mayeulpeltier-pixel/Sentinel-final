#!/usr/bin/env python3
# github_scraper.py — SENTINEL v3.45 — Surveillance GitHub acteurs privés défense
# =============================================================================
# CHANGELOG v3.41 :
# [GH-41-BUG1] _REPO_EXISTS_CACHE déclaré avant _check_repo_exists()
# [GH-41-BUG2] global DAYS_BACK supprimé — thread-safe
# [GH-41-BUG3] 403 : retry 1× au lieu de sleep+abandon
# [GH-41-FIX1] Champ "score" lettre supprimé — seul art_score float conservé
# [GH-41-FIX2] Double appel GET /repos/{owner}/{repo} éliminé
# [GH-41-FIX3] User-Agent mis à jour SENTINEL/3.41
# [GH-41-OPT1] ThreadPoolExecutor(max_workers=3) + rate limiter thread-safe
#
# CHANGELOG v3.42 :
# [GH-42-A]  X-RateLimit-Reset header : sleep précis au lieu de sleep(60)
# [GH-42-B]  Keyword boost commits : KEYWORDS_TECHNIQUES + _keyword_boost()
#            Appliqué sur les 5 derniers messages. Cap +2.0.
# [GH-42-D]  Star spike detection : _check_star_spike() lit/écrit SentinelDB
#            Seuil atteint → bonus art_score +1.5
#
# CHANGELOG v3.43 :
# [GH-43-E]  Seuils adaptatifs dans _check_star_spike() via _adaptive_threshold()
#            Double condition : growth >= threshold ET delta >= min_absolute
#            Paliers : ≥5000→4%+50abs, ≥2000→6%+30abs,
#                      ≥500→10%+20abs, ≥100→20%+10abs, <100→40%+5abs
#
# CHANGELOG v3.44 :
# [GH-44-F]  Bonus combo mots-clés : KEYWORD_COMBOS + _combo_boost()
#            Détecte les associations offensives sur l'ensemble de la série.
#            Signal qualitatif : lidar+targeting ≠ lidar seul + targeting seul.
#            Cap +1.5. Label combo injecté dans le résumé Claude.
#
# CHANGELOG v3.45 :
# [GH-45-G]  Exclusion commits documentaires du combo boost :
#            _DOC_PATTERNS + _is_doc_commit()
#
#            Problème résolu :
#              "Update README.md — lidar targeting section" contient lidar+targeting
#              mais c'est de la documentation, pas du code de guidage. En v3.44,
#              _combo_boost() se déclenchait à tort → faux positif offensif.
#
#            Architecture :
#              _keyword_boost() : tous les messages (signal faible doc conservé)
#              _combo_boost()   : UNIQUEMENT les commits code (kw_found_code)
#              kw_found_all     : pour le résumé Claude (affichage complet)
#
#            Cas limites couverts :
#              "add lidar driver" + "fix targeting bug"     → combo ✅ (code)
#              "Update README — lidar targeting section"    → pas de combo ✅
#              "Update README" + "add targeting module"     → combo ✅
#                  (targeting vient du commit code — correct)
#              100% commits doc avec tous les mots-clés     → pas de combo ✅
#
#            _DOC_PATTERNS (15 patterns) :
#              readme, changelog, docs/, doc/, typo, fix typo, update doc,
#              documentation, .md, comment, docstring, licence, license,
#              contributing, .rst
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("sentinel.github")

# ─── Configuration ───────────────────────────────────────────────────────────
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_API    = "https://api.github.com"
DAYS_BACK_CFG = int(os.environ.get("GITHUB_DAYS_BACK", "7"))

# ─── Mots-clés sensibles défense / robotique — [GH-42-B] ────────────────────
KEYWORDS_TECHNIQUES: dict[str, float] = {
    # Autonomie & navigation
    "autonomy":     1.0,
    "navigation":   0.8,
    "waypoint":     0.8,
    "pathfinding":  0.8,
    "localization": 0.7,
    # Contrôle & actionneurs
    "control":      0.8,
    "arm":          0.9,
    "driver":       0.9,
    "actuator":     0.8,
    "servo":        0.7,
    # Capteurs & vision
    "sensor":       0.7,
    "lidar":        1.0,
    "camera":       0.7,
    "perception":   1.0,
    "detection":    0.8,
    # Systèmes critiques
    "mission":      0.9,
    "payload":      1.0,
    "guidance":     1.0,
    "targeting":    1.2,
    "weapon":       1.5,
    "encryption":   1.0,
    "auth":         0.8,
    "api":          0.6,
    "sdk":          0.6,
}

# ─── Patterns documentaires — [GH-45-G] ──────────────────────────────────────
# Conservateur : la présence d'UN seul pattern classe le commit comme doc.
# Vise les faux positifs les plus fréquents (README, changelog, typos).
# Ne pas élargir sans analyse car certains patterns sont ambigus
# (ex: "update" peut être code ou doc — délibérément absent).
_DOC_PATTERNS = frozenset({
    "readme",
    "changelog",
    "docs/",
    "doc/",
    "typo",
    "fix typo",
    "update doc",
    "documentation",
    ".md",
    "comment",
    "docstring",
    "licence",
    "license",
    "contributing",
    ".rst",
})


def _is_doc_commit(message: str) -> bool:
    """
    [GH-45-G] Retourne True si le commit est vraisemblablement documentaire.

    Critère conservateur : présence d'UN pattern _DOC_PATTERNS suffit.
    Appliqué sur le message en minuscules — insensible à la casse.

    Exemples :
        "Update README.md — lidar targeting section" → True
        "add lidar driver + targeting module"        → False
        "Fix typo in mission planner"                → True
        "fix targeting bug in guidance loop"         → False
    """
    msg_lower = message.lower()
    return any(p in msg_lower for p in _DOC_PATTERNS)


# ─── Combos offensifs — [GH-44-F] ────────────────────────────────────────────
KEYWORD_COMBOS: list[tuple[frozenset, float, str]] = [
    # ── Guidage & ciblage ────────────────────────────────────────────────────
    (frozenset({"lidar",       "targeting"}),  0.8,  "guidage actif LIDAR"),
    (frozenset({"lidar",       "weapon"}),     1.0,  "arme guidée LIDAR"),
    (frozenset({"perception",  "targeting"}),  0.8,  "ciblage par vision"),
    (frozenset({"guidance",    "payload"}),    1.0,  "vecteur guidé"),
    (frozenset({"guidance",    "targeting"}),  1.0,  "système de guidage offensif"),
    (frozenset({"navigation",  "weapon"}),     0.8,  "arme autonome naviguante"),
    # ── Autonomie offensive ──────────────────────────────────────────────────
    (frozenset({"autonomy",    "targeting"}),  0.8,  "ciblage autonome"),
    (frozenset({"autonomy",    "weapon"}),     1.2,  "système d'arme autonome"),
    (frozenset({"autonomy",    "payload"}),    0.8,  "porteur autonome"),
    (frozenset({"waypoint",    "payload"}),    0.8,  "drone porteur programmé"),
    # ── ISR & surveillance ───────────────────────────────────────────────────
    (frozenset({"sensor",      "mission"}),    0.6,  "capteur mission ISR"),
    (frozenset({"camera",      "targeting"}),  0.8,  "désignation optique"),
    (frozenset({"detection",   "targeting"}),  0.8,  "detect-and-engage"),
    (frozenset({"perception",  "mission"}),    0.6,  "système ISR embarqué"),
    # ── Frappe de précision ──────────────────────────────────────────────────
    (frozenset({"guidance",    "weapon"}),     1.2,  "frappe de précision"),
    (frozenset({"actuator",    "payload"}),    0.8,  "largage actif"),
    (frozenset({"control",     "weapon"}),     0.8,  "contrôle d'arme"),
    # ── Guerre électronique & sécurité ───────────────────────────────────────
    (frozenset({"encryption",  "auth"}),       0.4,  "sécurité renforcée"),
    (frozenset({"encryption",  "mission"}),    0.6,  "communications sécurisées mission"),
    (frozenset({"sdk",         "weapon"}),     0.8,  "SDK armes (transfert techno)"),
    (frozenset({"api",         "targeting"}),  0.6,  "API ciblage exposée"),
]

# ─── Rate limiter thread-safe — [GH-41-OPT1] ─────────────────────────────────
_rate_lock         = threading.Lock()
_last_request_time = 0.0

# ─── Cache existence repos — [GH-41-BUG1] ────────────────────────────────────
_REPO_EXISTS_CACHE: dict[str, dict | None] = {}

# ─── Dépôts surveillés ────────────────────────────────────────────────────────
REPOS_SURVEILLES: list[dict] = [
    {"owner": "anduril",          "repo": "lattice-sdk-python",       "actor": "Anduril",         "score": 8.0},
    {"owner": "anduril",          "repo": "rules_ll",                 "actor": "Anduril",         "score": 6.5},
    {"owner": "skydio",           "repo": "skydio-python-client-sdk", "actor": "Skydio",          "score": 7.0},
    {"owner": "skydio",           "repo": "skydio-skills",            "actor": "Skydio",          "score": 7.5},
    {"owner": "boston-dynamics",  "repo": "spot-sdk",                 "actor": "Boston Dynamics", "score": 8.0},
    {"owner": "boston-dynamics",  "repo": "spot-cpp-sdk",             "actor": "Boston Dynamics", "score": 7.5},
    {"owner": "palantir",         "repo": "osdk-ts",                  "actor": "Palantir",        "score": 7.0},
    {"owner": "palantir",         "repo": "gotham-sdk-python",        "actor": "Palantir",        "score": 8.0},
    {"owner": "darpa-sd2e",       "repo": "program-milestones",       "actor": "DARPA",           "score": 9.0},
    {"owner": "DARPA-ASKEM",      "repo": "hackathon",                "actor": "DARPA",           "score": 7.0},
    {"owner": "jobyaviation",     "repo": "common",                   "actor": "Joby Aviation",   "score": 6.5},
    {"owner": "PX4",              "repo": "PX4-Autopilot",            "actor": "PX4 Autopilot",   "score": 7.5},
    {"owner": "ArduPilot",        "repo": "ardupilot",                "actor": "ArduPilot",       "score": 7.0},
    {"owner": "dstacademy",       "repo": "ptt",                      "actor": "DSTA Singapore",  "score": 6.5},
]


# ─── Helpers HTTP ─────────────────────────────────────────────────────────────

def _rate_limited_get(endpoint: str, timeout: int = 10) -> Optional[dict | list]:
    """Appel API GitHub avec rate limiting thread-safe."""
    global _last_request_time
    min_interval = 1.2 if not GITHUB_TOKEN else 0.2

    with _rate_lock:
        now  = time.time()
        wait = min_interval - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.time()

    return _github_get(endpoint, timeout)


def _github_get(
    endpoint: str, timeout: int = 10, _retry: bool = True
) -> Optional[dict | list]:
    """
    Appel HTTP brut vers l'API GitHub.
    [GH-42-A] 403 : lit X-RateLimit-Reset pour un sleep précis.
    [GH-41-BUG3] retry 1× unique.
    """
    headers = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "SENTINEL/3.45",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    url = f"{GITHUB_API}{endpoint}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            remaining = resp.headers.get("X-RateLimit-Remaining", "?")
            log.debug(f"GitHub API quota restant : {remaining}")
            return json.loads(resp.read().decode("utf-8"))

    except urllib.error.HTTPError as e:
        if e.code == 404:
            log.debug(f"GitHub 404 : {endpoint}")
        elif e.code == 403:
            if _retry:
                reset_ts = int(e.headers.get("X-RateLimit-Reset", 0))
                wait     = max(1, reset_ts - int(time.time()) + 2) if reset_ts else 60
                log.warning(
                    f"GitHub 403 quota atteint — attente précise {wait}s "
                    f"(reset_ts={reset_ts}) puis retry unique"
                )
                time.sleep(wait)
                return _github_get(endpoint, timeout, _retry=False)
            else:
                log.warning(f"GitHub 403 persistant après retry : {endpoint} — abandon")
        else:
            log.warning(f"GitHub HTTP {e.code} : {endpoint}")
        return None

    except Exception as exc:
        log.warning(f"GitHub erreur : {endpoint} → {exc}")
        return None


def _cutoff_iso(days_back: int) -> str:
    """[GH-41-BUG2] days_back en paramètre — thread-safe."""
    dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Keyword boost — [GH-42-B] ───────────────────────────────────────────────

def _keyword_boost(messages: list[str]) -> float:
    """
    [GH-42-B] Bonus score sur TOUS les messages (commits code ET doc).
    Un README mentionnant "weapon" reste un signal faible légitime.
    Un seul bonus par message (break intra-message). Cap +2.0.
    """
    total = 0.0
    for msg in messages:
        msg_lower = msg.lower()
        for kw, weight in KEYWORDS_TECHNIQUES.items():
            if kw in msg_lower:
                total += weight
                break
    return min(2.0, total)


# ─── Combo boost — [GH-44-F] ─────────────────────────────────────────────────

def _combo_boost(kw_found: list[str]) -> tuple[float, list[str]]:
    """
    [GH-44-F] Bonus additionnel si plusieurs mots-clés critiques
    apparaissent ensemble dans la même série de commits.

    [GH-45-G] Appliqué UNIQUEMENT sur kw_found_code (commits non-documentaires).
    Un README "lidar+targeting" ne déclenche pas de combo offensif.

    Cap à +1.5. Plusieurs combos peuvent se cumuler jusqu'au cap.
    Retourne (bonus: float, labels_déclenchés: list[str]).
    """
    kw_set      = set(kw_found)
    total_bonus = 0.0
    triggered:  list[str] = []

    for required_kws, bonus, label in KEYWORD_COMBOS:
        if required_kws.issubset(kw_set):
            total_bonus += bonus
            triggered.append(label)

    return min(1.5, total_bonus), triggered


# ─── Seuils adaptatifs — [GH-43-E] ───────────────────────────────────────────

def _adaptive_threshold(current_stars: int) -> tuple[float, int]:
    """
    [GH-43-E] Seuil relatif + plancher absolu selon la taille du dépôt.
    Retourne (growth_threshold, min_absolute_stars).
    """
    if current_stars >= 5000:
        return 0.04, 50
    elif current_stars >= 2000:
        return 0.06, 30
    elif current_stars >= 500:
        return 0.10, 20
    elif current_stars >= 100:
        return 0.20, 10
    else:
        return 0.40, 5


# ─── Star spike detection — [GH-42-D / GH-43-E] ─────────────────────────────

def _check_star_spike(
    owner: str, repo: str, current_stars: int, days: int = 7
) -> float:
    """
    [GH-42-D → GH-43-E] Détecte une hausse soudaine de stars.
    Double condition : growth >= threshold ET delta >= min_absolute.
    Fallback silencieux si SentinelDB indisponible.
    Retourne +1.5 si spike détecté, +0.0 sinon.
    """
    try:
        from db_manager import SentinelDB
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        prev_stars = SentinelDB.get_github_stars(owner, repo, days_ago=days)
        SentinelDB.save_github_stars(owner, repo, current_stars, today)

        if prev_stars is not None and prev_stars > 0:
            delta  = current_stars - prev_stars
            growth = delta / prev_stars

            threshold, min_abs = _adaptive_threshold(current_stars)

            if growth >= threshold and delta >= min_abs:
                log.info(
                    f"STAR SPIKE {owner}/{repo} : "
                    f"{prev_stars} → {current_stars} stars "
                    f"(+{growth:.1%}, +{delta} abs, "
                    f"seuils: {threshold:.0%}/{min_abs}) — signal émergent"
                )
                return 1.5

    except Exception as exc:
        log.debug(f"_check_star_spike {owner}/{repo} indisponible : {exc}")

    return 0.0


# ─── Validation existence repo — [GH-41-FIX2] ────────────────────────────────

def _check_repo_exists(owner: str, repo: str) -> Optional[dict]:
    """
    Retourne dict (données repo) ou None.
    Données réutilisées par _fetch_repo_info() — évite un 2e appel API.
    Cache en mémoire par session.
    """
    cache_key = f"{owner}/{repo}"
    if cache_key in _REPO_EXISTS_CACHE:
        return _REPO_EXISTS_CACHE[cache_key]

    data = _rate_limited_get(f"/repos/{owner}/{repo}", timeout=8)

    if isinstance(data, dict) and not data.get("private", True):
        _REPO_EXISTS_CACHE[cache_key] = data
        return data
    else:
        _REPO_EXISTS_CACHE[cache_key] = None
        log.warning(f"GitHub: {owner}/{repo} non accessible — ignoré")
        return None


# ─── Collecte releases ────────────────────────────────────────────────────────

def _fetch_releases(
    owner: str, repo: str, actor: str, base_score: float, days_back: int
) -> list[dict]:
    """Collecte les releases récentes d'un dépôt."""
    data = _rate_limited_get(f"/repos/{owner}/{repo}/releases?per_page=10")
    if not isinstance(data, list):
        return []

    cutoff   = _cutoff_iso(days_back)
    articles = []

    for rel in data:
        published = rel.get("published_at") or rel.get("created_at", "")
        if published < cutoff:
            continue
        tag   = rel.get("tag_name", "")
        title = rel.get("name") or f"{actor} {repo} {tag}"
        body  = (rel.get("body") or "")[:300]
        url   = rel.get("html_url", f"https://github.com/{owner}/{repo}")

        articles.append({
            "titre":     f"[GITHUB RELEASE] {actor} — {title}",
            "resume":    f"Nouvelle release {tag} sur {owner}/{repo}. {body}",
            "url":       url,
            "date":      published[:10],
            "source":    f"GitHub/{actor}",
            "art_score": min(10.0, base_score + 0.5),
        })

    return articles


# ─── Collecte commits — [GH-42-B] + [GH-44-F] + [GH-45-G] ──────────────────

def _fetch_commits(
    owner: str, repo: str, actor: str, base_score: float, days_back: int
) -> list[dict]:
    """
    Collecte les commits récents sur la branche principale.

    Pipeline de scoring :
        1. _keyword_boost(messages)          → signal faible sur TOUS les messages
                                               (code + doc) — cap +2.0
        2. _is_doc_commit() par message      → classification code vs doc [GH-45-G]
        3. _combo_boost(kw_found_code)        → signal fort sur commits CODE
                                               uniquement [GH-44-F + GH-45-G]
                                               cap +1.5
        4. art_score = min(10, max(3, base-2) + kw_boost + combo_bonus)

    [GH-45-G] Séparation code/doc :
        kw_found_code → alimente _combo_boost() (combos offensifs)
        kw_found_all  → affiché dans le résumé Claude (visibilité complète)
        Si 0 commits code dans la série → combo_bonus = 0.0
    """
    since = _cutoff_iso(days_back)
    data  = _rate_limited_get(f"/repos/{owner}/{repo}/commits?since={since}&per_page=5")
    if not isinstance(data, list) or not data:
        return []

    count     = len(data)
    last_date = (data[0].get("commit", {}).get("author") or {}).get("date", "")[:10]
    url       = f"https://github.com/{owner}/{repo}/commits"

    # Collecte tous les messages de la série
    messages = [
        (c.get("commit", {}).get("message") or "")[:120].split("
")[0]
        for c in data
    ]
    last_msg = messages[0] if messages else ""

    # [GH-42-B] Boost mots-clés : TOUS les messages (code + doc)
    kw_boost = _keyword_boost(messages)

    # [GH-45-G] Séparation commits code vs doc
    code_messages = [m for m in messages if not _is_doc_commit(m)]
    doc_messages  = [m for m in messages if     _is_doc_commit(m)]

    if doc_messages:
        log.debug(
            f"GH-45-G {owner}/{repo} : "
            f"{len(doc_messages)}/{len(messages)} commits documentaires "
            f"exclus du combo boost"
        )

    # [GH-44-F + GH-45-G] Combo boost : UNIQUEMENT commits code
    kw_found_code = [
        kw for kw in KEYWORDS_TECHNIQUES
        if any(kw in m.lower() for m in code_messages)
    ]
    combo_bonus, combo_labels = _combo_boost(kw_found_code)

    # Mots-clés complets pour le résumé Claude (affichage — pas le scoring)
    kw_found_all = [
        kw for kw in KEYWORDS_TECHNIQUES
        if any(kw in m.lower() for m in messages)
    ]

    # Score final
    art_score = min(10.0, max(3.0, base_score - 2.0) + kw_boost + combo_bonus)

    # Construction du résumé enrichi
    kw_str = f" | Mots-clés : {', '.join(kw_found_all[:5])}" if kw_found_all else ""

    if combo_labels:
        doc_note = (
            f" (commits code uniquement, {len(doc_messages)} doc exclus)"
            if doc_messages else ""
        )
        combo_str = f" | ⚠️ COMBO OFFENSIF : {', '.join(combo_labels)}{doc_note}"
        log.info(
            f"COMBO {owner}/{repo} : {combo_labels} "
            f"(+{combo_bonus:.1f} score, art_score final={art_score:.1f}, "
            f"code={len(code_messages)}/{len(messages)} commits)"
        )
    else:
        combo_str = ""

    return [{
        "titre":     f"[GITHUB ACTIVITÉ] {actor}/{repo} — {count} commit(s) en {days_back}j",
        "resume":    (
            f"Activité récente sur {owner}/{repo}. "
            f"Dernier commit : {last_msg}"
            f"{kw_str}{combo_str}"
        ),
        "url":       url,
        "date":      last_date,
        "source":    f"GitHub/{actor}",
        "art_score": art_score,
    }]


# ─── Info dépôt — [GH-41-FIX2] + [GH-42-D / GH-43-E] ───────────────────────

def _fetch_repo_info(
    owner: str, repo: str, actor: str, base_score: float,
    days_back: int, repo_data: dict,
) -> list[dict]:
    """
    [GH-41-FIX2] Reçoit repo_data directement — pas de 2e appel API.
    [GH-42-D]    _check_star_spike() → bonus +1.5 si spike détecté.
    [GH-43-E]    Seuils adaptatifs dans _check_star_spike().
    """
    pushed_at = repo_data.get("pushed_at", "")[:10]
    stars     = repo_data.get("stargazers_count", 0)
    desc      = (repo_data.get("description") or "")[:200]
    cutoff    = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

    if pushed_at < cutoff:
        return []
    if stars < 50 and base_score < 8.0:
        return []

    star_boost = _check_star_spike(owner, repo, stars, days=7)
    art_score  = min(10.0, base_score + (0.5 if stars > 500 else 0.0) + star_boost)
    spike_tag  = " 🚀 STAR SPIKE" if star_boost > 0 else ""

    return [{
        "titre":     f"[GITHUB INFO] {actor}/{repo} — {stars} ⭐{spike_tag} — {pushed_at}",
        "resume":    f"{desc} | Dépôt {owner}/{repo} actif. {stars} étoiles GitHub.",
        "url":       f"https://github.com/{owner}/{repo}",
        "date":      pushed_at,
        "source":    f"GitHub/{actor}",
        "art_score": art_score,
    }]


# ─── Scraping d'un seul repo — [GH-41-OPT1] ─────────────────────────────────

def _scrape_one_repo(repo_cfg: dict, days_back: int) -> list[dict]:
    """Unité de travail pour ThreadPoolExecutor."""
    owner      = repo_cfg["owner"]
    repo       = repo_cfg["repo"]
    actor      = repo_cfg["actor"]
    base_score = repo_cfg["score"]

    repo_data = _check_repo_exists(owner, repo)
    if repo_data is None:
        return []

    log.info(f"GitHub scraping : {owner}/{repo} ({actor})")

    arts = _fetch_releases(owner, repo, actor, base_score, days_back)
    if not arts:
        arts = _fetch_commits(owner, repo, actor, base_score, days_back)

    arts += _fetch_repo_info(owner, repo, actor, base_score, days_back, repo_data)
    return arts


# ─── Point d'entrée principal ─────────────────────────────────────────────────

def run_github_scraper(days_back: int = DAYS_BACK_CFG) -> list[dict]:
    """
    Surveille les dépôts GitHub des acteurs privés défense.
    Retourne des articles au format pipeline SENTINEL (art_score float).

    v3.45 :
    [GH-42-A] X-RateLimit-Reset header — sleep précis
    [GH-42-B] Keyword boost — signal technique sur tous les commits
    [GH-42-D] Star spike detection — signal émergent communautaire
    [GH-43-E] Seuils adaptatifs — double condition growth + plancher absolu
    [GH-44-F] Combo boost — signal qualitatif multi-mots-clés offensifs
    [GH-45-G] Exclusion commits doc du combo — élimine les faux positifs README
    """
    all_articles: list[dict] = []
    seen_urls:    set[str]   = set()

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_scrape_one_repo, repo_cfg, days_back): repo_cfg
            for repo_cfg in REPOS_SURVEILLES
        }
        for future in as_completed(futures):
            repo_cfg = futures[future]
            try:
                arts = future.result()
                for a in arts:
                    if a["url"] not in seen_urls:
                        seen_urls.add(a["url"])
                        all_articles.append(a)
            except Exception as e:
                log.warning(
                    f"GitHub: erreur {repo_cfg['owner']}/{repo_cfg['repo']} → {e}"
                )

    # Sauvegarde snapshot JSON (écriture atomique)
    if all_articles:
        out = Path("data/github_scraper_latest.json")
        out.parent.mkdir(exist_ok=True)
        tmp = out.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(all_articles, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(out)
        log.info(f"GitHub : {len(all_articles)} articles sauvegardés → {out}")

    log.info(
        f"GitHub scraper terminé : {len(all_articles)} articles "
        f"depuis {len(REPOS_SURVEILLES)} dépôts ({days_back} derniers jours)"
    )
    return all_articles


# ─── Smoke test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(name)s %(message)s"
    )

    print("── Tests _is_doc_commit [GH-45-G] ──")
    doc_cases = [
        ("Update README.md — lidar targeting section", True),
        ("add lidar driver + targeting module",        False),
        ("Fix typo in mission planner",                True),
        ("fix targeting bug in guidance loop",         False),
        ("docs/architecture: add weapon diagram",      True),
        ("implement payload release actuator",         False),
    ]
    for msg, expected in doc_cases:
        result = _is_doc_commit(msg)
        status = "✅" if result == expected else "❌ ERREUR"
        print(f"  {status}  {'DOC ' if result else 'CODE'} | {msg[:60]}")

    print("
── Tests _combo_boost [GH-44-F] (sur kw_found_code) ──")
    combo_cases = [
        (["lidar", "targeting"],            "+0.8 guidage actif LIDAR"),
        (["guidance", "payload", "weapon"], "+1.5 (cap) frappe+vecteur"),
        (["auth", "encryption"],            "+0.4 sécurité"),
        (["navigation"],                    "+0.0 aucun combo"),
        ([],                                "+0.0 aucun kw code (ex: 100% doc)"),
    ]
    for kws, expected in combo_cases:
        bonus, labels = _combo_boost(kws)
        print(f"  bonus={bonus:.1f} labels={labels}  [{expected}]")

    print("
── Scraping live (si GITHUB_TOKEN défini) ──")
    articles = run_github_scraper()
    for a in sorted(articles, key=lambda x: -x["art_score"])[:10]:
        print(f"  [{a['art_score']:4.1f}] {a['titre'][:88]}")
