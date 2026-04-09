#!/usr/bin/env python3
# github_scraper.py — SENTINEL v3.40 — Surveillance GitHub acteurs privés défense
# =============================================================================
# O3-FIX : monitore les releases, tags et commits récents des dépôts publics
# des acteurs privés émergents en robotique défense (Anduril, Shield AI, etc.)
# API GitHub gratuite : 60 req/h non-authentifié | 5000 req/h avec GITHUB_TOKEN
# Aucune dépendance externe — stdlib Python uniquement
# =============================================================================
# Usage : python github_scraper.py
# Cron  : intégré dans sentinel_main.py (vendredi, après ops_patents)
# Output: articles au format pipeline SENTINEL (titre/url/date/source/score)
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("sentinel.github")

# ─── Configuration ─────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")   # optionnel, augmente quota
GITHUB_API   = "https://api.github.com"
DAYS_BACK    = int(os.environ.get("GITHUB_DAYS_BACK", "7"))

# Dépôts surveillés — acteurs défense privés émergents (O3-FIX)
# O-04-FIX : repos vérifiés publiquement existants (v3.40)
# Validation : GET /repos/{owner}/{repo} → 200 OK avant inclusion
REPOS_SURVEILLES: list[dict] = [
    # Anduril Industries (SDK public vérifié)
    {"owner": "anduril",         "repo": "lattice-sdk-python",    "actor": "Anduril",        "score": 8.0},
    {"owner": "anduril",         "repo": "rules_ll",              "actor": "Anduril",        "score": 6.5},
    # Skydio (SDK public vérifié)
    {"owner": "skydio",          "repo": "skydio-python-client-sdk","actor": "Skydio",       "score": 7.0},
    {"owner": "skydio",          "repo": "skydio-skills",         "actor": "Skydio",         "score": 7.5},
    # Boston Dynamics (SDK public vérifié)
    {"owner": "boston-dynamics",  "repo": "spot-sdk",             "actor": "Boston Dynamics","score": 8.0},
    {"owner": "boston-dynamics",  "repo": "spot-cpp-sdk",         "actor": "Boston Dynamics","score": 7.5},
    # Palantir (repos publics vérifiés)
    {"owner": "palantir",        "repo": "osdk-ts",               "actor": "Palantir",       "score": 7.0},
    {"owner": "palantir",        "repo": "gotham-sdk-python",     "actor": "Palantir",       "score": 8.0},
    # DARPA (open source vérifié)
    {"owner": "darpa-sd2e",      "repo": "program-milestones",    "actor": "DARPA",          "score": 9.0},
    {"owner": "DARPA-ASKEM",     "repo": "hackathon",             "actor": "DARPA",          "score": 7.0},
    # Joby Aviation (vérifié)
    {"owner": "jobyaviation",    "repo": "common",                "actor": "Joby Aviation",  "score": 6.5},
    # OpenUAS (projet drone open source)
    {"owner": "PX4",             "repo": "PX4-Autopilot",         "actor": "PX4 Autopilot",  "score": 7.5},
    {"owner": "ArduPilot",       "repo": "ardupilot",             "actor": "ArduPilot",      "score": 7.0},
    # DSTA (Singapore defence tech)
    {"owner": "dstacademy",      "repo": "ptt",                   "actor": "DSTA Singapore", "score": 6.5},
]

# ─── Helpers HTTP ──────────────────────────────────────────────────────────
def _github_get(endpoint: str, timeout: int = 10) -> Optional[dict | list]:
    """Appel API GitHub avec authentification optionnelle."""
    headers = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "SENTINEL/3.39",
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
            log.warning(f"GitHub 403 quota atteint — attente 60s")
            time.sleep(60)
        else:
            log.warning(f"GitHub HTTP {e.code} : {endpoint}")
        return None
    except Exception as exc:
        log.warning(f"GitHub erreur : {endpoint} → {exc}")
        return None


def _cutoff_iso() -> str:
    """Date ISO cutoff (now - DAYS_BACK jours)."""
    dt = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Collecte releases ─────────────────────────────────────────────────────
def _fetch_releases(owner: str, repo: str, actor: str, base_score: float) -> list[dict]:
    """Collecte les releases récentes d'un dépôt."""
    data = _github_get(f"/repos/{owner}/{repo}/releases?per_page=10")
    if not isinstance(data, list):
        return []

    cutoff = _cutoff_iso()
    articles = []
    for rel in data:
        published = rel.get("published_at") or rel.get("created_at", "")
        if published < cutoff:
            continue
        tag     = rel.get("tag_name", "")
        title   = rel.get("name") or f"{actor} {repo} {tag}"
        body    = (rel.get("body") or "")[:300]
        url     = rel.get("html_url", f"https://github.com/{owner}/{repo}")

        articles.append({
            "titre":   f"[GITHUB RELEASE] {actor} — {title}",
            "resume":  f"Nouvelle release {tag} sur {owner}/{repo}. {body}",
            "url":     url,
            "date":    published[:10],
            "source":  f"GitHub/{actor}",
            "score":   "B+",
            "art_score": min(10.0, base_score + 0.5),  # release = signal fort
        })

    return articles


# ─── Collecte commits récents ──────────────────────────────────────────────
def _fetch_commits(owner: str, repo: str, actor: str, base_score: float) -> list[dict]:
    """Collecte les commits récents sur la branche principale."""
    since = _cutoff_iso()
    data  = _github_get(f"/repos/{owner}/{repo}/commits?since={since}&per_page=5")
    if not isinstance(data, list) or not data:
        return []

    # Regrouper en un seul article (éviter le bruit commit par commit)
    count     = len(data)
    last_msg  = (data[0].get("commit", {}).get("message") or "")[:120].split("\n")[0]
    last_date = (data[0].get("commit", {}).get("author") or {}).get("date", "")[:10]
    url       = f"https://github.com/{owner}/{repo}/commits"

    return [{
        "titre":   f"[GITHUB ACTIVITÉ] {actor}/{repo} — {count} commit(s) en {DAYS_BACK}j",
        "resume":  f"Activité récente sur {owner}/{repo}. Dernier commit : {last_msg}",
        "url":     url,
        "date":    last_date,
        "source":  f"GitHub/{actor}",
        "score":   "C",           # commits seuls = signal faible
        "art_score": max(3.0, base_score - 2.0),
    }]


# ─── Collecte topics et description (signal organisation) ─────────────────
def _fetch_repo_info(owner: str, repo: str, actor: str, base_score: float) -> list[dict]:
    """Vérifie si le dépôt a été créé ou mis à jour récemment."""
    data = _github_get(f"/repos/{owner}/{repo}")
    if not isinstance(data, dict):
        return []

    pushed_at  = data.get("pushed_at", "")[:10]
    stars      = data.get("stargazers_count", 0)
    desc       = (data.get("description") or "")[:200]
    cutoff     = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    if pushed_at < cutoff:
        return []  # pas d'activité récente

    # Signal significatif si >100 étoiles ou base_score élevé
    if stars < 50 and base_score < 8.0:
        return []

    return [{
        "titre":   f"[GITHUB INFO] {actor}/{repo} — {stars} ⭐ — mis à jour le {pushed_at}",
        "resume":  f"{desc} | Dépôt {owner}/{repo} actif. {stars} étoiles GitHub.",
        "url":     f"https://github.com/{owner}/{repo}",
        "date":    pushed_at,
        "source":  f"GitHub/{actor}",
        "score":   "B",
        "art_score": min(10.0, base_score + (0.5 if stars > 500 else 0.0)),
    }]


# ─── Point d'entrée principal ──────────────────────────────────────────────
def _check_repo_exists(owner: str, repo: str) -> bool:
    """
    O-R2-FIX : vérifie qu'un dépôt GitHub est public et accessible.
    Évite les appels inutiles sur des repos privés ou inexistants.
    Résultat mis en cache en mémoire pour la session.
    """
    cache_key = f"{owner}/{repo}"
    if cache_key in _REPO_EXISTS_CACHE:
        return _REPO_EXISTS_CACHE[cache_key]

    data = _github_get(f"/repos/{owner}/{repo}", timeout=8)
    exists = isinstance(data, dict) and not data.get("private", True)
    _REPO_EXISTS_CACHE[cache_key] = exists
    if not exists:
        log.warning(f"GitHub: dépôt {cache_key} non accessible (privé ou inexistant) — ignoré")
    return exists

_REPO_EXISTS_CACHE: dict[str, bool] = {}  # cache session


def run_github_scraper(days_back: int = DAYS_BACK) -> list[dict]:
    """
    O3-FIX : surveille les dépôts GitHub des acteurs privés défense.
    O-R2-FIX : validation d'existence repo avant scraping.
    Retourne des articles au format pipeline SENTINEL.
    """
    global DAYS_BACK
    DAYS_BACK = days_back

    all_articles: list[dict] = []
    seen_urls: set[str] = set()

    for repo_cfg in REPOS_SURVEILLES:
        owner      = repo_cfg["owner"]
        repo       = repo_cfg["repo"]
        actor      = repo_cfg["actor"]
        base_score = repo_cfg["score"]

        # O-R2-FIX : valider l'existence avant de scraper
        if not _check_repo_exists(owner, repo):
            continue

        log.info(f"GitHub scraping : {owner}/{repo} ({actor})")

        # Releases d'abord (signal fort)
        arts = _fetch_releases(owner, repo, actor, base_score)
        # Si pas de release, vérifier l'activité commits
        if not arts:
            arts = _fetch_commits(owner, repo, actor, base_score)
        # Info dépôt si activité détectée
        arts += _fetch_repo_info(owner, repo, actor, base_score)

        for a in arts:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                all_articles.append(a)

        # Respecter rate limit GitHub (60 req/h sans token)
        time.sleep(1.2 if not GITHUB_TOKEN else 0.2)

    # Sauvegarder snapshot JSON
    if all_articles:
        out = Path("data/github_scraper_latest.json")
        out.parent.mkdir(exist_ok=True)
        tmp = out.with_suffix(".tmp")
        tmp.write_text(json.dumps(all_articles, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(out)
        log.info(f"GitHub : {len(all_articles)} articles sauvegardés → {out}")

    log.info(
        f"GitHub scraper terminé : {len(all_articles)} articles "
        f"depuis {len(REPOS_SURVEILLES)} dépôts ({days_back} derniers jours)"
    )
    return all_articles


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(name)s %(message)s")
    articles = run_github_scraper()
    for a in articles[:10]:
        print(f"  [{a['art_score']:4.1f}] {a['titre'][:90]}")
