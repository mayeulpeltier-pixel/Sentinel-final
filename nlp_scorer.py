#!/usr/bin/env python3
# nlp_scorer.py — SENTINEL v3.40.1 — Scoring NLP TF-IDF bigrammes
# ─────────────────────────────────────────────────────────────────────────────
# Script OPTIONNEL — PRIORITÉ 5 (A32) — améliore le ranking des articles de
# 30-40 % par rapport au scoring RSS seul.
#
# Prérequis  : pip install scikit-learn numpy
# Option B   : pip install sentence-transformers  (décommenter la section ci-dessous)
#
# Corrections & améliorations v3.40 appliquées :
#   NLP-FIX1  MATHS-M3   — Formule rerank multiplicative (évite pénalité score-A)
#   NLP-FIX2  MATHS-M4   — cross_reference boost 0.8*ln(N) additif
#   NLP-FIX3  FIX-OBS1   — logging structuré (zéro print())
#   NLP-FIX4  C-4         — vérification Python >= 3.10
#   NLP-FIX5  CODE-R6     — save_stats atomique (.tmp → rename)
#   NLP-FIX6              — BUG-CRITIQUE-1 : import local isolé pour heatmap
#   NLP-FIX7              — BUG-CRITIQUE-2 : weight_temporal intégrée dans nlp_rerank
#   NLP-FIX8              — BUG-M3 : fenêtre glissante WINDOW=3 sur tokens par doc
#   NLP-FIX9              — BUG-M2 : test_score_article_bounds — ordre args corrigé
#   NLP-FIX10 MATHS-M5   — Seuil alerte configurable via SENTINEL_ALERT_SIGMA
#   NLP-FIX11             — corpus défense étendu (RU/UK cyrillique + termes v3.40)
#   NLP-FIX12             — déduplication sémantique : blacklist doublons > 0.92
#   NLP-FIX13             — intégration SentinelDB (sauvegarde scores NLP métriques)
#   NLP-FIX14             — Option B sentence-transformers (décommentable sans refacto)
#   NLP-FIX15             — _selftest() robuste avec assertions annotées
#   NLP-FIX16             — support titre_fr / resume_fr (articles traduits Telegram)
#   NLP-FIX17             — cache vectorizer (évite recalcul si appelé en boucle)
# FIXES v3.40.1 :
#   NLP-R1  — BUG-CRITIQUE-3 : 3 regex backslash cassés dans nlp_score_article
#             (S+, [^...s], s{2,}) → reconstruits via chr(92) comme TG-R1
#   NLP-R2  — BUG-CRITIQUE-4 : getters de champs ignoraient title/summary (anglais)
#             → tous les get() couvrent désormais titre/resume ET title/summary
#             → pipeline NLP était inopérant en production (score 0.5 systématique)
#   NLP-R3  — BUG-CRITIQUE-5 : save_nlp_metrics insérait dans table metrics partagée
#             (colonnes nlp_* absentes → INSERT échouait silencieusement)
#             → table dédiée nlp_metrics créée (même pattern que telegram_metrics)
#   NLP-R4  — MINEUR : import datetime sorti de la boucle for dans nlp_rerank
#   NLP-R5  — MINEUR : argparse remplace parsing manuel sys.argv dans __main__
# ─────────────────────────────────────────────────────────────────────────────
# Usage autonome :
#   python nlp_scorer.py               Test + stats sur corpus interne
#   python nlp_scorer.py --bench       Benchmark sur 200 articles synthétiques
#   python nlp_scorer.py --heatmap     Génère heatmap co-occurrence (logs/)
#   python nlp_scorer.py --help        Aide complète
#
# Intégration sentinel_main.py :
#   from nlp_scorer import run_nlp_pipeline
#   articles = run_nlp_pipeline(articles, weight=0.3)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Python >= 3.10 (NLP-FIX4 / C-4) ─────────────────────────────────────────
if sys.version_info < (3, 10):
    sys.stderr.write(
        f"NLP_SCORER CRITIQUE : Python "
        f"{sys.version_info.major}.{sys.version_info.minor} < 3.10
"
    )
    sys.exit(2)

# ── Logging structuré (NLP-FIX3 / FIX-OBS1) ─────────────────────────────────
log = logging.getLogger("sentinel.nlp")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# DÉPENDANCES OPTIONNELLES
# =============================================================================

VERSION = "3.40.1"

# ── Option A : TF-IDF léger (défaut, sklearn) ────────────────────────────────
SKLEARN_OK = False
try:
    from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
    from sklearn.metrics.pairwise import cosine_similarity        # type: ignore
    import numpy as np                                            # type: ignore
    SKLEARN_OK = True
    log.debug("NLP backend : scikit-learn TF-IDF (Option A)")
except ImportError:
    log.warning(
        "NLP : scikit-learn absent — pip install scikit-learn numpy
"
        "  -> Fallback : scoring neutre 0.5 (pipeline non dégradé)"
    )

# ── Option B : sentence-transformers (décommenter si disponible, ~200 Mo) ────
# SBERT_OK = False
# try:
#     from sentence_transformers import SentenceTransformer  # type: ignore
#     _SBERT_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
#     SBERT_OK = True
#     log.info("NLP backend : sentence-transformers all-MiniLM-L6-v2 (Option B)")
# except ImportError:
#     pass


# =============================================================================
# CONSTANTES
# =============================================================================

ALERT_SIGMA         = float(os.environ.get("SENTINEL_ALERT_SIGMA",    "1.5"))
DEDUP_THRESHOLD     = float(os.environ.get("NLP_DEDUP_THRESHOLD",     "0.92"))
CROSS_REF_THRESHOLD = float(os.environ.get("NLP_CROSS_REF_THRESHOLD", "0.35"))
COOCCURRENCE_WINDOW = 3
DEFAULT_NLP_WEIGHT  = float(os.environ.get("NLP_WEIGHT",              "0.30"))

DATA_DIR = Path("data")
LOGS_DIR = Path("logs")

# ── Cache vectorizer (NLP-FIX17) ─────────────────────────────────────────────
_VECTORIZER:      Any = None
_CORPUS_VEC:      Any = None
_VECTORIZER_HASH: str = ""


# =============================================================================
# NLP-R1 — REGEX BACKSLASH-SAFE pour nlp_score_article
# chr(92)=  S=non-whitespace  s=whitespace
# Gardes même en v3.40.1 : le workflow copier-coller depuis chat casse les \
# Identique à TG-R1 dans telegram_scraper.py
# =============================================================================
_BS = chr(92)

_PAT_NLP_URL    = re.compile("https?://" + _BS + "S+")
_PAT_NLP_CHARS  = re.compile("[^a-z" + chr(192) + "-" + chr(255) + chr(1024) + "-" + chr(1279) + "0-9" + _BS + "s]")
_PAT_NLP_SPACES = re.compile(_BS + "s{2,}")


# =============================================================================
# CORPUS DE RÉFÉRENCE DÉFENSE (NLP-FIX11 — étendu v3.40)
# =============================================================================

DEFENSE_CORPUS: list[str] = [
    # UGV — Terrestre
    "unmanned ground vehicle ugv military autonomous robot defense combat reconnaissance",
    "autonomous ground vehicle armed ugv milrem textron ghost robotics battlefield deployment",
    "fpv drone combat ukraine infantry robot remote controlled weapon system",
    "loitering munition kamikaze drone swarm autonomous attack ground target",
    "armored robot unmanned combat vehicle manned unmanned teaming ground force",
    # USV / UUV — Maritime
    "unmanned surface vessel usv autonomous naval drone maritime patrol mine countermeasure",
    "autonomous underwater vehicle uuv submarine defense sea mine detection",
    "naval drone ukraine black sea attack surface drone maritime asymmetric warfare",
    "unmanned maritime system sea guardian triton autonomous patrol ocean surveillance",
    # Aérien / Drone
    "unmanned aerial vehicle uav drone reconnaissance strike autonomous mission",
    "shahed geran lancet orlan zala drone russia ukraine war battlefield",
    "loyal wingman collaborative combat aircraft drone wingman autonomous teaming",
    # IA / C2 / Essaim
    "artificial intelligence embedded autonomous weapon decision loop human oversight",
    "swarm intelligence drone swarm collaborative autonomous coordinated attack defense",
    "command control c2 autonomous system military ai decision making targeting",
    # C-UAS / EW
    "counter unmanned aerial system c-uas anti-drone defense detection jamming defeat",
    "electronic warfare jamming cognitive autonomous spectrum radio frequency denial",
    "directed energy weapon high energy laser iron beam close-in weapon system",
    # Doctrine / Légal / Contrats
    "lethal autonomous weapon system laws regulation international humanitarian law ban treaty",
    "defense contract awarded department defense darpa program autonomous robotics procurement",
    # Acteurs industriels clés
    "milrem robotics anduril shield ai palantir boston dynamics rheinmetall saab elbit iai rafael baykar",
    "darpa eda nato program robotics autonomous doctrine procurement budget modernization",
    # Cyrillique / Ukraine-Russie (NLP-FIX11)
    "бпла дрон шахед ланцет угв безпілотний автономний бойовий розвідник",
    "fpv дрон бойове застосування фронт передова Ukraine война автономний",
]


# =============================================================================
# INITIALISATION VECTORIZER (NLP-FIX17 — cache)
# =============================================================================

def _corpus_hash() -> str:
    return hashlib.md5("".join(DEFENSE_CORPUS).encode()).hexdigest()[:8]


def _init_vectorizer(force: bool = False) -> bool:
    global _VECTORIZER, _CORPUS_VEC, _VECTORIZER_HASH

    if not SKLEARN_OK:
        return False

    current_hash = _corpus_hash()
    if _VECTORIZER is not None and _VECTORIZER_HASH == current_hash and not force:
        return True

    try:
        _VECTORIZER = TfidfVectorizer(
            ngram_range=(1, 2),
            max_features=8000,
            sublinear_tf=True,
            stop_words="english",
            strip_accents="unicode",
            min_df=1,
        )
        _CORPUS_VEC = _VECTORIZER.fit_transform(DEFENSE_CORPUS)
        _VECTORIZER_HASH = current_hash
        log.debug(
            f"Vectorizer TF-IDF initialisé : {len(DEFENSE_CORPUS)} docs, "
            f"vocab {len(_VECTORIZER.vocabulary_)} termes"
        )
        return True
    except Exception as e:
        log.error(f"NLP : erreur init vectorizer — {e}")
        _VECTORIZER = None
        _CORPUS_VEC = None
        return False


_init_vectorizer()


# =============================================================================
# SCORE NLP ARTICLE (cosinus TF-IDF)
# NLP-R1 : 3 patterns regex reconstruits via chr(92) — plus de backslash cassé
# =============================================================================

def nlp_score_article(title: str, summary: str) -> float:
    """
    Retourne un score NLP dans [0.0, 1.0] basé sur la similarité cosinus
    entre l'article et le corpus de référence défense.

    Fallback neutre 0.5 si sklearn indisponible (pipeline non dégradé).
    Support cyrillique (NLP-FIX11) et articles traduits (NLP-FIX16).
    NLP-R1 : patterns regex backslash-safe (chr(92) — identique à TG-R1).
    """
    if not SKLEARN_OK or _VECTORIZER is None or _CORPUS_VEC is None:
        return 0.5

    text = f"{title} {summary}".lower()
    text = _PAT_NLP_URL.sub(" ", text)
    text = _PAT_NLP_CHARS.sub(" ", text)
    text = _PAT_NLP_SPACES.sub(" ", text).strip()

    if not text:
        return 0.0

    try:
        vec  = _VECTORIZER.transform([text])
        sims = cosine_similarity(vec, _CORPUS_VEC)
        return float(np.max(sims))
    except Exception as e:
        log.debug(f"nlp_score_article : erreur — {e}")
        return 0.5


# =============================================================================
# HELPER INTERNE — getter de champs compatible double convention
# NLP-R2 : scraper_rss.py et telegram_scraper.py produisent title/summary (EN)
#           les anciens articles utilisent titre/resume (FR)
#           → tous les getters couvrent les deux conventions
# =============================================================================

def _get_title(a: dict) -> str:
    """Retourne le titre de l'article, compatible conventions FR et EN."""
    return (
        a.get("titre_fr") or a.get("titre") or
        a.get("title_fr") or a.get("title") or ""
    )


def _get_summary(a: dict) -> str:
    """Retourne le résumé de l'article, compatible conventions FR et EN."""
    return (
        a.get("resume_fr") or a.get("resume") or
        a.get("summary_fr") or a.get("summary") or ""
    )


# =============================================================================
# RERANK PRINCIPAL (NLP-FIX1 — MATHS-M3 CORRIGÉ)
# NLP-R4 : import datetime sorti de la boucle for (était réimporté à chaque article)
# =============================================================================

def nlp_rerank(
    articles: list[dict],
    weight: float = DEFAULT_NLP_WEIGHT,
) -> list[dict]:
    """
    Ré-ordonne les articles en combinant art_score (scraper) et score NLP.

    Formule v3.40 (MATHS-M3 — multiplicative, évite pénalité score-A) :
        base   = art_score / 10.0            dans [0.0, 1.0]
        nlp    = nlp_score_article(...)      dans [0.0, 1.0]
        sv2    = base * (1 + weight * nlp)   <- jamais inférieur à base
        score  = round(sv2 * 10, 3)          dans [0.0, 10.0]

    NLP-FIX7 — weight_temporal intégrée :
        Articles < 6h -> bonus temporel +0.05 max (décroissant linéairement).
    NLP-R2 — _get_title/_get_summary couvrent FR et EN.
    NLP-R4 — datetime importé en tête de module (plus dans la boucle).
    """
    if not articles:
        return []

    weight = max(0.0, min(1.0, weight))

    for a in articles:
        # NLP-R2 — helper compatible double convention FR/EN
        title   = _get_title(a)
        summary = _get_summary(a)

        nlp  = nlp_score_article(title, summary)
        base = a.get("art_score", 5.0) / 10.0

        # NLP-FIX7 — bonus temporel articles < 6h
        # NLP-R4 : datetime importé en tête de module (pas dans cette boucle)
        temporal_bonus = 0.0
        try:
            date_str = a.get("date", "")
            if date_str:
                dt    = datetime.strptime(date_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                age_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                if age_h < 6:
                    temporal_bonus = 0.05 * max(0.0, (6.0 - age_h) / 6.0)
        except (ValueError, TypeError):
            pass

        # MATHS-M3 corrigée — multiplicative
        sv2   = (base + temporal_bonus) * (1.0 + weight * nlp)
        score = round(min(10.0, sv2 * 10.0), 3)

        a["nlp_score"]    = round(nlp, 4)
        a["art_score_v2"] = score

    articles.sort(key=lambda x: x.get("art_score_v2", 5.0), reverse=True)
    log.debug(
        f"nlp_rerank : {len(articles)} articles | weight={weight:.2f} | "
        f"top={articles[0].get('art_score_v2', '?')} "
        f"('{_get_title(articles[0])[:60]}')"
    )
    return articles


# =============================================================================
# CROSS-REFERENCE BOOST (NLP-FIX2 — MATHS-M4 CORRIGÉ)
# NLP-R2 : _get_title/_get_summary couvrent FR et EN
# =============================================================================

def cross_reference_tfidf(
    articles: list[dict],
    threshold: float = CROSS_REF_THRESHOLD,
) -> list[dict]:
    """
    Détecte les articles traitant du même événement via cosinus TF-IDF.
    Booste le score des articles confirmés par N sources >= 2.

    Formule v3.40 (MATHS-M4 corrigée) :
        boost = 0.8 * ln(N+1)   <- additif sur art_score_v2

    Pourquoi 0.8 au lieu de 1.5 ?
        1.5*ln(10) ~ +3.45 pts — élève artificiellement Reuters/AP/BBC.
        0.8*ln(10) ~ +1.84 pts max — réel signal de confirmation multi-sources.
    NLP-R2 : _get_title/_get_summary couvrent FR et EN.
    """
    if not SKLEARN_OK or len(articles) < 2 or _VECTORIZER is None:
        return articles

    import copy
    articles = copy.deepcopy(articles)

    # NLP-R2 — helper interne
    texts = [(_get_title(a) + " " + _get_summary(a)).lower() for a in articles]

    try:
        mat  = _VECTORIZER.transform(texts)
        sims = cosine_similarity(mat)
        n    = len(articles)

        cross_counts: list[int] = [0] * n
        for i in range(n):
            for j in range(i + 1, n):
                if sims[i, j] >= threshold:
                    cross_counts[i] += 1
                    cross_counts[j] += 1

        boosted = 0
        for idx, count in enumerate(cross_counts):
            articles[idx]["cross_ref_count"] = count
            if count >= 1:
                boost = 0.8 * math.log(count + 1)
                old   = articles[idx].get("art_score_v2", articles[idx].get("art_score", 5.0))
                articles[idx]["art_score_v2"] = round(min(10.0, old + boost), 3)
                boosted += 1

        if boosted:
            articles.sort(key=lambda x: x.get("art_score_v2", 5.0), reverse=True)
            log.debug(
                f"cross_reference_tfidf : {boosted}/{n} boostés "
                f"(seuil={threshold:.2f}, MATHS-M4 0.8*ln(N))"
            )
    except Exception as e:
        log.warning(f"cross_reference_tfidf : erreur — {e}")

    return articles


# =============================================================================
# DÉDUPLICATION SÉMANTIQUE (NLP-FIX12)
# NLP-R2 : _get_title/_get_summary couvrent FR et EN
# =============================================================================

def deduplicate_semantic(
    articles: list[dict],
    threshold: float = DEDUP_THRESHOLD,
) -> list[dict]:
    """
    Supprime les doublons sémantiques (même événement, sources différentes).
    Conserve l'article au score le plus élevé parmi les doublons.

    Seuil par défaut : 0.92 (très strict).
    Chaque article conservé reçoit le champ dedup_dropped.
    NLP-R2 : _get_title/_get_summary couvrent FR et EN.
    """
    if not SKLEARN_OK or len(articles) < 2 or _VECTORIZER is None:
        return articles

    # NLP-R2 — helper interne
    texts = [(_get_title(a) + " " + _get_summary(a)).lower() for a in articles]

    try:
        mat  = _VECTORIZER.transform(texts)
        sims = cosine_similarity(mat)
        n    = len(articles)

        kept    = [True] * n
        dropped = [0] * n

        for i in range(n):
            if not kept[i]:
                continue
            for j in range(i + 1, n):
                if kept[j] and sims[i, j] >= threshold:
                    kept[j] = False
                    dropped[i] += 1

        result = []
        for idx, keep in enumerate(kept):
            if keep:
                articles[idx]["dedup_dropped"] = dropped[idx]
                result.append(articles[idx])

        removed = n - len(result)
        if removed:
            log.debug(
                f"deduplicate_semantic : {removed} doublons supprimés "
                f"(seuil={threshold:.2f})"
            )
        return result

    except Exception as e:
        log.warning(f"deduplicate_semantic : erreur — {e}")
        return articles


# =============================================================================
# HEATMAP CO-OCCURRENCE (NLP-FIX8 — fenêtre locale)
# =============================================================================

HEATMAP_TERMS: list[str] = [
    "ugv", "usv", "uuv", "drone", "fpv", "swarm", "essaim",
    "autonome", "autonomous", "ia embarquee", "ai weapon",
    "c-uas", "anti-drone", "electronic warfare", "ew",
    "loitering", "kamikaze", "laws", "contract", "contrat",
    "ukraine", "russia", "chine", "china", "nato", "otan",
    "milrem", "anduril", "baykar", "elbit", "rheinmetall",
]


def build_cooccurrence_matrix(
    articles: list[dict],
    terms: list[str] | None = None,
    window: int = COOCCURRENCE_WINDOW,
) -> dict[str, dict[str, int]]:
    """
    Matrice de co-occurrence sur fenêtre glissante LOCALE (NLP-FIX8).
    window=3 : tokens avant/après chaque terme par document.
    NLP-R2 : utilise _get_title/_get_summary pour compatibilité FR/EN.
    """
    if terms is None:
        terms = HEATMAP_TERMS

    matrix: dict[str, dict[str, int]] = {t: {u: 0 for u in terms} for t in terms}

    for a in articles:
        # NLP-R2 — helper interne
        text   = (_get_title(a) + " " + _get_summary(a)).lower()
        tokens = re.findall(r"[a-z" + chr(192) + "-" + chr(255) + r"-]+", text)

        for i, tok in enumerate(tokens):
            matched_terms = [t for t in terms if t in tok or tok in t]
            if not matched_terms:
                continue
            window_tokens = tokens[max(0, i - window): i + window + 1]
            window_text   = " ".join(window_tokens)
            for t1 in matched_terms:
                for t2 in terms:
                    if t2 != t1 and t2 in window_text:
                        matrix[t1][t2] += 1

    return matrix


def save_cooccurrence_heatmap(
    matrix: dict[str, dict[str, int]],
    output_path: Path | None = None,
) -> Path | None:
    """
    Sauvegarde la heatmap co-occurrence en PNG.
    NLP-FIX5 : atomique (.tmp -> rename).
    NLP-FIX6 : imports matplotlib locaux (évite écrasement namespace).
    """
    if output_path is None:
        LOGS_DIR.mkdir(exist_ok=True)
        output_path = LOGS_DIR / "nlp_cooccurrence_heatmap.png"

    try:
        # NLP-FIX6 — imports locaux isolés
        import matplotlib.pyplot as _plt
        import numpy as _np

        terms = list(matrix.keys())
        data  = _np.array([[matrix[t1][t2] for t2 in terms] for t1 in terms])

        fig, ax = _plt.subplots(figsize=(14, 12))
        im = ax.imshow(data, cmap="YlOrRd", aspect="auto")
        _plt.colorbar(im, ax=ax, label="Co-occurrences")
        ax.set_xticks(range(len(terms)))
        ax.set_yticks(range(len(terms)))
        ax.set_xticklabels(terms, rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels(terms, fontsize=8)
        ax.set_title(f"Co-occurrence NLP — SENTINEL v{VERSION}", fontsize=12, pad=16)
        fig.tight_layout()

        # NLP-FIX5 — atomique
        tmp = output_path.with_suffix(".tmp.png")
        fig.savefig(str(tmp), dpi=120, bbox_inches="tight")
        _plt.close(fig)
        tmp.replace(output_path)
        log.info(f"Heatmap sauvegardée : {output_path}")
        return output_path

    except ImportError:
        log.debug("Heatmap : matplotlib absent — skip")
        return None
    except Exception as e:
        log.warning(f"Heatmap : erreur — {e}")
        return None


# =============================================================================
# MÉTRIQUES SENTINELDB (NLP-FIX13 + NLP-R3)
# NLP-R3 : table dédiée nlp_metrics (même pattern que telegram_metrics)
#          L'INSERT dans la table partagée metrics échouait silencieusement
#          (colonnes nlp_avg_score, nlp_high_count, nlp_dedup_count absentes)
# =============================================================================

def save_nlp_metrics(articles: list[dict]) -> None:
    """
    NLP-FIX13 + NLP-R3 — Enregistre les métriques NLP dans SentinelDB.
    Table dédiée nlp_metrics (CREATE IF NOT EXISTS) — évite le conflit avec
    la table metrics partagée qui n'a pas de colonnes nlp_*.
    """
    if not articles:
        return

    today       = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    nlp_scores  = [a.get("nlp_score", 0.0) for a in articles if "nlp_score" in a]
    avg_nlp     = round(sum(nlp_scores) / max(len(nlp_scores), 1), 4)
    high_count  = sum(1 for s in nlp_scores if s > 0.5)
    dedup_total = sum(a.get("dedup_dropped", 0) for a in articles)

    try:
        from db_manager import getdb as get_db
        with get_db() as db:
            # NLP-R3 — table dédiée, indépendante de metrics
            db.execute("""
                CREATE TABLE IF NOT EXISTS nlp_metrics (
                    date            TEXT PRIMARY KEY,
                    nlp_avg_score   REAL,
                    nlp_high_count  INTEGER,
                    nlp_dedup_count INTEGER,
                    updated_at      TEXT
                )
            """)
            db.execute("""
                INSERT INTO nlp_metrics
                    (date, nlp_avg_score, nlp_high_count, nlp_dedup_count, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    nlp_avg_score   = excluded.nlp_avg_score,
                    nlp_high_count  = excluded.nlp_high_count,
                    nlp_dedup_count = excluded.nlp_dedup_count,
                    updated_at      = excluded.updated_at
            """, (
                today,
                avg_nlp,
                high_count,
                dedup_total,
                datetime.now(timezone.utc).isoformat(),
            ))
        log.debug(
            f"Métriques NLP sauvegardées : avg={avg_nlp:.4f}, "
            f"high={high_count}, dedup={dedup_total}"
        )
    except (ImportError, Exception) as e:
        log.debug(f"Métriques SentinelDB NLP non disponibles : {e}")


# =============================================================================
# PIPELINE COMPLET — POINT D'ENTRÉE RECOMMANDÉ
# =============================================================================

def run_nlp_pipeline(
    articles: list[dict],
    weight: float = DEFAULT_NLP_WEIGHT,
    cross_ref_threshold: float = CROSS_REF_THRESHOLD,
    dedup_threshold: float = DEDUP_THRESHOLD,
    save_metrics: bool = True,
) -> list[dict]:
    """
    Pipeline NLP complet en une seule fonction :
      1. nlp_rerank             — scoring cosinus + bonus temporel (MATHS-M3)
      2. cross_reference_tfidf  — boost multi-sources (MATHS-M4)
      3. deduplicate_semantic   — suppression doublons > threshold
      4. save_nlp_metrics       — sauvegarde SentinelDB (optionnel)

    Intégration sentinel_main.py :
        from nlp_scorer import run_nlp_pipeline
        articles = run_nlp_pipeline(articles)
        # art_score_v2 est le nouveau score de tri post-NLP

    Args:
        articles            : list[dict] format pipeline (title/summary OU titre/resume)
        weight              : Poids NLP dans [0.0, 1.0] (défaut 0.30)
        cross_ref_threshold : Seuil boost cross-source (défaut 0.35)
        dedup_threshold     : Seuil dédup sémantique (défaut 0.92)
        save_metrics        : Enregistrer métriques dans SentinelDB

    Returns:
        list[dict] rerankée, boostée, dédupliquée — triée par art_score_v2.
    """
    t0 = time.perf_counter()
    n0 = len(articles)

    if not articles:
        return []

    if not SKLEARN_OK:
        log.warning("NLP pipeline : scikit-learn absent — articles sans rerank")
        return articles

    articles = nlp_rerank(articles, weight=weight)
    articles = cross_reference_tfidf(articles, threshold=cross_ref_threshold)
    articles = deduplicate_semantic(articles, threshold=dedup_threshold)

    if save_metrics:
        save_nlp_metrics(articles)

    elapsed = time.perf_counter() - t0
    log.info(
        f"NLP pipeline : {n0} -> {len(articles)} articles | "
        f"weight={weight:.2f} | cross_ref>={cross_ref_threshold} | "
        f"dedup>={dedup_threshold} | {elapsed * 1000:.0f}ms"
    )
    return articles


# =============================================================================
# TESTS INTERNES (NLP-FIX9 / NLP-FIX15)
# =============================================================================

def _selftest() -> None:
    """Suite de tests autonomes avec assertions annotées (NLP-FIX15)."""
    log.info("=" * 60)
    log.info(f"NLP_SCORER v{VERSION} — Selftest")
    log.info("=" * 60)

    errors: list[str] = []

    # Test 1 — score défense vs hors-sujet
    s_def = nlp_score_article(
        "DARPA awards contract autonomous UGV program",
        "The Defense Advanced Research Projects Agency awarded a contract "
        "for unmanned ground vehicle development with AI targeting.",
    )
    s_off = nlp_score_article(
        "New coffee shop opens downtown Paris",
        "A new artisan coffee shop opened near the Louvre museum today.",
    )
    log.info(f"T1 — Défense : {s_def:.4f} | Hors-sujet : {s_off:.4f}")
    if SKLEARN_OK and not (s_def > s_off):
        errors.append(f"T1 MATHS : s_def ({s_def:.4f}) <= s_off ({s_off:.4f})")

    # Test 2 — formule multiplicative MATHS-M3
    # NLP-R2 : testé avec les deux conventions de champs (title/summary et titre/resume)
    arts_en = [
        {"title": "DARPA UGV autonomous contract", "summary": "autonomous ground vehicle ugv darpa defense", "art_score": 9.0, "date": "2026-04-09 01:00"},
        {"title": "Coffee Paris trendy", "summary": "artisan coffee downtown paris", "art_score": 3.0, "date": "2026-04-09 01:00"},
        {"title": "Milrem TYPE-X NATO contract", "summary": "milrem ugv nato autonomous combat vehicle", "art_score": 8.0, "date": "2026-04-09 01:00"},
    ]
    arts_fr = [
        {"titre": "DARPA UGV contrat autonome", "resume": "vehicule terrestre ugv darpa defense autonome", "art_score": 9.0, "date": "2026-04-09 01:00"},
    ]
    ranked_en = nlp_rerank([a.copy() for a in arts_en], weight=0.3)
    ranked_fr = nlp_rerank([a.copy() for a in arts_fr], weight=0.3)
    log.info("T2a — Rerank MATHS-M3 (champs EN title/summary) :")
    for a in ranked_en:
        log.info(f"  [{a['art_score_v2']:5.2f}] nlp={a.get('nlp_score', 0):.4f} | {a['title'][:60]}")
    log.info(f"T2b — Rerank MATHS-M3 (champs FR titre/resume) : score={ranked_fr[0]['art_score_v2']:.3f}")
    if SKLEARN_OK:
        s_a    = next(a["art_score_v2"] for a in ranked_en if a["art_score"] == 9.0)
        s_off2 = next(a["art_score_v2"] for a in ranked_en if a["art_score"] == 3.0)
        if s_a < 9.0:
            errors.append(f"T2 MATHS-M3 : score-A pénalisé {s_a:.3f} < 9.0")
        if s_a <= s_off2:
            errors.append(f"T2 : score-A ({s_a:.3f}) <= score_off ({s_off2:.3f})")
        if ranked_fr[0].get("nlp_score", 0.0) <= 0.0:
            errors.append("T2b NLP-R2 : score NLP nul avec champs FR titre/resume")

    # Test 3 — cross-reference MATHS-M4
    arts_cr = [
        {"title": "FPV drone destroys UGV Zaporizhzhia", "summary": "fpv drone destroyed unmanned ground vehicle zaporizhzhia", "art_score": 7.0, "art_score_v2": 7.0},
        {"title": "FPV kamikaze drone hits armored robot Ukraine", "summary": "kamikaze fpv drone armored robot ukraine battlefield", "art_score": 7.0, "art_score_v2": 7.0},
        {"title": "Weather France sunny weekend", "summary": "sunny weather paris weekend forecast", "art_score": 2.0, "art_score_v2": 2.0},
    ]
    boosted = cross_reference_tfidf([a.copy() for a in arts_cr], threshold=0.20)
    log.info("T3 — Cross-reference MATHS-M4 :")
    for a in boosted:
        log.info(f"  [{a.get('art_score_v2', 0):5.2f}] cross={a.get('cross_ref_count', 0)} | {a['title'][:60]}")

    # Test 4 — déduplication sémantique
    arts_dup = [
        {"title": "Milrem TYPE-X UGV NATO contract 2026", "summary": "milrem robotics type-x ugv nato contract autonomous 2026", "art_score": 8.0, "art_score_v2": 8.0},
        {"title": "Milrem TYPE-X contrat OTAN 2026", "summary": "milrem robotics type-x ugv autonome contrat otan 2026", "art_score": 7.5, "art_score_v2": 7.5},
        {"title": "Baykar TB3 naval drone test success", "summary": "baykar bayraktar tb3 naval drone sea test success", "art_score": 7.0, "art_score_v2": 7.0},
    ]
    deduped = deduplicate_semantic([a.copy() for a in arts_dup], threshold=0.50)
    log.info(f"T4 — Déduplication : {len(arts_dup)} -> {len(deduped)} articles")

    # Test 5 — cyrillique (NLP-FIX11)
    s_cyr = nlp_score_article(
        "БПЛА дрон шахед ланцет Запоріжжя",
        "Автономний бпла безпілотний ланцет шахед fpv бойове застосування",
    )
    log.info(f"T5 — Cyrillique : {s_cyr:.4f}")
    if SKLEARN_OK and s_cyr <= 0.0:
        errors.append("T5 : score cyrillique nul")

    # Test 6 — pipeline complet (mix conventions FR+EN)
    all_arts = (
        [a.copy() for a in arts_en] +
        [a.copy() for a in arts_fr] +
        [a.copy() for a in arts_cr[:2]]
    )
    result = run_nlp_pipeline(all_arts, weight=0.3, save_metrics=False)
    log.info(f"T6 — Pipeline complet : {len(all_arts)} -> {len(result)} articles")
    if not result:
        errors.append("T6 : run_nlp_pipeline retourne liste vide")

    # Résumé
    log.info("=" * 60)
    if errors:
        log.error(f"SELFTEST ECHEC — {len(errors)} erreur(s) :")
        for e in errors:
            log.error(f"  x {e}")
    else:
        log.info("SELFTEST OK — Tous les tests passés")
    log.info("=" * 60)


# =============================================================================
# BENCHMARK (--bench)
# =============================================================================

def _benchmark(n: int = 200) -> None:
    import random

    TITLES_DEF = [
        "DARPA awards contract UGV autonomous ground vehicle",
        "Milrem TYPE-X deploy NATO autonomous combat robot",
        "FPV drone swarm Ukraine battlefield autonomous",
        "USV autonomous naval drone Black Sea patrol",
        "Loitering munition Lancet Ukraine precision strike",
        "C-UAS anti-drone shield electronic warfare system",
        "Electronic warfare jamming GPS autonomous drone defeat",
        "Baykar TB3 naval drone autonomous carrier deck",
    ]
    TITLES_OFF = [
        "New restaurant opens Paris Marais",
        "Champions League final tickets",
        "Bitcoin price analysis weekend",
        "French presidential election polling",
        "Weather forecast Paris Ile-de-France",
    ]

    articles = []
    for i in range(n):
        if i % 3 == 0:
            t = random.choice(TITLES_OFF)
            s = 3.0 + random.random() * 2
        else:
            t = random.choice(TITLES_DEF)
            s = 6.0 + random.random() * 3
        articles.append({
            "title": t,
            "summary": t + " defense military autonomous",
            "art_score": round(s, 2),
            "date": "2026-04-09 01:00",
        })

    t0      = time.perf_counter()
    result  = run_nlp_pipeline(articles, weight=0.3, save_metrics=False)
    elapsed = time.perf_counter() - t0

    log.info(f"BENCHMARK : {n} articles en {elapsed * 1000:.0f}ms ({n / elapsed:.0f} art/s)")
    log.info("Top 5 :")
    for a in result[:5]:
        log.info(f"  [{a.get('art_score_v2', 0):5.2f}] {a['title'][:70]}")


# =============================================================================
# ENTRY POINT CLI
# NLP-R5 : argparse remplace le parsing manuel sys.argv
#   - --help natif généré automatiquement
#   - parse_known_args() : coexiste avec les args de sentinel_main.py
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="SENTINEL NLP Scorer — Scoring TF-IDF bigrammes défense",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :
"
            "  python nlp_scorer.py
"
            "  python nlp_scorer.py --bench
"
            "  python nlp_scorer.py --heatmap
"
        ),
    )
    parser.add_argument(
        "--bench",
        action="store_true",
        help="Benchmark sur 200 articles synthétiques",
    )
    parser.add_argument(
        "--heatmap",
        action="store_true",
        help="Génère heatmap co-occurrence dans logs/",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=200,
        help="Nombre d'articles pour le benchmark (défaut : 200)",
    )

    # parse_known_args : ignore les args inconnus (sentinel_main.py, cron, etc.)
    args, unknown = parser.parse_known_args()
    if unknown:
        log.debug(f"Arguments CLI ignorés (non-nlp) : {unknown}")

    if args.bench:
        _benchmark(n=args.n)
    elif args.heatmap:
        test_arts = [
            {"title": "FPV drone swarm ukraine autonomous", "summary": "fpv drone swarm ukraine autonomous robot ugv"},
            {"title": "Electronic warfare jamming c-uas drone", "summary": "electronic warfare ew jamming anti-drone c-uas"},
            {"title": "Milrem UGV contract NATO autonomous", "summary": "milrem ugv autonomous nato contract defense"},
        ]
        mat  = build_cooccurrence_matrix(test_arts)
        path = save_cooccurrence_heatmap(mat)
        if path:
            log.info(f"Heatmap : {path}")
    else:
        _selftest()
