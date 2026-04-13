#!/usr/bin/env python3
# sentinel_api.py — SENTINEL v3.52 — Appel Claude Sonnet + outil Tavily
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 :
# A07-FIX    call_api() définie UNE SEULE FOIS hors boucle agentique
# A31-FIX    SENTINEL_MODEL via var ENV anti-drift (CODE-5)
# CODE-R5    Bloc DEDUP mort supprimé de la boucle agentique
# R1-F3      Backoff natif Python 10s/20s/40s (tenacity optionnel)
# R1-F4      Fallback Haiku-4.5 sur 529/503, puis GPT-4o-mini, puis Ollama
# R1A3-NEW-3 Circuit-breaker cb_load/cb_fail/cb_ok/cb_active
# R6-NEW-2   Validation schéma deltas — listes garanties
# R6-NEW-3   TAVILY_MAX configurable via .env
# R6-NEW-4   Robustesse delta JSON malformé
# FIX-OBS1   Logging structuré via logger nommé
# VISUAL-R1  extract_metrics_from_report() + save vers SentinelDB
#
# Corrections v3.41 :
# API-FIX1   extract_metrics_from_report() : date_str optionnel
# API-FIX2   extract_metrics_from_report() : retourne dict
# API-FIX3   cleanup_dangling_tmp() exposé pour sentinel_main.py
#
# Corrections v3.50 — Compatibilité sentinel_main v3.50 :
# API-50-FIX1  run_sentinel() : paramètre report_type="daily" ajouté (MAIN-46-C)
# API-50-FIX2  run_sentinel_monthly() : nouvelle fonction (MAIN-43-FIX2)
#              Guard double injection INJECTION_MENSUELLE.
# API-50-FIX3  Regex extract_metrics_from_report() : s* d+ .[d]+
#              Patterns brisés (s* / d+) retournaient {} en silence.
# API-50-FIX4  _build_call_api() / _build_call_with_fallback() factorisés
#
# Corrections v3.51 :
# API-51-FIX1  Timeout Ollama configurable via OLLAMA_TIMEOUT (.env)
#              Défaut 180s au lieu de 60s codé en dur.
#
# Corrections v3.52 — Audit complet régression :
# API-52-FIX1  extract_metrics_from_report() : TOUTES les regex reconstruites
#              via chr(92) — même protection anti-copier-coller que TG-R1 /
#              NLP-R1 / MAIL-R1. API-50-FIX3 documentait le fix mais l'ancienne
#              technique (raw string) reste vulnérable. Dashboard renvoyait {}
#              en silence si corruption backslash présente.
#              BUG RÉSIDUEL CORRIGÉ — _PAT_INDICE : le pattern v3.52 original
#              ne matchait pas "Indice d'activité : 7.2/10" car \s*[:-]? ne
#              absorbe pas le texte intermédiaire "d'activité". Corrigé par
#              [^\d]* qui absorbe tout caractère non-numérique jusqu'au chiffre.
#              Tests : "Indice : 7.2/10", "Indice d'activité : 7.2/10",
#              "Indice d'activité global : 8.0/10" — tous matchent ✓
# API-52-FIX2  Prompts chargés en LAZY via _get_system_prompt() /
#              _get_user_prompt_template() — plus au niveau module.
#              Avant ce fix, tout import de sentinel_api (ex: memory_manager.py
#              → from sentinel_api import extract_memory_delta) déclenchait un
#              SystemExit si prompts/ absent, même sans besoin des prompts.
#              Rétrocompatibilité : SYSTEM_PROMPT / USER_PROMPT_TEMPLATE
#              exposés via module __getattr__ (PEP 562 / Python >= 3.7).
# API-52-FIX3  GPT-4o-mini fallback : _sanitize_messages_for_openai() filtre
#              les blocs tool_use / tool_result au format Anthropic avant envoi
#              OpenAI. Sans ce fix, une erreur 400 silencieuse perdait le rapport
#              si Anthropic + le fallback Haiku étaient indisponibles.
# API-52-FIX4  Circuit-breaker auto-reset temporel : cb_fail() enregistre
#              last_fail_ts, _cb_load() reset automatiquement après
#              CB_RESET_H heures (défaut 2h, configurable SENTINEL_CB_RESET_H).
#              Sans ce fix, le circuit restait ouvert indéfiniment après une
#              panne nocturne d'Anthropic, forçant Haiku toute la journée.
# API-52-FIX5  OPENAI_FALLBACK_MODEL configurable via ENV (était "gpt-4o-mini"
#              hardcodé). Cohérence avec SENTINEL_MODEL / HAIKU_MODEL.
# API-52-FIX6  _sanitize_tool_result_content() : normalise le champ content
#              d'un tool_result (str | list | dict → str) pour éviter le
#              TypeError dans OpenAI et Ollama.
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("sentinel.api")

# ── Modèles ───────────────────────────────────────────────────────────────────
SENTINEL_MODEL        = os.environ.get("SENTINEL_MODEL",        "claude-sonnet-4-6")
HAIKU_MODEL           = os.environ.get("HAIKU_MODEL",           "claude-haiku-4-5")
OPENAI_FALLBACK_MODEL = os.environ.get("OPENAI_FALLBACK_MODEL", "gpt-4o-mini")  # API-52-FIX5

# ── Paramètres API ────────────────────────────────────────────────────────────
SENTINEL_MAX_TOKENS = int(os.environ.get("SENTINEL_MAX_TOKENS", "16000"))
TAVILY_MAX          = int(os.environ.get("SENTINEL_TAVILY_MAX", "5"))

# ── Circuit-breaker ───────────────────────────────────────────────────────────
_CB_PATH    = Path("data") / "api_failures.json"
_CB_MAX     = int(os.environ.get("SENTINEL_CB_MAX",     "3"))
_CB_RESET_H = int(os.environ.get("SENTINEL_CB_RESET_H", "2"))  # API-52-FIX4

# ── Ollama — API-51-FIX1 ──────────────────────────────────────────────────────
OLLAMA_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", "180"))


# =============================================================================
# API-52-FIX1 — REGEX BACKSLASH-SAFE pour extract_metrics_from_report()
# chr(92) = "\"  —  même protection anti-copier-coller que :
#   TG-R1   (telegram_scraper.py)
#   NLP-R1  (nlp_scorer.py)
#   MAIL-R1 (mailer.py)
#
# BUG RÉSIDUEL CORRIGÉ — _PAT_INDICE :
#   Le pattern v3.52 original "\s*[:-]?\s*" ne matchait pas
#   "Indice d'activité : 7.2/10" car le texte intermédiaire
#   "d'activité" n'était pas absorbé.
#   Correction : [^\d]* absorbe tout caractère non-numérique
#   jusqu'au premier chiffre.
#   Tests validés :
#     "Indice d'activité : 7.2/10"         → 7.2  ✓
#     "Indice d'activité global : 8.0/10"  → 8.0  ✓
#     "Indice : 6.5/10"                    → 6.5  ✓
#     "Indice: 7.0/10"                     → 7.0  ✓
#     "Indice d'activité — 7.2/10"         → 7.2  ✓
# =============================================================================
_BS = chr(92)   # "\"

# Pattern : Indice[^\d]*([\d]+(?:\.[\d]+)?)\s*/?\s*10
# [^\d]* absorbe le texte intermédiaire ("d'activité", "global", "—", etc.)
_PAT_INDICE = re.compile(
    r"Indice"
    + "[^" + _BS + "d]*"
    + "([" + _BS + "d]+(?:"
    + _BS + ".[" + _BS + "d]+)?)"
    + _BS + "s*/?"
    + _BS + "s*10",
    re.IGNORECASE,
)

# Pattern : Alerte\s*[:-]?\s*(VERT|ORANGE|ROUGE)
_PAT_ALERTE = re.compile(
    r"Alerte"
    + _BS + r"s*[:-]?"
    + _BS + r"s*(VERT|ORANGE|ROUGE)",
    re.IGNORECASE,
)

# Pattern : Sources\s+analys[eé]es?\s*[:-]?\s*([\d]+)
_PAT_SOURCES = re.compile(
    r"Sources"
    + _BS + r"s+analys[eé]es?"
    + _BS + r"s*[:-]?"
    + _BS + r"s*([" + _BS + r"d]+)",
    re.IGNORECASE,
)

# Pattern : Pertinentes?\s*[:-]?\s*([\d]+)
_PAT_PERTINENTS = re.compile(
    r"Pertinentes?"
    + _BS + r"s*[:-]?"
    + _BS + r"s*([" + _BS + r"d]+)",
    re.IGNORECASE,
)


def _make_pat_count(alias: str) -> re.Pattern:
    """
    Construit dynamiquement un pattern count sécurisé via chr(92).
    Utilisé pour les alias géographiques et domaines.
    Accepte : chiffres seuls ou chiffres suivis de "%" ou "." (pourcentages).
    """
    return re.compile(
        re.escape(alias)
        + _BS + r"s*[:%]?"
        + _BS + r"s*([" + _BS + r"d]+(?:"
        + _BS + r"." + _BS + r"d+)?)",
        re.IGNORECASE,
    )


# =============================================================================
# UTILITAIRE — Nettoyage fichiers .tmp résiduels (API-FIX3)
# =============================================================================

def cleanup_dangling_tmp(
    directories: list[str] | None = None,
    max_age_h: int = 24,
) -> int:
    """
    Supprime les fichiers .tmp résiduels laissés par un crash système.
    Appelé en tête de sentinel_main.py après initdb().
    Retourne le nombre de fichiers supprimés.
    """
    if directories is None:
        directories = ["data"]
    cutoff  = time.time() - max_age_h * 3600
    removed = 0
    for directory in directories:
        d = Path(directory)
        if not d.is_dir():
            continue
        for tmp_file in d.glob("*.tmp"):
            try:
                if tmp_file.stat().st_mtime < cutoff:
                    tmp_file.unlink(missing_ok=True)
                    removed += 1
                    log.info(f"cleanup_dangling_tmp : supprimé {tmp_file}")
            except OSError as e:
                log.warning(
                    f"cleanup_dangling_tmp : impossible de supprimer {tmp_file} : {e}"
                )
    if removed:
        log.info(
            f"cleanup_dangling_tmp : {removed} fichier(s) .tmp résiduel(s) supprimé(s)"
        )
    return removed


# =============================================================================
# CHARGEMENT DES PROMPTS — LAZY (API-52-FIX2)
# =============================================================================

def _load_prompt(filename: str) -> str:
    """
    Charge un fichier prompt depuis ./prompts/.
    Lève SystemExit explicite si absent (B9 / F-3).
    """
    p = Path("prompts") / filename
    if not p.exists():
        log.critical(f"PROMPT {p} introuvable — lancer initprompts.py en premier.")
        raise SystemExit(f"ERREUR : {p} manquant. Exécuter : python initprompts.py")
    return p.read_text(encoding="utf-8")


def _load_prompt_optional(
    filename: str,
    fallback_filename: str,
) -> tuple[str, str]:
    """
    [API-50-FIX1] Charge filename si présent, sinon fallback_filename.
    Retourne (contenu, nom_du_fichier_effectivement_chargé).
    Ne lève jamais SystemExit — le fallback est garanti d'exister.
    """
    p = Path("prompts") / filename
    if p.exists():
        log.debug(f"PROMPT chargé : {filename}")
        return p.read_text(encoding="utf-8"), filename
    log.info(
        f"PROMPT {filename} absent — fallback {fallback_filename} utilisé. "
        f"Créer prompts/{filename} pour personnaliser ce type de rapport."
    )
    return _load_prompt(fallback_filename), fallback_filename


# ── API-52-FIX2 : Cache lazy — plus de chargement au niveau module ────────────
# Avant ce fix : SYSTEM_PROMPT = _load_prompt("system.txt") au niveau module
# → SystemExit à l'import si prompts/ absent, même pour
#   "from sentinel_api import extract_memory_delta"
_SYSTEM_PROMPT_CACHE:        str | None = None
_USER_PROMPT_TEMPLATE_CACHE: str | None = None


def _get_system_prompt() -> str:
    """Retourne SYSTEM_PROMPT en lazy (chargé au 1er appel, jamais à l'import)."""
    global _SYSTEM_PROMPT_CACHE
    if _SYSTEM_PROMPT_CACHE is None:
        _SYSTEM_PROMPT_CACHE = _load_prompt("system.txt")
    return _SYSTEM_PROMPT_CACHE


def _get_user_prompt_template() -> str:
    """Retourne USER_PROMPT_TEMPLATE en lazy."""
    global _USER_PROMPT_TEMPLATE_CACHE
    if _USER_PROMPT_TEMPLATE_CACHE is None:
        _USER_PROMPT_TEMPLATE_CACHE = _load_prompt("daily.txt")
    return _USER_PROMPT_TEMPLATE_CACHE


def __getattr__(name: str):
    """
    API-52-FIX2 — PEP 562 (Python >= 3.7) : lazy module attributes.
    Les accès à SYSTEM_PROMPT / USER_PROMPT_TEMPLATE déclenchent le
    chargement à la 1ère lecture, pas à l'import.
    Rétrocompatibilité totale avec le code existant.
    """
    if name == "SYSTEM_PROMPT":
        return _get_system_prompt()
    if name == "USER_PROMPT_TEMPLATE":
        return _get_user_prompt_template()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ── Outil Tavily ──────────────────────────────────────────────────────────────
TAVILY_TOOL = {
    "name": "web_search",
    "description": (
        "Recherche web pour vérifier un fait défense. "
        f"UNIQUEMENT pour les faits CRITIQUES — max {TAVILY_MAX} appels/rapport."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Requête courte ≤ 10 mots",
            }
        },
        "required": ["query"],
    },
}


# =============================================================================
# CIRCUIT-BREAKER (R1A3-NEW-3 / API-52-FIX4)
# =============================================================================

def _cb_load() -> dict:
    """
    Charge l'état du circuit-breaker depuis data/api_failures.json.
    API-52-FIX4 : auto-reset si last_fail_ts > _CB_RESET_H heures.
    Sans ce fix, le circuit restait ouvert indéfiniment après une panne
    nocturne, forçant Haiku toute la journée suivante.
    """
    try:
        if _CB_PATH.exists():
            data         = json.loads(_CB_PATH.read_text())
            last_fail_ts = data.get("last_fail_ts", 0)
            if time.time() - last_fail_ts > _CB_RESET_H * 3600:
                log.info(
                    f"CIRCUIT-BREAKER auto-reset : dernier échec il y a "
                    f"> {_CB_RESET_H}h — réessai Sonnet autorisé"
                )
                return {"count": 0, "active": False, "last_fail_ts": 0}
            return data
    except Exception:
        pass
    return {"count": 0, "active": False, "last_fail_ts": 0}


def _cb_save(data: dict) -> None:
    _CB_PATH.parent.mkdir(exist_ok=True)
    tmp = _CB_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(_CB_PATH)


def cb_fail() -> bool:
    """
    Enregistre un échec API. Retourne True si le circuit est désormais ouvert.
    API-52-FIX4 : enregistre last_fail_ts pour l'auto-reset temporel.
    """
    d                 = _cb_load()
    d["count"]        = d.get("count", 0) + 1
    d["active"]       = d["count"] >= _CB_MAX
    d["last_fail_ts"] = time.time()
    if d["active"]:
        log.error(
            f"CIRCUIT-BREAKER {d['count']} échecs consécutifs "
            f"— bascule Haiku-only (seuil={_CB_MAX}). "
            f"Auto-reset dans {_CB_RESET_H}h."
        )
    _cb_save(d)
    return d["active"]


def cb_ok() -> None:
    """Remet le circuit-breaker à zéro après un succès Sonnet."""
    _cb_save({"count": 0, "active": False, "last_fail_ts": 0})


def cb_active() -> bool:
    """True si le circuit est ouvert (trop d'échecs récents ET < _CB_RESET_H)."""
    return _cb_load().get("active", False)


# =============================================================================
# TAVILY WEB SEARCH
# =============================================================================

def do_web_search(query: str) -> str:
    """
    Appel Tavily avec troncature résultats pour éviter overflow contexte.
    Retourne du JSON string (résultats ou erreur).
    """
    try:
        from tavily import TavilyClient  # type: ignore
        client    = TavilyClient(api_key=os.environ.get("TAVILY_API_KEY", ""))
        results   = client.search(query, max_results=3)
        truncated = [
            {
                "title":   r.get("title",   ""),
                "url":     r.get("url",     ""),
                "content": r.get("content", "")[:800],
            }
            for r in results.get("results", [])[:3]
        ]
        return json.dumps(truncated, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# EXTRACT MEMORY DELTA (R6-NEW-2 / R6-NEW-4)
# =============================================================================

def extract_memory_delta(report_text: str) -> dict:
    """
    Extrait les mises à jour mémoire depuis la section 9 du rapport.
    Retourne toujours un dict avec 3 clés → listes (jamais None).
    """
    _empty: dict = {
        "nouvelles_tendances": [],
        "alertes_ouvertes":    [],
        "alertes_closes":      [],
    }

    match = re.search(
        r"DEBUTJSONDELTA(.+?)FINJSONDELTA",
        report_text,
        re.DOTALL,
    )
    if not match:
        log.debug(
            "MEMORY Aucun bloc JSON delta — enrichissement via compression Haiku uniquement"
        )
        return _empty

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        log.warning(f"MEMORY JSON delta invalide : {e}")
        return _empty

    def _safe_list(val, default: list) -> list:
        if isinstance(val, list):
            return val
        if val and isinstance(val, str):
            return [val]
        return default

    return {
        "nouvelles_tendances": _safe_list(data.get("nouvelles_tendances"), []),
        "alertes_ouvertes":    _safe_list(data.get("alertes_ouvertes"),    []),
        "alertes_closes":      _safe_list(data.get("alertes_closes"),      []),
    }


# =============================================================================
# EXTRACT METRICS FROM REPORT (VISUAL-R1 + API-52-FIX1)
# =============================================================================

def extract_metrics_from_report(
    report_text: str,
    date_str: str | None = None,
) -> dict:
    """
    Extrait les métriques structurées du MODULE 1 et les enregistre
    dans SentinelDB via savemetrics().

    API-FIX1     — date_str optionnel (défaut : date UTC du jour).
    API-FIX2     — Retourne le dict (était None).
    API-52-FIX1  — Toutes les regex reconstruites via chr(92).
                   Bug résiduel _PAT_INDICE corrigé : [^digits]* absorbe
                   le texte intermédiaire ("d'activité", "global", etc.)
    VISUAL-R1    — Élimine l'extraction regex fragile dans dashboard.py.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).date().isoformat()

    metrics: dict = {}

    try:
        from db_manager import SentinelDB

        # ── Indice d'activité ─────────────────────────────────────────────
        # _PAT_INDICE corrigé : [^\d]* absorbe "d'activité", "global", etc.
        m = _PAT_INDICE.search(report_text)
        if m:
            metrics["indice"] = float(m.group(1))

        # ── Niveau alerte ─────────────────────────────────────────────────
        m = _PAT_ALERTE.search(report_text)
        if m:
            metrics["alerte"] = m.group(1).upper()

        # ── Sources analysées ─────────────────────────────────────────────
        m = _PAT_SOURCES.search(report_text)
        if m:
            metrics["nb_articles"] = int(m.group(1))

        # ── Sources pertinentes ───────────────────────────────────────────
        m = _PAT_PERTINENTS.search(report_text)
        if m:
            metrics["nb_pertinents"] = int(m.group(1))

        # ── Répartition géographique (MODULE 1) ───────────────────────────
        geo_aliases = {
            "geousa":    ["USA", "États-Unis", "Etats-Unis"],
            "geoeurope": ["Europe"],
            "geoasie":   ["Asie", "Asie-Pac"],
            "geomo":     ["MO", "Moyen-Orient"],
            "georussie": ["Russie"],
        }
        for col, aliases in geo_aliases.items():
            for alias in aliases:
                mg = _make_pat_count(alias).search(report_text)
                if mg:
                    metrics[col] = float(mg.group(1))
                    break

        # ── Répartition domaines ──────────────────────────────────────────
        dom_aliases = {
            "terrestre":   ["Terrestre"],
            "maritime":    ["Maritime"],
            "transverse":  ["Transverse"],
            "contractuel": ["Contractuel"],
        }
        for col, aliases in dom_aliases.items():
            for alias in aliases:
                md = _make_pat_count(alias).search(report_text)
                if md:
                    metrics[col] = float(md.group(1))
                    break

        if metrics:
            SentinelDB.savemetrics(date_str, **metrics)
            log.info(f"METRICS {len(metrics)} champs enregistrés pour {date_str}")
        else:
            log.debug(f"METRICS Aucune métrique extraite du rapport {date_str}")

    except Exception as e:
        log.warning(f"METRICS extract_metrics_from_report non bloquant : {e}")

    return metrics


# =============================================================================
# VALIDATION STRUCTURE RAPPORT (C3-FIX)
# =============================================================================

_REQUIRED_MODULES = [
    "RÉSUMÉ EXÉCUTIF", "MODULE 1", "MODULE 2", "MODULE 3",
    "MODULE 4", "MODULE 5", "MODULE 6", "MODULE 7", "MODULE 8", "MODULE 9",
]


def _validate_report_structure(report_text: str) -> tuple[bool, list[str]]:
    """
    C3-FIX : vérifie présence des 9 modules + résumé.
    Seuil SOFT : > 3 modules manquants → WARNING (dégradation gracieuse,
    jamais bloquant). Le rapport est toujours utilisé même si incomplet.
    """
    missing    = []
    text_upper = report_text.upper()
    for module in _REQUIRED_MODULES:
        if module not in text_upper:
            missing.append(module)
    return len(missing) <= 3, missing


# =============================================================================
# API-52-FIX3 — NETTOYAGE MESSAGES ANTHROPIC → FORMAT OPENAI
# =============================================================================

def _sanitize_tool_result_content(content) -> str:
    """
    API-52-FIX6 : normalise le champ content d'un tool_result.
    L'API OpenAI exige que content soit une string, pas une liste ou un dict.
    """
    if isinstance(content, str):
        return content[:500]
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", "")[:300])
            elif isinstance(item, str):
                parts.append(item[:300])
        return " ".join(parts)[:500]
    if isinstance(content, dict):
        return str(content)[:500]
    return str(content)[:500]


def _sanitize_messages_for_openai(messages: list[dict]) -> list[dict]:
    """
    API-52-FIX3 : convertit les messages Anthropic en format OpenAI.

    Problème : après un tool_use Tavily, messages contient :
      - {"role": "assistant", "content": [TextBlock, ToolUseBlock]}
      - {"role": "user",      "content": [{"type": "tool_result", ...}]}
    L'API OpenAI ne comprend pas ces types → erreur 400 silencieuse.

    Solution : extraire uniquement les TextBlock, convertir tool_result
    en note textuelle (contexte préservé), ignorer ToolUseBlock.
    """
    clean: list[dict] = []
    for msg in messages:
        role    = msg.get("role", "user")
        content = msg.get("content", "")

        if isinstance(content, str):
            if content.strip():
                clean.append({"role": role, "content": content})
            continue

        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                # SDK objects (have .type attribute)
                if hasattr(block, "type"):
                    if block.type == "text" and getattr(block, "text", ""):
                        parts.append(block.text)
                    continue

                # Plain dicts
                if not isinstance(block, dict):
                    continue
                btype = block.get("type", "")
                if btype == "text":
                    parts.append(block.get("text", ""))
                elif btype == "tool_result":
                    result_str = _sanitize_tool_result_content(
                        block.get("content", "")
                    )
                    if result_str:
                        parts.append(f"[Résultat recherche web : {result_str}]")
                # btype == "tool_use" → ignoré intentionnellement

            text = "\n".join(p for p in parts if p).strip()
            if text:
                clean.append({"role": role, "content": text})

    return clean


# =============================================================================
# HELPERS INTERNES — Factorisés (API-50-FIX4)
# =============================================================================

def _build_call_api(client: anthropic.Anthropic):
    """
    Retourne call_api() avec tenacity si disponible, sinon backoff natif.
    A07-FIX : définie UNE SEULE FOIS, jamais dans une boucle.
    R1-F3   : backoff natif 10s → 20s → 40s.
    """
    try:
        from tenacity import retry, wait_exponential, stop_after_attempt  # type: ignore

        @retry(wait=wait_exponential(min=10, max=180), stop=stop_after_attempt(3))
        def call_api(**kw):
            return client.messages.create(**kw)

        log.debug("RETRY tenacity activé")
        return call_api

    except ImportError:
        def call_api(**kw):  # type: ignore[no-redef]
            for attempt in range(3):
                try:
                    return client.messages.create(**kw)
                except anthropic.APIError as e:
                    if attempt >= 2:
                        raise
                    wait = min(120, 10 * (2 ** attempt))
                    log.warning(
                        f"API tentative {attempt + 1}/3 échouée, "
                        f"retry dans {wait}s : {e}"
                    )
                    time.sleep(wait)
            raise RuntimeError("call_api : 3 tentatives épuisées")

        return call_api


def _build_call_with_fallback(call_api):
    """
    Retourne call_with_fallback() : Sonnet → Haiku → OPENAI_FALLBACK_MODEL → Ollama.
    R1-F4 / F-03-FIX  : partagé entre run_sentinel() et run_sentinel_monthly().
    API-51-FIX1        : timeout Ollama via OLLAMA_TIMEOUT (défaut 180s).
    API-52-FIX3        : messages nettoyés pour OpenAI (tool_use/result filtrés).
    API-52-FIX5        : OPENAI_FALLBACK_MODEL configurable via ENV.
    API-52-FIX6        : _sanitize_tool_result_content() pour Ollama.
    """
    def call_with_fallback(**kw):
        try:
            resp = call_api(**kw)
            cb_ok()
            return resp

        except anthropic.APIStatusError as e:
            status = getattr(e, "status_code", 0)
            if status not in (529, 503):
                raise

            log.warning(
                f"MODE DÉGRADÉ Anthropic surchargé HTTP {status} — bascule Haiku-4.5"
            )
            cb_fail()

            # ── Tentative Haiku ───────────────────────────────────────────
            haiku_kw               = dict(kw)
            haiku_kw["model"]      = HAIKU_MODEL
            haiku_kw["max_tokens"] = min(kw.get("max_tokens", 8000), 8000)
            haiku_kw.pop("tools", None)
            try:
                resp = call_api(**haiku_kw)
                log.warning("MODE DÉGRADÉ Rapport Haiku généré — qualité réduite")
                return resp
            except Exception as he:
                log.warning(
                    f"MODE DÉGRADÉ Haiku échoue : {he} "
                    f"— tentative {OPENAI_FALLBACK_MODEL}"
                )

            # ── Fallback OpenAI (API-52-FIX3 + API-52-FIX5) ──────────────
            try:
                import openai  # type: ignore

                oc = openai.OpenAI()

                # API-52-FIX3 : nettoyer les messages Anthropic avant envoi OpenAI
                raw_messages = list(kw.get("messages", []))
                clean_msgs   = _sanitize_messages_for_openai(raw_messages)
                sys_msg      = {"role": "system", "content": kw.get("system", "")}

                r = oc.chat.completions.create(
                    model      = OPENAI_FALLBACK_MODEL,   # API-52-FIX5
                    messages   = [sys_msg] + clean_msgs,
                    max_tokens = min(kw.get("max_tokens", 4096), 16000),
                )
                txt = r.choices[0].message.content or ""

                class _Block:
                    def __init__(self, t: str):
                        self.type = "text"
                        self.text = t

                class _FallbackResp:
                    stop_reason = "end_turn"
                    def __init__(self, t: str):
                        self.content = [_Block(t)]

                log.warning(
                    f"MODE DÉGRADÉ {OPENAI_FALLBACK_MODEL} utilisé — pipeline maintenu"
                )
                return _FallbackResp(txt)  # type: ignore[return-value]

            except Exception as e2:
                log.warning(
                    f"MODE DÉGRADÉ {OPENAI_FALLBACK_MODEL} échoue : {e2} "
                    f"— tentative Ollama"
                )

            # ── Fallback Ollama local (F-03-FIX + API-51-FIX1) ───────────
            try:
                import urllib.request
                import json as _json

                ollama_url   = os.environ.get("OLLAMA_URL",   "http://localhost:11434")
                ollama_model = os.environ.get("OLLAMA_MODEL", "mistral:7b")

                # F-03-FIX : vérifier que Ollama ET le modèle sont disponibles
                with urllib.request.urlopen(
                    f"{ollama_url}/api/tags", timeout=5
                ) as _tags_resp:
                    _tags_data        = _json.loads(_tags_resp.read())
                    _available_models = [
                        m.get("name", "") for m in _tags_data.get("models", [])
                    ]
                if not any(ollama_model in m for m in _available_models):
                    raise RuntimeError(
                        f"Ollama : modèle {ollama_model!r} non disponible. "
                        f"Modèles présents : {_available_models[:3]}. "
                        f"Lancer : ollama pull {ollama_model}"
                    )

                # API-52-FIX3 : nettoyer les messages pour Ollama aussi
                raw_messages = list(kw.get("messages", []))
                clean_msgs   = _sanitize_messages_for_openai(raw_messages)
                sys_content  = kw.get("system", "")
                user_content = "\n".join(
                    m["content"] for m in clean_msgs
                    if m.get("role") == "user" and m.get("content")
                )

                payload = _json.dumps({
                    "model":   ollama_model,
                    "prompt":  f"{sys_content}\n\n{user_content}",
                    "stream":  False,
                    "options": {
                        "num_predict": min(kw.get("max_tokens", 4096), 4096)
                    },
                }).encode()

                req = urllib.request.Request(
                    f"{ollama_url}/api/generate",
                    data    = payload,
                    headers = {"Content-Type": "application/json"},
                    method  = "POST",
                )

                # API-51-FIX1 : OLLAMA_TIMEOUT remplace le 60s codé en dur
                with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp_o:
                    ollama_data = _json.loads(resp_o.read())

                txt = ollama_data.get("response", "")

                class _OllamaBlock:
                    def __init__(self, t):
                        self.type = "text"
                        self.text = t

                class _OllamaResp:
                    stop_reason = "end_turn"
                    def __init__(self, t):
                        self.content = [_OllamaBlock(t)]

                log.warning(
                    f"MODE DÉGRADÉ Ollama ({ollama_model}) utilisé "
                    f"— timeout={OLLAMA_TIMEOUT}s — pipeline maintenu"
                )
                return _OllamaResp(txt)  # type: ignore

            except Exception as e3:
                raise RuntimeError(
                    f"Anthropic + {OPENAI_FALLBACK_MODEL} + Ollama indisponibles : "
                    f"{e} / {e2} / {e3}"
                ) from e

    return call_with_fallback


# =============================================================================
# RUN SENTINEL — PIPELINE QUOTIDIEN
# =============================================================================

def run_sentinel(
    articles_text:  str,
    memory_context: str,
    date_obj=None,
    model:       str | None = None,
    report_type: str        = "daily",
) -> tuple[str, dict]:
    """
    Lance l'analyse SENTINEL complète (quotidienne ou vendredi R&D).

    Paramètres
    ----------
    articles_text  : Articles formatés par format_for_claude()
    memory_context : Mémoire compressée des 7 derniers jours
    date_obj       : Date du rapport (défaut : aujourd'hui UTC)
    model          : Modèle à utiliser (défaut : SENTINEL_MODEL)
    report_type    : "daily" | "friday_rd" (MAIN-46-C)
                     Charge prompts/{report_type}.txt si présent,
                     fallback prompts/daily.txt sinon. Jamais de SystemExit.

    Retourne
    --------
    (report_text: str, memory_deltas: dict)
    """
    if date_obj is None:
        date_obj = datetime.now(timezone.utc).date()
    active_model = model or SENTINEL_MODEL
    client       = anthropic.Anthropic()

    # [API-50-FIX1] Charger le prompt selon le type de rapport
    # [API-52-FIX2] Lazy — plus de chargement à l'import
    prompt_template, used_prompt = _load_prompt_optional(
        f"{report_type}.txt", fallback_filename="daily.txt"
    )
    if used_prompt != f"{report_type}.txt":
        log.info(
            f"run_sentinel report_type={report_type!r} "
            f"— fallback daily.txt "
            f"(créer prompts/{report_type}.txt pour personnaliser)"
        )

    tavily_available = bool(os.environ.get("TAVILY_API_KEY"))

    user_prompt = (
        prompt_template
        # Nouvelle convention avec underscores
        .replace("DATE_AUJOURD_HUI",             date_obj.strftime("%d/%m/%Y"))
        .replace("ARTICLES_FILTRES_PAR_SCRAPER", articles_text)
        .replace("MEMOIRE_COMPRESSE_7_JOURS",    memory_context)
        .replace("OUI_ou_NON",                   "OUI" if tavily_available else "NON")
        # Rétrocompatibilité ancienne convention sans underscores
        .replace("DATEAUJOURDHUI",               date_obj.strftime("%d/%m/%Y"))
        .replace("ARTICLESFILTRESPARSCRAPER",    articles_text)
        .replace("MEMOIRECOMPRESSE7JOURS",       memory_context)
        .replace("OUIouNON",                     "OUI" if tavily_available else "NON")
    )

    tools    = [TAVILY_TOOL] if tavily_available else []
    messages = [{"role": "user", "content": user_prompt}]

    call_api           = _build_call_api(client)
    call_with_fallback = _build_call_with_fallback(call_api)

    # ── Boucle agentique (max 10 tours, TAVILY_MAX appels web) ───────────────
    tavily_calls = 0
    resp         = None
    kw: dict     = {}

    for _turn in range(10):
        kw = dict(
            model      = HAIKU_MODEL if cb_active() else active_model,
            max_tokens = SENTINEL_MAX_TOKENS,
            system     = _get_system_prompt(),   # API-52-FIX2 : lazy
            tools      = tools if not cb_active() else [],
            messages   = messages,
        )
        resp = call_with_fallback(**kw)

        if resp.stop_reason == "end_turn":
            break

        if resp.stop_reason == "tool_use":
            tool_results = []
            for block in resp.content:
                if hasattr(block, "type") and block.type == "tool_use" and block.name == "web_search":
                    if tavily_calls >= TAVILY_MAX:
                        result = json.dumps(
                            {"error": f"quota {TAVILY_MAX} appels Tavily atteint"}
                        )
                        log.info(
                            f"TAVILY quota {TAVILY_MAX} atteint — résultat vide injecté"
                        )
                    else:
                        tavily_calls += 1
                        query  = block.input.get("query", "")
                        result = do_web_search(query)
                        log.info(
                            f"TAVILY [{tavily_calls}/{TAVILY_MAX}] query={query!r}"
                        )

                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })

            messages.append({"role": "assistant", "content": resp.content})
            if tool_results:
                messages.append({"role": "user", "content": tool_results})
            else:
                break
        else:
            break

    # ── Extraire le texte ─────────────────────────────────────────────────────
    if resp is None:
        log.error("API Réponse vide après boucle agentique — rapport non généré")
        return "", {"nouvelles_tendances": [], "alertes_ouvertes": [], "alertes_closes": []}

    report_text = next(
        (b.text for b in resp.content if hasattr(b, "text") and b.text),
        "",
    )
    if not report_text.strip():
        log.error("API Rapport Claude vide")
        return "", {"nouvelles_tendances": [], "alertes_ouvertes": [], "alertes_closes": []}

    log.info(
        f"API Rapport généré {len(report_text)} chars "
        f"| model={kw.get('model', '?')} "
        f"| report_type={report_type!r} "
        f"| {tavily_calls} appel(s) Tavily"
    )

    is_valid, missing = _validate_report_structure(report_text)
    if not is_valid:
        log.warning(
            f"C3 STRUCTURE RAPPORT INCOMPLÈTE : {len(missing)} modules manquants : "
            f"{missing}. Dégradation gracieuse — rapport utilisé quand même."
        )
    else:
        log.info(
            f"C3 Structure valide "
            f"({len(_REQUIRED_MODULES) - len(missing)}/10 modules présents)"
        )

    memory_deltas = extract_memory_delta(report_text)
    extract_metrics_from_report(report_text, str(date_obj))

    return report_text, memory_deltas


# =============================================================================
# RUN SENTINEL MONTHLY — RAPPORT MENSUEL (API-50-FIX2)
# =============================================================================

# Prompt minimal intégré — utilisé si prompts/monthly.txt est absent.
# Contrairement aux prompts daily/system (obligatoires), le mensuel est
# optionnel : le fallback garantit le fonctionnement dès le 1er déploiement.
_MONTHLY_PROMPT_FALLBACK = """\
Tu es SENTINEL, système d'analyse stratégique défense & sécurité.

Tu reçois les résumés hebdomadaires compressés du mois MOIS.
Génère le rapport mensuel stratégique SENTINEL pour MOIS.

Structure obligatoire :
  RÉSUMÉ EXÉCUTIF MENSUEL
  MODULE 1 — INDICATEURS DU MOIS (indice moyen, niveau alerte dominant)
  MODULE 2 — TENDANCES LONG-TERME (confirmées, émergentes, régressives)
  MODULE 3 — ACTEURS CLÉS DU MOIS (top 5, évolution vs mois précédent)
  MODULE 4 — ALERTES PERSISTANTES (ouvertes, closes, nouvelles)
  MODULE 5 — ANALYSE STRATÉGIQUE MENSUELLE
  MODULE 6 — SIGNAUX FAIBLES À SURVEILLER
  MODULE 7 — NOTE MÉTHODOLOGIQUE
    (Mentionner tout rattrapage de collecte GitHub si indiqué
     dans les blocs <context_metadata> ou [RATTRAPAGE GITHUB Xj].)

Respecte les contraintes de biais transmises dans <context_metadata>
sur l'intégralité du rapport.

RÉSUMÉS HEBDOMADAIRES (MOIS) :
INJECTION_MENSUELLE
"""


def run_sentinel_monthly(
    injection: str,
    mois:      str,
    model:     str | None = None,
) -> tuple[str, dict]:
    """
    [API-50-FIX2] Rapport mensuel SENTINEL via synthèse Sonnet.

    Appelé par sentinel_main.run_monthly_report() après la phase MapReduce.
    Architecture single-turn : données déjà compressées par Haiku en MAP,
    Tavily n'apporte aucune valeur ajoutée sur des résumés déjà filtrés.
    Même circuit-breaker et fallback que run_sentinel().

    Paramètres
    ----------
    injection : Blocs [SEMAINE N] produits par run_monthly_report()
    mois      : Ex : "April 2026" (strftime %B %Y)
    model     : Modèle Sonnet (défaut SENTINEL_MODEL)

    Guard double injection (API-50-FIX2) :
        Compatibilité ancienne (INJECTIONMENSUELLE) et nouvelle (INJECTION_MENSUELLE).
        Si le template contient le placeholder → remplace uniquement.
        Sinon → append après le template.

    Retourne
    --------
    (report_text: str, memory_deltas: dict)
    """
    active_model = model or SENTINEL_MODEL
    client       = anthropic.Anthropic()

    # Charger le prompt mensuel — fallback intégré si absent
    monthly_path = Path("prompts") / "monthly.txt"
    if monthly_path.exists():
        monthly_template = monthly_path.read_text(encoding="utf-8")
        log.debug("MONTHLY prompt chargé depuis prompts/monthly.txt")
    else:
        monthly_template = _MONTHLY_PROMPT_FALLBACK
        log.info(
            "MONTHLY prompts/monthly.txt absent — prompt minimal intégré utilisé. "
            "Créer prompts/monthly.txt pour un meilleur contrôle du format."
        )

    # [API-50-FIX2] Guard double injection
    # Compatibilité ancienne (INJECTIONMENSUELLE) et nouvelle (INJECTION_MENSUELLE)
    if "INJECTION_MENSUELLE" in monthly_template:
        user_content = (
            monthly_template
            .replace("MOIS", mois)
            .replace("INJECTION_MENSUELLE", injection)
        )
    elif "INJECTIONMENSUELLE" in monthly_template:
        user_content = (
            monthly_template
            .replace("MOIS", mois)
            .replace("INJECTIONMENSUELLE", injection)
        )
    else:
        # Template sans placeholder explicite → append
        user_content = (
            monthly_template.replace("MOIS", mois)
            + f"\n\n---\nRÉSUMÉS HEBDOMADAIRES ({mois}) :\n\n{injection}"
        )

    call_api           = _build_call_api(client)
    call_with_fallback = _build_call_with_fallback(call_api)

    kw = dict(
        model      = HAIKU_MODEL if cb_active() else active_model,
        max_tokens = SENTINEL_MAX_TOKENS,
        system     = _get_system_prompt(),   # API-52-FIX2 : lazy
        messages   = [{"role": "user", "content": user_content}],
        # Pas de tools — données déjà compressées par Haiku en phase MAP
    )

    try:
        resp = call_with_fallback(**kw)
    except Exception as e:
        log.error(f"run_sentinel_monthly : erreur Claude : {e}", exc_info=True)
        cb_fail()
        return "", {}

    report_text = next(
        (b.text for b in resp.content if hasattr(b, "text") and b.text),
        "",
    )

    if not report_text.strip():
        log.error("run_sentinel_monthly : rapport vide retourné par Claude")
        return "", {}

    log.info(
        f"MONTHLY Rapport mensuel généré : {len(report_text)} chars "
        f"| model={kw['model']} | mois={mois}"
    )

    memory_deltas = extract_memory_delta(report_text)
    return report_text, memory_deltas

