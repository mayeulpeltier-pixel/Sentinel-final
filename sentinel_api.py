#!/usr/bin/env python3
# sentinel_api.py — SENTINEL v3.60 — Appel Claude Sonnet + outil Tavily
# ─────────────────────────────────────────────────────────────────────────────
# Historique des corrections (v3.40 → v3.60) :
#
# v3.40 :
#   A07-FIX    call_api() définie UNE SEULE FOIS hors boucle agentique
#   A31-FIX    SENTINEL_MODEL via var ENV anti-drift (CODE-5)
#   CODE-R5    Bloc DEDUP mort supprimé de la boucle agentique
#   R1-F3      Backoff natif Python 10s/20s/40s (tenacity optionnel)
#   R1-F4      Fallback Haiku → GPT-4o-mini → Ollama sur 529/503
#   R1A3-NEW-3 Circuit-breaker cb_load/cb_fail/cb_ok/cb_active
#   R6-NEW-2   Validation schéma deltas — listes garanties
#   R6-NEW-3   TAVILY_MAX configurable via .env
#   R6-NEW-4   Robustesse delta JSON malformé
#   FIX-OBS1   Logging structuré via logger nommé
#   VISUAL-R1  extract_metrics_from_report() + save vers SentinelDB
#
# v3.41 :
#   API-FIX1   extract_metrics_from_report() : date_str optionnel
#   API-FIX2   extract_metrics_from_report() : retourne dict
#   API-FIX3   cleanup_dangling_tmp() exposé pour sentinel_main.py
#
# v3.50 :
#   API-50-FIX1  run_sentinel() : paramètre report_type="daily" ajouté
#   API-50-FIX2  run_sentinel_monthly() : nouvelle fonction
#   API-50-FIX3  Regex extract_metrics_from_report() : patterns corrigés
#   API-50-FIX4  _build_call_api() / _build_call_with_fallback() factorisés
#
# v3.51 :
#   API-51-FIX1  Timeout Ollama configurable via OLLAMA_TIMEOUT (.env)
#
# v3.52 :
#   API-52-FIX1  Regex reconstruites via chr(92) — anti-copier-coller
#                Bug résiduel _PAT_INDICE corrigé : [^d]* absorbe le
#                texte intermédiaire ("d'activité", "global", etc.)
#   API-52-FIX2  Prompts chargés en LAZY — plus de SystemExit à l'import
#   API-52-FIX3  GPT-4o-mini fallback : _sanitize_messages_for_openai()
#   API-52-FIX4  Circuit-breaker auto-reset temporel après _CB_RESET_H
#   API-52-FIX5  OPENAI_FALLBACK_MODEL configurable via ENV
#   API-52-FIX6  _sanitize_tool_result_content() : str | list | dict → str
#
# v3.60 — Audit complet inter-scripts (corrections audit 2026-04) :
#   API-60-FIX1  Chemins absolus : _PROJECT_ROOT + _CB_PATH + _PROMPTS_DIR
#                Avant : Path("data"), Path("prompts") — relatifs au CWD.
#                En cron ou appel depuis sous-dossier → mauvais répertoire.
#                Correction : Path(__file__).resolve().parent partout.
#
#   API-60-FIX2  BUG-CRIT-1/2 corrigé : run_sentinel() reçoit save_metrics=True
#                sentinel_main.py doit passer save_metrics=False et capturer
#                memory_deltas (plus de "_") pour éviter double savemetrics()
#                et double extract_memory_delta().
#                Le tuple de retour reste (str, dict) pour compatibilité.
#                Voir docstring run_sentinel() pour le contrat exact.
#
#   API-60-FIX3  load_dotenv() déplacé : appelé UNIQUEMENT si __name__=="__main__"
#                ou si la variable SENTINEL_API_STANDALONE=1 est définie.
#                Avant : load_dotenv() au niveau module → appelé à chaque import
#                par sentinel_main.py, memory_manager.py, etc. — surcharge I/O.
#
#   API-60-FIX4  cleanup_dangling_tmp() : répertoires par défaut étendus
#                ["data", "output"] + utilise _PROJECT_ROOT.
#
#   API-60-FIX5  run_sentinel_monthly() : extrait et retourne aussi memory_deltas
#                La v3.52 retournait toujours {} en cas de succès mensuel.
#
#   API-60-FIX6  Validation avertissement modèles Claude au 1er appel.
#                "claude-sonnet-4-6" / "claude-haiku-4-5" sont validés vs
#                le pattern Anthropic (avertissement si format inhabituel).
#
#   API-60-FIX7  datetime.now(timezone.utc) systématique — zéro datetime naïf.
#
#   API-60-FIX8  _MONTHLY_PROMPT_FALLBACK : placeholder INJECTION_MENSUELLE
#                (underscore) ajouté en priorité — INJECTIONMENSUELLE (legacy)
#                conservé en fallback. Alignement avec init_prompts.py v3.54.
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

# ── API-60-FIX3 : load_dotenv() uniquement en standalone, pas à l'import ─────
if os.environ.get("SENTINEL_API_STANDALONE") or __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

# ── API-60-FIX1 : Chemins absolus ─────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent
_PROMPTS_DIR  = _PROJECT_ROOT / "prompts"
_DATA_DIR     = _PROJECT_ROOT / "data"
_OUTPUT_DIR   = _PROJECT_ROOT / "output"

log = logging.getLogger("sentinel.api")

VERSION = "3.60"

# =============================================================================
# CONSTANTES — lues depuis .env (pas de valeurs capturées à l'import pour
# les constantes qui pourraient changer entre tests sans redémarrage)
# =============================================================================

def _get_sentinel_model() -> str:
    return os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")

def _get_haiku_model() -> str:
    return os.environ.get("HAIKU_MODEL", "claude-haiku-4-5")

def _get_openai_fallback_model() -> str:
    return os.environ.get("OPENAI_FALLBACK_MODEL", "gpt-4o-mini")

# Accès direct conservés pour rétrocompatibilité
SENTINEL_MODEL        = os.environ.get("SENTINEL_MODEL",        "claude-sonnet-4-6")
HAIKU_MODEL           = os.environ.get("HAIKU_MODEL",           "claude-haiku-4-5")
OPENAI_FALLBACK_MODEL = os.environ.get("OPENAI_FALLBACK_MODEL", "gpt-4o-mini")

# ── Paramètres API ────────────────────────────────────────────────────────────
SENTINEL_MAX_TOKENS = int(os.environ.get("SENTINEL_MAX_TOKENS", "16000"))
TAVILY_MAX          = int(os.environ.get("SENTINEL_TAVILY_MAX", "5"))

# ── Circuit-breaker ───────────────────────────────────────────────────────────
_CB_PATH    = _DATA_DIR / "api_failures.json"   # API-60-FIX1 : chemin absolu
_CB_MAX     = int(os.environ.get("SENTINEL_CB_MAX",     "3"))
_CB_RESET_H = int(os.environ.get("SENTINEL_CB_RESET_H", "2"))

# ── Ollama ────────────────────────────────────────────────────────────────────
OLLAMA_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", "180"))

# =============================================================================
# API-60-FIX6 — Validation du format des noms de modèles Anthropic
# =============================================================================

_MODEL_WARNED: set[str] = set()

def _warn_model_name(model: str) -> None:
    """
    Émet un avertissement unique si le nom de modèle ne ressemble pas
    à un identifiant Anthropic valide.
    """
    if model in _MODEL_WARNED:
        return
    _MODEL_WARNED.add(model)
    if not model.startswith("claude-") and not model.startswith("gpt-"):
        log.warning(
            f"API-60-FIX6 Nom de modèle inhabituel : {model!r}. "
            f"Vérifier que ce modèle est disponible sur l'API Anthropic. "
            f"Format attendu : claude-* ou gpt-*."
        )

# =============================================================================
# API-52-FIX1 — REGEX BACKSLASH-SAFE pour extract_metrics_from_report()
# chr(92) = ""  —  même protection anti-copier-coller que :
#   TG-R1 (telegram_scraper.py) / NLP-R1 (nlp_scorer.py) / MAIL-R1 (mailer.py)
# =============================================================================
_BS = chr(92)  # ""

# Pattern : Indice[^d]*([d]+(?:.[d]+)?)s*/?s*10
_PAT_INDICE = re.compile(
    r"Indice"
    + "[^" + _BS + "d]*"
    + "([" + _BS + "d]+(?:"
    + _BS + ".[" + _BS + "d]+)?)"
    + _BS + "s*/?"
    + _BS + "s*10",
    re.IGNORECASE,
)

# Pattern : Alertes*[:-]?s*(VERT|ORANGE|ROUGE)
_PAT_ALERTE = re.compile(
    r"Alerte"
    + _BS + r"s*[:-]?"
    + _BS + r"s*(VERT|ORANGE|ROUGE)",
    re.IGNORECASE,
)

# Pattern : Sourcess+analys[eé]es?s*[:-]?s*([d]+)
_PAT_SOURCES = re.compile(
    r"Sources"
    + _BS + r"s+analys[eé]es?"
    + _BS + r"s*[:-]?"
    + _BS + r"s*([" + _BS + r"d]+)",
    re.IGNORECASE,
)

# Pattern : Pertinentes?s*[:-]?s*([d]+)
_PAT_PERTINENTS = re.compile(
    r"Pertinentes?"
    + _BS + r"s*[:-]?"
    + _BS + r"s*([" + _BS + r"d]+)",
    re.IGNORECASE,
)

def _make_pat_count(alias: str) -> re.Pattern:
    """
    Construit dynamiquement un pattern count sécurisé via chr(92).
    Accepte chiffres seuls ou chiffres suivis de "%" ou "." (pourcentages).
    """
    return re.compile(
        re.escape(alias)
        + _BS + r"s*[:%]?"
        + _BS + r"s*([" + _BS + r"d]+(?:"
        + _BS + r"." + _BS + r"d+)?)",
        re.IGNORECASE,
    )

# =============================================================================
# UTILITAIRE — Nettoyage fichiers .tmp résiduels (API-FIX3 / API-60-FIX4)
# =============================================================================

def cleanup_dangling_tmp(
    directories: list[str] | None = None,
    max_age_h: int = 24,
) -> int:
    """
    Supprime les fichiers .tmp résiduels laissés par un crash système.
    Appelé en tête de sentinel_main.py après initdb().

    API-60-FIX4 : répertoires par défaut étendus ["data", "output"].
                  Utilise _PROJECT_ROOT pour chemins absolus.

    Retourne le nombre de fichiers supprimés.
    """
    if directories is None:
        directories = ["data", "output"]
    cutoff  = time.time() - max_age_h * 3600
    removed = 0
    for directory in directories:
        d = _PROJECT_ROOT / directory
        if not d.is_dir():
            continue
        for tmp_file in d.glob("*.tmp"):
            try:
                if tmp_file.stat().st_mtime < cutoff:
                    tmp_file.unlink(missing_ok=True)
                    removed += 1
                    log.info(f"cleanup_dangling_tmp : supprimé {tmp_file.name}")
            except OSError as e:
                log.warning(
                    f"cleanup_dangling_tmp : impossible de supprimer "
                    f"{tmp_file.name} : {e}"
                )
    if removed:
        log.info(
            f"cleanup_dangling_tmp : {removed} fichier(s) .tmp "
            f"résiduel(s) supprimé(s)"
        )
    return removed

# =============================================================================
# CHARGEMENT DES PROMPTS — LAZY (API-52-FIX2 / API-60-FIX1)
# =============================================================================

def _load_prompt(filename: str) -> str:
    """
    Charge un fichier prompt depuis ./prompts/ (chemin absolu).
    Lève SystemExit explicite si absent.
    API-60-FIX1 : utilise _PROMPTS_DIR (absolu) au lieu de Path("prompts").
    """
    p = _PROMPTS_DIR / filename
    if not p.exists():
        log.critical(
            f"PROMPT {p} introuvable — lancer init_prompts.py en premier."
        )
        raise SystemExit(
            f"ERREUR : {p} manquant. Exécuter : python init_prompts.py"
        )
    return p.read_text(encoding="utf-8")

def _load_prompt_optional(
    filename: str,
    fallback_filename: str,
) -> tuple[str, str]:
    """
    Charge filename si présent, sinon fallback_filename.
    Retourne (contenu, nom_du_fichier_effectivement_chargé).
    Ne lève jamais SystemExit — le fallback est garanti d'exister.
    API-60-FIX1 : utilise _PROMPTS_DIR (absolu).
    """
    p = _PROMPTS_DIR / filename
    if p.exists():
        log.debug(f"PROMPT chargé : {filename}")
        return p.read_text(encoding="utf-8"), filename
    log.info(
        f"PROMPT {filename} absent — fallback {fallback_filename} utilisé. "
        f"Créer prompts/{filename} pour personnaliser ce type de rapport."
    )
    return _load_prompt(fallback_filename), fallback_filename

# ── Cache lazy (API-52-FIX2) ─────────────────────────────────────────────────
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
    SYSTEM_PROMPT / USER_PROMPT_TEMPLATE chargés à la 1ère lecture, pas à l'import.
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
    API-60-FIX1 : _CB_PATH est absolu.
    """
    try:
        if _CB_PATH.exists():
            data         = json.loads(_CB_PATH.read_text(encoding="utf-8"))
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
    """Sauvegarde atomique write-then-rename."""
    _CB_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CB_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(_CB_PATH)
    except OSError as e:
        log.warning(f"CIRCUIT-BREAKER impossible de sauvegarder état : {e}")

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
        client  = TavilyClient(api_key=os.environ.get("TAVILY_API_KEY", ""))
        results = client.search(query, max_results=3)
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

    IMPORTANT : sentinel_main.py doit utiliser les deltas retournés
    par run_sentinel() au lieu d'appeler cette fonction séparément.
    Voir BUG-CRIT-2 dans l'audit et docstring run_sentinel().
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
            "MEMORY Aucun bloc JSON delta — "
            "enrichissement via compression Haiku uniquement"
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
# EXTRACT METRICS FROM REPORT (VISUAL-R1 / API-52-FIX1 / API-60-FIX2)
# =============================================================================

def extract_metrics_from_report(
    report_text: str,
    date_str:    str | None = None,
    *,
    save: bool = True,
) -> dict:
    """
    Extrait les métriques structurées du MODULE 1 et les enregistre
    dans SentinelDB via savemetrics().

    API-FIX1    — date_str optionnel (défaut : date UTC du jour).
    API-FIX2    — Retourne le dict (était None).
    API-52-FIX1 — Toutes les regex reconstruites via chr(92).
    API-60-FIX2 — Paramètre save=True : passer save=False si l'appelant
                  (sentinel_main.py) gère lui-même la sauvegarde, pour
                  éviter le double savemetrics() (BUG-CRIT-1).
    API-60-FIX7 — datetime.now(timezone.utc) — jamais naïf.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).date().isoformat()

    metrics: dict = {}

    try:
        # ── Indice d'activité ─────────────────────────────────────────────
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

        if metrics and save:
            from db_manager import SentinelDB
            SentinelDB.savemetrics(date_str, **metrics)
            log.info(f"METRICS {len(metrics)} champs enregistrés pour {date_str}")
        elif metrics and not save:
            log.debug(
                f"METRICS {len(metrics)} champs extraits pour {date_str} "
                f"(save=False — appelant gère la sauvegarde)"
            )
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
    Seuil SOFT : > 3 modules manquants → WARNING (dégradation gracieuse).
    """
    missing    = []
    text_upper = report_text.upper()
    for module in _REQUIRED_MODULES:
        if module not in text_upper:
            missing.append(module)
    return len(missing) <= 3, missing

# =============================================================================
# API-52-FIX3 + API-52-FIX6 — NETTOYAGE MESSAGES ANTHROPIC → FORMAT OPENAI
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
    Filtre les blocs tool_use / tool_result incompatibles → erreur 400.
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
                if hasattr(block, "type"):
                    if block.type == "text" and getattr(block, "text", ""):
                        parts.append(block.text)
                    continue
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

            text = "
".join(p for p in parts if p).strip()
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
    API-60-FIX6 : _warn_model_name() au 1er appel.
    Modèles relus au moment de la construction (pas à l'import du module).
    """
    _sonnet_model = _get_sentinel_model()
    _haiku_model  = _get_haiku_model()
    _openai_model = _get_openai_fallback_model()

    _warn_model_name(_sonnet_model)
    _warn_model_name(_haiku_model)

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
                f"MODE DÉGRADÉ Anthropic surchargé HTTP {status} "
                f"— bascule {_haiku_model}"
            )
            cb_fail()

            # ── Tentative Haiku ───────────────────────────────────────────
            haiku_kw               = dict(kw)
            haiku_kw["model"]      = _haiku_model
            haiku_kw["max_tokens"] = min(kw.get("max_tokens", 8000), 8000)
            haiku_kw.pop("tools", None)
            try:
                resp = call_api(**haiku_kw)
                log.warning("MODE DÉGRADÉ Rapport Haiku généré — qualité réduite")
                return resp
            except Exception as he:
                log.warning(
                    f"MODE DÉGRADÉ Haiku échoue : {he} "
                    f"— tentative {_openai_model}"
                )

            # ── Fallback OpenAI (API-52-FIX3 / API-52-FIX5) ──────────────
            try:
                import openai  # type: ignore

                oc           = openai.OpenAI()
                raw_messages = list(kw.get("messages", []))
                clean_msgs   = _sanitize_messages_for_openai(raw_messages)
                sys_msg      = {"role": "system", "content": kw.get("system", "")}

                r = oc.chat.completions.create(
                    model      = _openai_model,
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
                    f"MODE DÉGRADÉ {_openai_model} utilisé — pipeline maintenu"
                )
                return _FallbackResp(txt)  # type: ignore[return-value]

            except Exception as e2:
                log.warning(
                    f"MODE DÉGRADÉ {_openai_model} échoue : {e2} "
                    f"— tentative Ollama"
                )

            # ── Fallback Ollama local (F-03-FIX / API-51-FIX1) ───────────
            try:
                import urllib.request
                import json as _json

                ollama_url   = os.environ.get("OLLAMA_URL",   "http://localhost:11434")
                ollama_model = os.environ.get("OLLAMA_MODEL", "mistral:7b")

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

                raw_messages = list(kw.get("messages", []))
                clean_msgs   = _sanitize_messages_for_openai(raw_messages)
                sys_content  = kw.get("system", "")
                user_content = "
".join(
                    m["content"] for m in clean_msgs
                    if m.get("role") == "user" and m.get("content")
                )

                payload = _json.dumps({
                    "model":   ollama_model,
                    "prompt":  f"{sys_content}

{user_content}",
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
                    f"Anthropic + {_openai_model} + Ollama indisponibles : "
                    f"{e} / {e2} / {e3}"
                ) from e

    return call_with_fallback

# =============================================================================
# RUN SENTINEL — PIPELINE QUOTIDIEN (API-60-FIX2)
# =============================================================================

def run_sentinel(
    articles_text:  str,
    memory_context: str,
    date_obj=None,
    model:        str | None = None,
    report_type:  str        = "daily",
    save_metrics: bool       = True,
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
    save_metrics   : [API-60-FIX2] Si True (défaut), sauvegarde les métriques
                     en DB ICI. Passer save_metrics=False si sentinel_main.py
                     appelle extract_metrics_from_report() séparément pour
                     éviter BUG-CRIT-1 (double savemetrics()).

    Retourne
    --------
    (report_text: str, memory_deltas: dict)

    ⚠️  CONTRAT sentinel_main.py (BUG-CRIT-2) :
        Capturer memory_deltas au lieu de les ignorer avec "_" :
            report_text, memory_deltas = _timed("claude_daily", run_sentinel, ...)
        Puis passer memory_deltas à _process_report_data_to_db() sans
        rappeler extract_memory_delta(report_text).
    """
    if date_obj is None:
        date_obj = datetime.now(timezone.utc).date()  # API-60-FIX7

    active_model = model or _get_sentinel_model()
    client       = anthropic.Anthropic()

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
        # Nouvelle convention avec underscores (init_prompts v3.54)
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
            system     = _get_system_prompt(),
            tools      = tools if not cb_active() else [],
            messages   = messages,
        )
        resp = call_with_fallback(**kw)

        if resp.stop_reason == "end_turn":
            break

        if resp.stop_reason == "tool_use":
            tool_results = []
            for block in resp.content:
                if (
                    hasattr(block, "type")
                    and block.type == "tool_use"
                    and block.name == "web_search"
                ):
                    if tavily_calls >= TAVILY_MAX:
                        result = json.dumps(
                            {"error": f"quota {TAVILY_MAX} appels Tavily atteint"}
                        )
                        log.info(
                            f"TAVILY quota {TAVILY_MAX} atteint "
                            f"— résultat vide injecté"
                        )
                    else:
                        tavily_calls += 1
                        query  = block.input.get("query", "")
                        result = do_web_search(query)
                        log.info(
                            f"TAVILY [{tavily_calls}/{TAVILY_MAX}] "
                            f"query={query!r}"
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
    _empty_deltas = {
        "nouvelles_tendances": [],
        "alertes_ouvertes":    [],
        "alertes_closes":      [],
    }

    if resp is None:
        log.error("API Réponse vide après boucle agentique — rapport non généré")
        return "", _empty_deltas

    report_text = next(
        (b.text for b in resp.content if hasattr(b, "text") and b.text),
        "",
    )
    if not report_text.strip():
        log.error("API Rapport Claude vide")
        return "", _empty_deltas

    log.info(
        f"API Rapport généré {len(report_text)} chars "
        f"| model={kw.get('model', '?')} "
        f"| report_type={report_type!r} "
        f"| {tavily_calls} appel(s) Tavily"
    )

    # ── Validation structure ──────────────────────────────────────────────────
    is_valid, missing = _validate_report_structure(report_text)
    if not is_valid:
        log.warning(
            f"C3 STRUCTURE RAPPORT INCOMPLÈTE : {len(missing)} modules "
            f"manquants : {missing}. Dégradation gracieuse — rapport utilisé."
        )
    else:
        log.info(
            f"C3 Structure valide "
            f"({len(_REQUIRED_MODULES) - len(missing)}/{len(_REQUIRED_MODULES)} "
            f"modules présents)"
        )

    # ── Extraction des deltas mémoire (API-60-FIX2) ───────────────────────────
    # sentinel_main.py DOIT capturer ces deltas (pas "_") — voir docstring.
    memory_deltas = extract_memory_delta(report_text)

    # ── Extraction des métriques (API-60-FIX2) ────────────────────────────────
    # save=save_metrics : passer save_metrics=False depuis sentinel_main.py
    # pour éviter le double savemetrics() (BUG-CRIT-1).
    extract_metrics_from_report(report_text, str(date_obj), save=save_metrics)

    return report_text, memory_deltas

# =============================================================================
# RUN SENTINEL MONTHLY — RAPPORT MENSUEL (API-50-FIX2 / API-60-FIX5/8)
# =============================================================================

# Prompt minimal intégré — utilisé si prompts/monthly.txt est absent.
# API-60-FIX8 : INJECTION_MENSUELLE (underscore) en priorité.
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
    Architecture single-turn : données déjà compressées par Haiku en MAP.
    Même circuit-breaker et fallback que run_sentinel().

    API-60-FIX5 : extrait et retourne memory_deltas (était toujours {}).
    API-60-FIX8 : INJECTION_MENSUELLE (underscore) en priorité sur
                  INJECTIONMENSUELLE (legacy). Alignement init_prompts v3.54.

    Retourne : (report_text: str, memory_deltas: dict)
    """
    active_model = model or _get_sentinel_model()
    client       = anthropic.Anthropic()

    # Charger le prompt mensuel — fallback intégré si absent
    monthly_path = _PROMPTS_DIR / "monthly.txt"  # API-60-FIX1 : absolu
    if monthly_path.exists():
        monthly_template = monthly_path.read_text(encoding="utf-8")
        log.debug("MONTHLY prompt chargé depuis prompts/monthly.txt")
    else:
        monthly_template = _MONTHLY_PROMPT_FALLBACK
        log.info(
            "MONTHLY prompts/monthly.txt absent — prompt minimal intégré utilisé. "
            "Créer prompts/monthly.txt pour un meilleur contrôle du format."
        )

    # [API-60-FIX8] Priorité : INJECTION_MENSUELLE (underscore) → INJECTIONMENSUELLE (legacy)
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
        log.warning(
            "MONTHLY Aucun placeholder INJECTION_MENSUELLE dans le template "
            "— injection en fin de prompt (format dégradé)"
        )
        user_content = (
            monthly_template.replace("MOIS", mois)
            + f"

---
RÉSUMÉS HEBDOMADAIRES ({mois}) :

{injection}"
        )

    call_api           = _build_call_api(client)
    call_with_fallback = _build_call_with_fallback(call_api)

    kw = dict(
        model      = HAIKU_MODEL if cb_active() else active_model,
        max_tokens = SENTINEL_MAX_TOKENS,
        system     = _get_system_prompt(),
        messages   = [{"role": "user", "content": user_content}],
        # Pas de tools — données déjà compressées par Haiku en phase MAP
    )

    _empty_deltas = {
        "nouvelles_tendances": [],
        "alertes_ouvertes":    [],
        "alertes_closes":      [],
    }

    try:
        resp = call_with_fallback(**kw)
    except Exception as e:
        log.error(f"run_sentinel_monthly : erreur Claude : {e}", exc_info=True)
        cb_fail()
        return "", _empty_deltas

    report_text = next(
        (b.text for b in resp.content if hasattr(b, "text") and b.text),
        "",
    )

    if not report_text.strip():
        log.error("run_sentinel_monthly : rapport vide retourné par Claude")
        return "", _empty_deltas

    log.info(
        f"MONTHLY Rapport mensuel généré : {len(report_text)} chars "
        f"| model={kw['model']} | mois={mois}"
    )

    # API-60-FIX5 : extraire les deltas mémoire du rapport mensuel
    memory_deltas = extract_memory_delta(report_text)
    if any(memory_deltas.values()):
        log.info(
            f"MONTHLY Deltas mémoire extraits : "
            f"{len(memory_deltas['nouvelles_tendances'])} tendances, "
            f"{len(memory_deltas['alertes_ouvertes'])} alertes ouvertes, "
            f"{len(memory_deltas['alertes_closes'])} alertes closes"
        )

    return report_text, memory_deltas

# =============================================================================
# SELFTEST — CLI
# =============================================================================

if __name__ == "__main__":
    import sys

    os.environ["SENTINEL_API_STANDALONE"] = "1"
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level   = logging.DEBUG,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )
    log.info(f"sentinel_api.py v{VERSION} — selftest")
    log.info(f"PROJECT_ROOT : {_PROJECT_ROOT}")
    log.info(f"PROMPTS_DIR  : {_PROMPTS_DIR} (exists={_PROMPTS_DIR.exists()})")
    log.info(f"DATA_DIR     : {_DATA_DIR}    (exists={_DATA_DIR.exists()})")
    log.info(f"SENTINEL_MODEL = {_get_sentinel_model()!r}")
    log.info(f"HAIKU_MODEL    = {_get_haiku_model()!r}")
    log.info(f"CB_ACTIVE      = {cb_active()}")

    _test_report = """
    MODULE 1 — SYNTHÈSE
    Indice d'activité global : 7.5/10
    Alerte : ORANGE
    Sources analysées : 123
    Sources pertinentes : 45
    Répartition géographique : USA 35%, Europe 28%, Asie 18%, Russie 12%
    Domaines : Terrestre 42%, Maritime 31%, Transverse 18%, Contractuel 9%
    DEBUTJSONDELTA
    {"nouvelles_tendances": ["Drone FPV"], "alertes_ouvertes": [], "alertes_closes": []}
    FINJSONDELTA
    """
    metrics = extract_metrics_from_report(_test_report, save=False)
    assert metrics.get("indice")      == 7.5,      f"indice KO : {metrics}"
    assert metrics.get("alerte")      == "ORANGE",  f"alerte KO : {metrics}"
    assert metrics.get("nb_articles") == 123,       f"nb_articles KO : {metrics}"
    assert metrics.get("geousa")      == 35.0,      f"geousa KO : {metrics}"
    log.info(f"✓ extract_metrics_from_report : {metrics}")

    deltas = extract_memory_delta(_test_report)
    assert deltas["nouvelles_tendances"] == ["Drone FPV"], f"deltas KO : {deltas}"
    log.info(f"✓ extract_memory_delta : {deltas}")

    n = cleanup_dangling_tmp(max_age_h=0)
    log.info(f"✓ cleanup_dangling_tmp : {n} fichier(s) supprimé(s)")

    log.info("Tous les selftests passent ✓")
    sys.exit(0)
