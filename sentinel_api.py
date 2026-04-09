# sentinel_api.py — SENTINEL v3.40 — Appel Claude Sonnet + outil Tavily
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   A07-FIX     call_api() définie UNE SEULE FOIS hors boucle agentique
#   A31-FIX     SENTINEL_MODEL via var ENV anti-drift (CODE-5)
#   CODE-R5     Bloc DEDUP mort supprimé de la boucle agentique
#   R1-F3       Backoff natif Python 10s/20s/40s (tenacity optionnel)
#   R1-F4       Fallback Haiku-4.5 sur 529/503, puis GPT-4o-mini
#   R1A3-NEW-3  Circuit-breaker cb_load/cb_fail/cb_ok/cb_active
#   R6-NEW-2    Validation schéma deltas — listes garanties même si Claude
#               retourne string ou None
#   R6-NEW-3    TAVILY_MAX configurable via .env
#   R6-NEW-4    Robustesse delta JSON malformé (tous les cas de test couverts)
#   FIX-OBS1    Logging structuré via logger nommé, plus de print()
#   VISUAL-R1   extract_metrics_from_report() + save vers SentinelDB
#               remplace l'extraction regex HTML dans dashboard.py
# ─────────────────────────────────────────────────────────────────────────────
# Dépendances obligatoires : anthropic
# Optionnelles : tavily-python (vérif web), tenacity (retry avancé),
#                openai (fallback GPT-4o-mini), python-dotenv
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Optional

import anthropic

# ── Logger structuré (FIX-OBS1 — remplace print) ─────────────────────────────
log = logging.getLogger("sentinel.api")

# ── Modèles (A31-FIX / CODE-5 — anti-drift, configurable via .env) ───────────
SENTINEL_MODEL = os.environ.get("SENTINEL_MODEL", "claude-sonnet-4-6")
HAIKU_MODEL    = os.environ.get("HAIKU_MODEL",    "claude-haiku-4-5")

# ── Paramètres API ────────────────────────────────────────────────────────────
SENTINEL_MAX_TOKENS = int(os.environ.get("SENTINEL_MAX_TOKENS", "16000"))  # E1-FIX
TAVILY_MAX          = int(os.environ.get("SENTINEL_TAVILY_MAX", "5"))      # R6-NEW-3

# ── Chemins circuit-breaker ───────────────────────────────────────────────────
_CB_PATH  = Path("data") / "api_failures.json"
_CB_MAX   = int(os.environ.get("SENTINEL_CB_MAX", "3"))  # CODE-6

# ── Chargement des prompts ────────────────────────────────────────────────────
def _load_prompt(filename: str) -> str:
    """
    Charge un fichier prompt depuis ./prompts/.
    Lève SystemExit explicite avec message clair si absent (B9 / F-3).
    """
    p = Path("prompts") / filename
    if not p.exists():
        log.critical(f"PROMPT {p} introuvable — lancer initprompts.py en premier.")
        raise SystemExit(f"ERREUR : {p} manquant. Exécuter : python initprompts.py")
    return p.read_text(encoding="utf-8")


SYSTEM_PROMPT        = _load_prompt("system.txt")
USER_PROMPT_TEMPLATE = _load_prompt("daily.txt")

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


# ─────────────────────────────────────────────────────────────────────────────
# CIRCUIT-BREAKER (R1A3-NEW-3 / CODE-6)
# ─────────────────────────────────────────────────────────────────────────────

def _cb_load() -> dict:
    """Charge le compteur d'échecs API depuis le fichier JSON."""
    try:
        if _CB_PATH.exists():
            return json.loads(_CB_PATH.read_text())
    except Exception:
        pass
    return {"count": 0, "active": False}


def _cb_save(data: dict) -> None:
    _CB_PATH.parent.mkdir(exist_ok=True)
    tmp = _CB_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(_CB_PATH)


def cb_fail() -> bool:
    """
    Enregistre un échec API.
    Retourne True si le circuit est désormais ouvert (trop d'échecs).
    R1A3-NEW-3 / CODE-6
    """
    d = _cb_load()
    d["count"] = d.get("count", 0) + 1
    d["active"] = d["count"] >= _CB_MAX
    if d["active"]:
        log.error(
            f"CIRCUIT-BREAKER {d['count']} échecs consécutifs "
            f"— bascule Haiku-only (seuil={_CB_MAX})"
        )
    _cb_save(d)
    return d["active"]


def cb_ok() -> None:
    """Remet le circuit-breaker à zéro après un succès. R1A3-NEW-3"""
    _cb_save({"count": 0, "active": False})


def cb_active() -> bool:
    """True si le circuit est ouvert (trop d'échecs récents). R1A3-NEW-3"""
    return _cb_load().get("active", False)


# ─────────────────────────────────────────────────────────────────────────────
# TAVILY WEB SEARCH
# ─────────────────────────────────────────────────────────────────────────────

def do_web_search(query: str) -> str:
    """
    Appel Tavily API avec troncature résultats pour éviter overflow contexte.
    Retourne du JSON string (résultats ou erreur).
    """
    try:
        from tavily import TavilyClient  # type: ignore
        client  = TavilyClient(api_key=os.environ.get("TAVILY_API_KEY", ""))
        results = client.search(query, max_results=3)
        truncated = [
            {
                "title":   r.get("title", ""),
                "url":     r.get("url",   ""),
                "content": r.get("content", "")[:800],
            }
            for r in results.get("results", [])[:3]
        ]
        return json.dumps(truncated, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT MEMORY DELTA (R6-NEW-2 / R6-NEW-4)
# ─────────────────────────────────────────────────────────────────────────────

def extract_memory_delta(report_text: str) -> dict:
    """
    Extrait les mises à jour mémoire depuis la section 9 du rapport.
    Le prompt demande à Claude de produire un bloc JSON entre
    DEBUTJSONDELTA … FINJSONDELTA.

    R6-NEW-2 : validation schéma — listes garanties même si Claude retourne
               un string, None ou un entier à la place d'une liste.
    R6-NEW-4 : robustesse totale — JSON malformé, marqueurs absents,
               clés manquantes → retourne toujours le même dict vide.
    """
    _empty: dict = {
        "nouvelles_tendances": [],
        "alertes_ouvertes":    [],
        "alertes_closes":      [],
    }

    # Chercher le bloc entre marqueurs
    match = re.search(
        r"DEBUTJSONDELTA(.+?)FINJSONDELTA",
        report_text,
        re.DOTALL,
    )
    if not match:
        log.debug("MEMORY Aucun bloc JSON delta — enrichissement via compression Haiku uniquement")
        return _empty

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        log.warning(f"MEMORY JSON delta invalide : {e}")
        return _empty

    # R6-NEW-2 : forcer les listes même si Claude retourne string/None/entier
    def _safe_list(val, default: list) -> list:
        if isinstance(val, list):
            return val
        if val and isinstance(val, str):
            return [val]  # chaîne isolée → liste à un élément
        return default

    return {
        "nouvelles_tendances": _safe_list(data.get("nouvelles_tendances"), []),
        "alertes_ouvertes":    _safe_list(data.get("alertes_ouvertes"),    []),
        "alertes_closes":      _safe_list(data.get("alertes_closes"),      []),
    }


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT METRICS FROM REPORT (VISUAL-R1 — NEW v3.40)
# ─────────────────────────────────────────────────────────────────────────────

def extract_metrics_from_report(report_text: str, date_str: str) -> None:
    """
    Extrait les métriques structurées du MODULE 1 du rapport Claude et les
    enregistre dans SentinelDB via save_metrics().

    VISUAL-R1 : élimine l'extraction regex fragile dans dashboard.py.
    Dégradation gracieuse — si extraction impossible, ne bloque pas le pipeline.
    """
    try:
        from db_manager import SentinelDB

        metrics: dict = {}

        # ── Indice d'activité ─────────────────────────────────────────────
        m = re.search(r"Indice\s*[:\-]?\s*([\d]+(?:\.[\d]+)?)\s*/?\s*10",
                      report_text, re.IGNORECASE)
        if m:
            metrics["indice"] = float(m.group(1))

        # ── Niveau alerte ─────────────────────────────────────────────────
        m = re.search(r"Alerte\s*[:\-]?\s*(VERT|ORANGE|ROUGE)",
                      report_text, re.IGNORECASE)
        if m:
            metrics["alerte"] = m.group(1).upper()

        # ── Sources analysées / pertinentes ───────────────────────────────
        m = re.search(r"Sources\s+analys[eé]es?\s*[:\-]?\s*(\d+)",
                      report_text, re.IGNORECASE)
        if m:
            metrics["nb_articles"] = int(m.group(1))

        m = re.search(r"Pertinentes?\s*[:\-]?\s*(\d+)",
                      report_text, re.IGNORECASE)
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
                mg = re.search(
                    rf"{re.escape(alias)}\s*[:\-]?\s*(\d+)",
                    report_text, re.IGNORECASE
                )
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
                md = re.search(
                    rf"{re.escape(alias)}\s*[:\-]?\s*(\d+)",
                    report_text, re.IGNORECASE
                )
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


# ─────────────────────────────────────────────────────────────────────────────
# RUN SENTINEL — FONCTION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION STRUCTURE RAPPORT (C3-FIX)
# ─────────────────────────────────────────────────────────────────────────────

_REQUIRED_MODULES = [
    "RÉSUMÉ EXÉCUTIF", "MODULE 1", "MODULE 2", "MODULE 3",
    "MODULE 4", "MODULE 5", "MODULE 6", "MODULE 7", "MODULE 8", "MODULE 9",
]

def _validate_report_structure(report_text: str) -> tuple[bool, list[str]]:
    """
    C3-FIX : vérifie que les 9 modules + résumé sont présents dans le rapport.
    Retourne (is_valid, missing_modules).
    Si >3 modules manquants, le rapport est considéré invalide.
    """
    missing = []
    text_upper = report_text.upper()
    for module in _REQUIRED_MODULES:
        if module not in text_upper:
            missing.append(module)
    is_valid = len(missing) <= 3
    return is_valid, missing


def run_sentinel(
    articles_text: str,
    memory_context: str,
    date_obj,
) -> tuple[str, dict]:
    """
    Lance l'analyse SENTINEL complète.

    Paramètres
    ----------
    articles_text  : str   Articles formatés par format_for_claude()
    memory_context : str   Mémoire compressée des 7 derniers jours
    date_obj       : date  Date du rapport (datetime.date)

    Retourne
    --------
    (report_text: str, memory_deltas: dict)

    Corrections
    -----------
    A07-FIX    call_api() définie UNE SEULE FOIS hors de la boucle agentique
    CODE-R5    Bloc DEDUP mort supprimé de la boucle
    R1-F3      Backoff natif 10s/20s/40s + tenacity si disponible
    R1-F4      Fallback Haiku-4.5 sur 529/503, puis GPT-4o-mini
    R1A3-NEW-3 Circuit-breaker intégré
    """
    client = anthropic.Anthropic()  # lit ANTHROPIC_API_KEY depuis l'env

    # ── Vérifier Tavily ───────────────────────────────────────────────────────
    tavily_available = bool(os.environ.get("TAVILY_API_KEY"))

    # ── Construire le prompt utilisateur ─────────────────────────────────────
    user_prompt = (
        USER_PROMPT_TEMPLATE
        .replace("DATEAUJOURDHUI",           date_obj.strftime("%d/%m/%Y"))
        .replace("ARTICLESFILTRESPARSCRAPER", articles_text)
        .replace("MEMOIRECOMPRESSE7JOURS",   memory_context)
        .replace("OUIouNON",                  "OUI" if tavily_available else "NON")
    )

    tools   = [TAVILY_TOOL] if tavily_available else []
    messages = [{"role": "user", "content": user_prompt}]

    # ── call_api() — définie UNE SEULE FOIS hors boucle (A07-FIX) ────────────
    # Tenacity optionnel — backoff natif Python si absent (R1-F3)
    try:
        from tenacity import retry, wait_exponential, stop_after_attempt  # type: ignore
        @retry(wait=wait_exponential(min=10, max=180), stop=stop_after_attempt(3))
        def call_api(**kw):
            return client.messages.create(**kw)
        log.debug("RETRY tenacity activé")
    except ImportError:
        def call_api(**kw):  # type: ignore[no-redef]
            """Backoff natif 10s → 20s → 40s (R1-F3)."""
            for attempt in range(3):
                try:
                    return client.messages.create(**kw)
                except anthropic.APIError as e:
                    if attempt >= 2:
                        raise
                    wait = min(120, 10 * (2 ** attempt))   # 10s, 20s, cap 120s
                    log.warning(f"API tentative {attempt + 1}/3 échouée, retry dans {wait}s : {e}")
                    time.sleep(wait)
            raise RuntimeError("call_api : 3 tentatives épuisées")  # never reached

    # ── call_with_fallback() — Haiku sur 529/503, puis GPT-4o-mini (R1-F4) ───
    def call_with_fallback(**kw) -> anthropic.types.Message:
        """
        Appel Anthropic avec bascule automatique :
          1. Sonnet (nominal)
          2. Haiku-4.5 si OverloadedError (529) ou ServiceUnavailable (503)
          3. GPT-4o-mini si Haiku échoue aussi
        """
        try:
            resp = call_api(**kw)
            cb_ok()  # succès → reset circuit-breaker
            return resp
        except anthropic.APIStatusError as e:
            status = getattr(e, "status_code", 0)
            if status in (529, 503):
                log.warning(f"MODE DÉGRADÉ Anthropic surchargé HTTP {status} — bascule Haiku-4.5")
                cb_fail()
                # Tentative Haiku
                haiku_kw = dict(kw)
                haiku_kw["model"]      = HAIKU_MODEL
                haiku_kw["max_tokens"] = min(kw.get("max_tokens", 8000), 8000)
                haiku_kw.pop("tools", None)  # Haiku ne supporte pas les tools
                try:
                    resp = call_api(**haiku_kw)
                    log.warning("MODE DÉGRADÉ Rapport Haiku généré — qualité réduite")
                    return resp
                except Exception as he:
                    log.warning(f"MODE DÉGRADÉ Haiku échoue aussi : {he} — tentative GPT-4o-mini")
                    # Fallback GPT-4o-mini (R1-F4)
                    try:
                        import openai  # type: ignore
                        oc = openai.OpenAI()
                        sys_msg = {"role": "system", "content": kw.get("system", "")}
                        msgs    = list(kw.get("messages", []))
                        r = oc.chat.completions.create(
                            model      = "gpt-4o-mini",
                            messages   = [sys_msg] + msgs,
                            max_tokens = kw.get("max_tokens", 4096),
                        )
                        txt = r.choices[0].message.content

                        # Créer un objet compatible anthropic.Message
                        class _Block:
                            def __init__(self, text: str):
                                self.type = "text"
                                self.text = text

                        class _FallbackResp:
                            stop_reason = "end_turn"
                            def __init__(self, text: str):
                                self.content = [_Block(text)]

                        log.warning("MODE DÉGRADÉ GPT-4o-mini utilisé — pipeline maintenu")
                        return _FallbackResp(txt)  # type: ignore[return-value]
                    except Exception as e2:
                        # F3-FIX : Ollama local (Mistral 7B) comme 3ème fallback
                        try:
                            import urllib.request, urllib.error
                            ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
                            ollama_model = os.environ.get("OLLAMA_MODEL", "mistral:7b")
                            # F-03-FIX : vérifier que Ollama ET le modèle sont disponibles
                            import json as _json_tags
                            with urllib.request.urlopen(f"{ollama_url}/api/tags", timeout=5) as _tags_resp:
                                _tags_data = _json_tags.loads(_tags_resp.read())
                                _available_models = [m.get("name","") for m in _tags_data.get("models",[])]
                                if not any(ollama_model in m for m in _available_models):
                                    raise RuntimeError(
                                        f"Ollama: modèle {ollama_model!r} non disponible. "
                                        f"Modèles présents: {_available_models[:3]}. "
                                        f"Lancer: ollama pull {ollama_model}"
                                    )
                            import json as _json
                            sys_content = kw.get("system", "")
                            user_content = ""
                            for msg in kw.get("messages", []):
                                if msg.get("role") == "user":
                                    c = msg.get("content", "")
                                    user_content = c if isinstance(c, str) else str(c)
                            payload = _json.dumps({
                                "model": ollama_model,
                                "prompt": f"{sys_content}\n\n{user_content}",
                                "stream": False,
                                "options": {"num_predict": min(kw.get("max_tokens", 4096), 4096)}
                            }).encode()
                            req = urllib.request.Request(
                                f"{ollama_url}/api/generate",
                                data=payload,
                                headers={"Content-Type": "application/json"},
                                method="POST",
                            )
                            with urllib.request.urlopen(req, timeout=60) as resp:  # F-05-FIX: 60s max (was 300s)
                                ollama_data = _json.loads(resp.read())
                            txt = ollama_data.get("response", "")
                            class _OllamaBlock:
                                def __init__(self, t): self.type = "text"; self.text = t
                            class _OllamaResp:
                                stop_reason = "end_turn"
                                def __init__(self, t): self.content = [_OllamaBlock(t)]
                            log.warning(f"MODE DÉGRADÉ Ollama ({ollama_model}) utilisé — pipeline maintenu")
                            return _OllamaResp(txt)  # type: ignore
                        except Exception as e3:
                            raise RuntimeError(
                                f"Anthropic + OpenAI + Ollama indisponibles : {e} / {e2} / {e3}"
                            ) from e
            raise  # Autre erreur Anthropic → relever

    # ── Boucle agentique (max 10 tours, TAVILY_MAX appels web) ───────────────
    # CODE-R5 : le bloc DEDUP mort (défini dans la v3.37 mais jamais exécuté)
    #           a été supprimé — call_api() est défini UNE SEULE FOIS ci-dessus.
    tavily_calls = 0
    resp         = None

    for _turn in range(10):
        kw = dict(
            model      = HAIKU_MODEL if cb_active() else SENTINEL_MODEL,
            max_tokens = SENTINEL_MAX_TOKENS,
            system     = SYSTEM_PROMPT,
            tools      = tools,
            messages   = messages,
        )

        resp = call_with_fallback(**kw)

        # Fin naturelle
        if resp.stop_reason == "end_turn":
            break

        # Claude demande un appel d'outil
        if resp.stop_reason == "tool_use":
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use" and block.name == "web_search":
                    if tavily_calls >= TAVILY_MAX:
                        result = json.dumps(
                            {"error": f"quota {TAVILY_MAX} appels Tavily atteint"}
                        )
                        log.info(f"TAVILY quota {TAVILY_MAX} atteint — résultat vide injecté")
                    else:
                        tavily_calls += 1
                        query  = block.input.get("query", "")
                        result = do_web_search(query)
                        log.info(f"TAVILY [{tavily_calls}/{TAVILY_MAX}] query={query!r}")

                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })

            # Ajouter la réponse assistant + les résultats outils
            messages.append({"role": "assistant", "content": resp.content})
            if tool_results:
                messages.append({"role": "user", "content": tool_results})
            else:
                break  # Aucun tool_use exploitable
        else:
            break  # stop_reason inattendu

    # ── Extraire le texte du rapport ──────────────────────────────────────────
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
        f"| {tavily_calls} appel(s) Tavily"
    )

    # C3-FIX : valider la structure du rapport (9 modules attendus)
    is_valid, missing = _validate_report_structure(report_text)
    if not is_valid:
        log.warning(
            f"C3 STRUCTURE RAPPORT INCOMPLÈTE : {len(missing)} modules manquants : {missing}. "
            f"Le rapport sera utilisé tel quel (dégradation gracieuse)."
        )
    else:
        log.info(f"C3 Structure rapport valide ({len(_REQUIRED_MODULES) - len(missing)}/10 modules présents)")

    # ── Extraire les deltas mémoire (section 9) ───────────────────────────────
    memory_deltas = extract_memory_delta(report_text)

    # ── Enregistrer les métriques dans SentinelDB (VISUAL-R1) ────────────────
    extract_metrics_from_report(report_text, str(date_obj))

    return report_text, memory_deltas