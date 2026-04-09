#!/usr/bin/env python3
# memory_manager.py --- SENTINEL v3.41 --- Gestion mémoire sans saturation contexte
# =============================================================================
# CHANGELOG v3.40 :
# [MM-FIX-1] HAIKU_MODEL déclaré via .env — était commenté en v3.37 → NameError fatal
# [MM-FIX-2] compress_report() retourne str JSON — était dict → incompatible SQLite
# [MM-FIX-3] report_text[-20000:] — garde la fin du rapport (M6-M9)
#            (était report_text[:4000]+[...]+report_text[-2000:] → coupait modules 6-9)
# [MM-FIX-4] FIX-MM2 alertes_closes matching flou décommenté (était `pass`)
# [MM-FIX-5] Caps tendances[-50:] / alertes_actives[-30:] décommentés (étaient commentés)
# [MM-NEW-1] get_compressed_memory() : SentinelDB primaire, JSON legacy fallback
# [MM-NEW-2] update_memory() : dual-write SentinelDB + JSON (rollback garanti)
# [MM-NEW-3] next() safe guard sur resp.content (plus de IndexError resp.content[0])
#
# CHANGELOG v3.41 — compatibilité sentinel_main.py v3.44 :
# [MM-41-FIX1] update_memory() : delta_updates et report_date rendus optionnels.
#              sentinel_main.py v3.44 appelle update_memory(report_text) avec un
#              seul argument → TypeError en v3.40. Les deltas sont désormais extraits
#              automatiquement via extract_memory_delta() si non fournis.
# [MM-41-NEW1] compress_text_haiku() : nouvelle fonction de résumé court (~300 chars)
#              en texte libre, utilisée par la phase MAP de run_monthly_report().
#              Distincte de compress_report() qui produit un JSON structuré complet
#              (~1500 tokens). Même fallback et guard next() que compress_report().
# =============================================================================

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

_VERSION = "3.41"
log = logging.getLogger("sentinel.memory")

# ---------------------------------------------------------------------------
# CONSTANTES
# ---------------------------------------------------------------------------
MEMORY_FILE = Path("data/sentinel_memory.json")

# [MM-FIX-1] HAIKU_MODEL était commenté en v3.37 → NameError à l'appel de compress_report()
HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5")

# ---------------------------------------------------------------------------
# UTILITAIRE — écriture atomique (inchangé v3.37)
# ---------------------------------------------------------------------------
@contextmanager
def atomic_write(target: Path, encoding: str = "utf-8"):
    """Context manager d'écriture atomique tmp→rename résistant aux crashs."""
    tmp = target.with_suffix(".tmp")
    try:
        yield tmp
        tmp.rename(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise

# ---------------------------------------------------------------------------
# PROMPTS DE COMPRESSION
# ---------------------------------------------------------------------------
COMPRESS_PROMPT = """\
Tu reçois un rapport SENTINEL complet. Compresse-le en JSON strict (600 mots max).
Retourne UNIQUEMENT un objet JSON avec ces champs exactement :

"date": "YYYY-MM-DD",
"indice": 7.5,
"delta_j1": "+0.5",
"alerte": "VERT",
"faits_critiques": ["Fait 1 (source A)", "Fait 2 (source B)", "Fait 3 (source C)"],
"nouvelles_tendances": ["Tendance émergente 1"],
"tendances_confirmees": ["Tendance confirmée 1"],
"alertes_ouvertes": ["Alerte 1"],
"alertes_closes": [],
"acteurs_notables": ["Acteur 1 (pays, activité)"],
"contrats_montants": ["Acteur X : 120M USD (source)"]

PAS de texte avant ou après le JSON. PAS de balises markdown.
"""

# [MM-41-NEW1] Prompt dédié à la compression texte libre (phase MAP mensuelle)
COMPRESS_HAIKU_PROMPT = """\
Résume le texte suivant en 2-3 phrases synthétiques (300 caractères maximum).
Conserve les faits clés, acteurs principaux et niveaux d'alerte.
Réponds en texte brut uniquement, sans JSON ni balises markdown.
"""

# ---------------------------------------------------------------------------
# FONCTIONS JSON LEGACY (fallback si SentinelDB indisponible)
# ---------------------------------------------------------------------------
def load_memory() -> dict:
    """
    Charge la mémoire JSON.
    DEPRECATED v3.37 → utiliser SentinelDB en priorité.
    Conservé comme fallback de dernier recours et pour dual-write.
    """
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log.warning(f"[MEMORY] sentinel_memory.json illisible ({exc}) — mémoire vide")
    return {
        "compressed_reports": [],
        "tendances": [],
        "alertes_actives": [],
        "acteurs": {},
    }

def save_memory(memory: dict) -> None:
    """
    Sauvegarde atomique JSON.
    DEPRECATED v3.37 → SentinelDB est la source de vérité.
    Conservé pour dual-write (rollback si DB corrompue).
    """
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with atomic_write(MEMORY_FILE) as tmp:
        tmp.write_text(
            json.dumps(memory, ensure_ascii=False, indent=2), encoding="utf-8"
        )

# ---------------------------------------------------------------------------
# COMPRESSION JSON STRUCTURÉE — stockage SentinelDB (inchangé v3.40)
# ---------------------------------------------------------------------------
def compress_report(report_text: str) -> str:
    """
    Compresse un rapport SENTINEL via Claude Haiku (coût ~0.01 EUR).
    Retourne un JSON STRING compatible SQLite.

    v3.40 :
    [MM-FIX-2] Retourne un JSON STRING (str) — double compatibilité SQLite + update_memory()
    [MM-FIX-3] report_text[-20000:] — garde M6-M9 (fin du rapport)
    [MM-FIX-1] HAIKU_MODEL depuis .env (était NameError en v3.37)
    [MM-NEW-3] next() safe guard sur resp.content (plus de IndexError)
    """
    if not report_text:
        return json.dumps(
            {"date": str(datetime.today().date()), "alerte": "VERT", "resume_brut": ""},
            ensure_ascii=False,
        )

    raw = ""
    try:
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=1500,
            system=COMPRESS_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": report_text[-20000:],  # [MM-FIX-3] garde M6-M9
                }
            ],
        )
        # [MM-NEW-3] next() évite IndexError si resp.content est vide
        raw = next(
            (b.text for b in resp.content if hasattr(b, "text") and b.text), ""
        ).strip()

        json.loads(raw)  # validation : lève JSONDecodeError si Haiku n'a pas suivi
        return raw       # JSON string valide ✓

    except json.JSONDecodeError:
        log.warning("[MEMORY] compress_report : JSON Haiku invalide — fallback brut")
        return json.dumps(
            {
                "date": str(datetime.today().date()),
                "alerte": "VERT",
                "resume_brut": (raw[:800] if raw else report_text[-800:]),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        log.exception("[MEMORY] compress_report Haiku échec — fallback rawtail")
        return json.dumps(
            {
                "date": str(datetime.today().date()),
                "alerte": "VERT",
                "resume_brut": report_text[-2000:],
            },
            ensure_ascii=False,
        )

# ---------------------------------------------------------------------------
# COMPRESSION TEXTE LIBRE — phase MAP rapport mensuel — [MM-41-NEW1]
# ---------------------------------------------------------------------------
def compress_text_haiku(text: str) -> str:
    """
    Résumé court en texte libre via Haiku (~300 chars max).

    Utilisée par run_monthly_report() dans sentinel_main.py v3.44 :
    · Phase MAP    : compresse chaque rapport journalier (30×) en parallèle
    · Phase REDUCE : compresse chaque bloc hebdomadaire (4×) en séquentiel

    Pourquoi une fonction distincte de compress_report() ?
    · compress_report() → JSON structuré ~1500 tokens (stockage SentinelDB)
    · compress_text_haiku() → texte libre ~300 chars (injection MapReduce)
    Les deux usages sont incompatibles : mélanger les deux casserait soit le
    stockage DB (si on injecte du texte libre) soit le MapReduce (si on injecte
    du JSON brut de 1500 tokens).

    Fallback : si Haiku échoue, retourne les 300 premiers chars du texte brut.
    """
    if not text or not text.strip():
        return ""

    raw = ""
    try:
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=200,
            system=COMPRESS_HAIKU_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": text[-3000:],  # dernier 3000 chars = fin du rapport
                }
            ],
        )
        # Même guard next() que compress_report() [MM-NEW-3]
        raw = next(
            (b.text for b in resp.content if hasattr(b, "text") and b.text), ""
        ).strip()
        return raw[:300]  # cap strict à 300 chars

    except Exception as exc:
        log.warning(f"[MEMORY] compress_text_haiku échec ({exc}) — fallback tronqué")
        return text[:300]

# ---------------------------------------------------------------------------
# LECTURE MÉMOIRE — injection dans Claude (inchangé v3.40)
# ---------------------------------------------------------------------------
def get_compressed_memory(n_days: int = 7) -> str:
    """
    Retourne la mémoire compressée prête à injecter dans Claude (~5000 tokens max).

    [MM-NEW-1] v3.40 : SentinelDB en source primaire, JSON legacy en fallback.

    Structure retournée (JSON string) :
    - derniers_rapports : list[dict] — n_days rapports compressés
    - tendances_actives : list[str] — 12 dernières tendances
    - alertes_actives   : list[str] — alertes ouvertes
    - acteurs_surveillance : list[str] — top 20 acteurs par score
    """
    # Source primaire : SentinelDB
    try:
        from db_manager import SentinelDB

        raw_reports = SentinelDB.getrecentreports(ndays=n_days)
        derniers_rapports = []
        for r in raw_reports:
            if r.get("compressed"):
                try:
                    derniers_rapports.append(json.loads(r["compressed"]))
                except json.JSONDecodeError:
                    # compressed corrompu → reconstitue depuis colonnes atomiques
                    derniers_rapports.append(
                        {
                            "date":   r["date"],
                            "indice": r["indice"],
                            "alerte": r["alerte"],
                        }
                    )
            else:
                derniers_rapports.append(
                    {"date": r["date"], "indice": r["indice"], "alerte": r["alerte"]}
                )

        tendances = [t["texte"] for t in SentinelDB.gettendancesactives()]
        alertes   = [a["texte"] for a in SentinelDB.getalertesactives()]
        acteurs   = [a["nom"]   for a in SentinelDB.getacteurs(limit=20)]

        return json.dumps(
            {
                "derniers_rapports":    derniers_rapports,
                "tendances_actives":    tendances[-12:],
                "alertes_actives":      alertes,
                "acteurs_surveillance": acteurs,
            },
            ensure_ascii=False,
            indent=2,
        )

    except Exception as db_err:
        log.warning(
            f"[MEMORY] SentinelDB indisponible ({db_err}) — fallback JSON legacy"
        )

    # Fallback : JSON legacy
    try:
        memory = load_memory()
        recent = memory.get("compressed_reports", [])[-n_days:]
        return json.dumps(
            {
                "derniers_rapports":    recent,
                "tendances_actives":    memory.get("tendances", [])[-12:],
                "alertes_actives":      memory.get("alertes_actives", []),
                "acteurs_surveillance": list(memory.get("acteurs", {}).keys())[:20],
            },
            ensure_ascii=False,
            indent=2,
        )
    except Exception as fallback_err:
        log.error(
            f"[MEMORY] Fallback JSON également échoué ({fallback_err}) — mémoire vide"
        )
        return json.dumps(
            {
                "derniers_rapports":    [],
                "tendances_actives":    [],
                "alertes_actives":      [],
                "acteurs_surveillance": [],
            },
            ensure_ascii=False,
        )

# ---------------------------------------------------------------------------
# MISE À JOUR MÉMOIRE — après chaque rapport
# ---------------------------------------------------------------------------
def update_memory(
    report_text: str,
    delta_updates: dict | None = None,  # [MM-41-FIX1] optionnel — était obligatoire
    report_date=None,                   # [MM-41-FIX1] optionnel — était obligatoire
) -> None:
    """
    Met à jour la mémoire après chaque rapport (journalier ou mensuel).

    v3.41 [MM-41-FIX1] :
        delta_updates et report_date sont désormais optionnels.
        sentinel_main.py v3.44 appelle update_memory(report_text) avec un seul
        argument — l'ancienne signature obligatoire causait un TypeError.
        Si delta_updates n'est pas fourni, les deltas sont extraits automatiquement
        depuis le rapport via extract_memory_delta() (sentinel_api.py).
        Si report_date n'est pas fourni, datetime.today().date() est utilisé.

    v3.40 :
    [MM-NEW-2] dual-write SentinelDB (source de vérité) + JSON (fallback).
    [MM-FIX-4] FIX-MM2 : alertes_closes avec matching flou réactivé (était `pass`).
    [MM-FIX-5] Caps tendances[-50:] / alertes_actives[-30:] réactivés.
    """
    # [MM-41-FIX1] Valeurs par défaut si arguments non fournis
    if report_date is None:
        report_date = datetime.today().date()
    today_str = str(report_date)

    if delta_updates is None:
        # Extraction automatique des deltas depuis le rapport
        try:
            from sentinel_api import extract_memory_delta
            delta_updates = extract_memory_delta(report_text)
            log.info("[MEMORY] delta_updates extrait automatiquement depuis le rapport")
        except Exception as exc:
            log.warning(f"[MEMORY] extract_memory_delta impossible ({exc}) — deltas vides")
            delta_updates = {}

    # ── 1. Compression du rapport ─────────────────────────────────────────
    compressed_str = compress_report(report_text)
    try:
        compressed_dict = json.loads(compressed_str)
    except json.JSONDecodeError:
        compressed_dict = {
            "date":       today_str,
            "alerte":     "VERT",
            "resume_brut": compressed_str[:500],
        }

    compressed_dict["date"] = today_str

    indice  = float(compressed_dict.get("indice", 5.0))
    alerte  = str(compressed_dict.get("alerte", "VERT"))
    rawtail = report_text[-2000:]  # FIX-DB8 : rawtail exploitable dashboard

    # ── 2. SentinelDB — source de vérité v3.40 ───────────────────────────
    try:
        from db_manager import SentinelDB

        SentinelDB.savereport(
            date=today_str,
            indice=indice,
            alerte=alerte,
            compressed=compressed_str,
            rawtail=rawtail,
        )

        for t in delta_updates.get("nouvelles_tendances", []):
            if isinstance(t, str) and t.strip():
                SentinelDB.savetendance(t.strip(), today_str)

        for a in delta_updates.get("alertes_ouvertes", []):
            if isinstance(a, str) and a.strip():
                SentinelDB.ouvrirealerte(a.strip(), today_str)

        # [MM-FIX-4] closealerte() décommenté (était `pass` en v3.37)
        for a in delta_updates.get("alertes_closes", []):
            if isinstance(a, str) and a.strip():
                SentinelDB.closealerte(a.strip(), today_str)

        log.info("[MEMORY] SentinelDB mise à jour avec succès")

    except Exception as db_err:
        log.warning(
            f"[MEMORY] SentinelDB update impossible ({db_err}) — écriture JSON seule"
        )

    # ── 3. Dual-write JSON legacy (rollback / compatibilité dashboard) ────
    try:
        memory = load_memory()

        # Ajout du rapport compressé
        memory.setdefault("compressed_reports", []).append(compressed_dict)
        memory["compressed_reports"] = memory["compressed_reports"][-50:]  # [MM-FIX-5] cap 50

        # Tendances
        for t in delta_updates.get("nouvelles_tendances", []):
            if isinstance(t, str) and t.strip():
                memory.setdefault("tendances", []).append(t.strip())
        memory["tendances"] = memory.get("tendances", [])[-50:]  # [MM-FIX-5] cap 50

        # Alertes ouvertes
        for a in delta_updates.get("alertes_ouvertes", []):
            if isinstance(a, str) and a.strip():
                if a.strip() not in memory.get("alertes_actives", []):
                    memory.setdefault("alertes_actives", []).append(a.strip())
        memory["alertes_actives"] = memory.get("alertes_actives", [])[-30:]  # [MM-FIX-5]

        # [MM-FIX-4] Alertes closes — matching flou réactivé
        for a_close in delta_updates.get("alertes_closes", []):
            if not isinstance(a_close, str):
                continue
            a_close_lower = a_close.lower().strip()
            memory["alertes_actives"] = [
                a for a in memory.get("alertes_actives", [])
                if a_close_lower not in a.lower()
            ]

        save_memory(memory)
        log.info("[MEMORY] JSON legacy mis à jour (dual-write)")

    except Exception as json_err:
        log.warning(f"[MEMORY] Dual-write JSON échoué ({json_err}) — SentinelDB seule")
