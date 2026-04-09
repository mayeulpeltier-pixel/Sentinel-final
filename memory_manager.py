#!/usr/bin/env python3
# memory_manager.py --- SENTINEL v3.41 --- Gestion mémoire sans saturation contexte
# =============================================================================
# CHANGELOG v3.40 :
# [MM-FIX-1] HAIKU_MODEL déclaré via .env — était commenté en v3.37 → NameError fatal
# [MM-FIX-2] compress_report() retourne str JSON — était dict → incompatible SQLite
# [MM-FIX-3] report_text[-20000:] — garde la fin du rapport (M6-M9)
# (était report_text[:4000]+[...]+report_text[-2000:] → coupait modules 6-9)
# [MM-FIX-4] FIX-MM2 alertes_closes matching flou décommenté (était `pass`)
# [MM-FIX-5] Caps tendances[-50:] / alertes_actives[-30:] décommentés (étaient commentés)
# [MM-NEW-1] get_compressed_memory() : SentinelDB primaire, JSON legacy fallback
# [MM-NEW-2] update_memory() : dual-write SentinelDB + JSON (rollback garanti)
# [MM-NEW-3] next() safe guard sur resp.content (plus de IndexError resp.content[0])
#
# CHANGELOG v3.41 :
# [MM-FIX-6] atomic_write() : tmp.rename(target) → tmp.replace(target)
#             rename() lève FileExistsError sur Windows si target existe déjà.
#             replace() est atomique et écrase la cible sur Linux ET Windows.
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
# UTILITAIRE — écriture atomique
# ---------------------------------------------------------------------------
@contextmanager
def atomic_write(target: Path, encoding: str = "utf-8"):
    """
    Context manager d'écriture atomique tmp→replace résistant aux crashs.

    [MM-FIX-6] Utilise tmp.replace(target) au lieu de tmp.rename(target) :
    - Path.rename() lève FileExistsError sur Windows si target existe déjà.
    - Path.replace() est atomique et écrase silencieusement la cible
      sur Linux (rename(2)) ET Windows (MoveFileExW), garantissant
      un comportement identique sur toutes les plateformes.
    """
    tmp = target.with_suffix(".tmp")
    try:
        yield tmp
        tmp.replace(target)  # [MM-FIX-6] était tmp.rename(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise

# ---------------------------------------------------------------------------
# PROMPT DE COMPRESSION (inchangé v3.37)
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
# COMPRESSION — cœur du module
# ---------------------------------------------------------------------------
def compress_report(report_text: str) -> str:
    """
    Compresse un rapport SENTINEL via Claude Haiku (coût ~0.01 EUR).

    v3.40 :
    [MM-FIX-2] Retourne un JSON STRING (str) — double compatibilité :
        · Stockage SQLite : SentinelDB.savereport(..., compressed=str, ...)
        · update_memory() interne : json.loads(compressed_str) pour le dict
    [MM-FIX-3] report_text[-20000:] — garde M6-M9 (fin du rapport)
        (était report_text[:4000]+[...]+report_text[-2000:] → perdait M6-M9)
    [MM-FIX-1] HAIKU_MODEL depuis .env (était NameError en v3.37)
    [MM-NEW-3] next() safe guard sur resp.content (plus de IndexError)

    Fallback rawtail : si Haiku échoue, retourne les 2000 derniers chars
    en JSON brut — exploitable par dashboard.py (FIX-DB8).
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
# LECTURE MÉMOIRE — injection dans Claude
# ---------------------------------------------------------------------------
def get_compressed_memory(n_days: int = 7) -> str:
    """
    Retourne la mémoire compressée prête à injecter dans Claude (~5000 tokens max).

    [MM-NEW-1] v3.40 : SentinelDB en source primaire, JSON legacy en fallback.

    Structure retournée (JSON string) :
    - derniers_rapports      : list[dict] — n_days rapports compressés
    - tendances_actives      : list[str]  — 12 dernières tendances
    - alertes_actives        : list[str]  — alertes ouvertes
    - acteurs_surveillance   : list[str]  — top 20 acteurs par score
    """
    # ── Source primaire : SentinelDB ──────────────────────────────────────
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

    # ── Fallback : JSON legacy ─────────────────────────────────────────────
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
    delta_updates: dict,
    report_date,
) -> None:
    """
    Met à jour la mémoire après chaque rapport (journalier ou mensuel).

    [MM-NEW-2] v3.40 : dual-write SentinelDB (source de vérité) + JSON (fallback).
    [MM-FIX-4] FIX-MM2 : alertes_closes avec matching flou réactivé (était `pass`).
    [MM-FIX-5] Caps tendances[-50:] / alertes_actives[-30:] réactivés.

    Appels SentinelDB v3.40 (signatures exactes db_manager.py) :
        savereport(date, indice, alerte, compressed, rawtail)
        savetendance(texte, date)
        ouvrirealerte(texte, date)
        closealerte(texte, date)
    """
    today_str = str(report_date)

    # ── 1. Compression du rapport ──────────────────────────────────────────
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

    # ── 2. SentinelDB — source de vérité v3.40 ────────────────────────────
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

        # [MM-FIX-4] FIX-MM2 décommenté — closealerte(texte, date) signature v3.40
        for a in delta_updates.get("alertes_closes", []):
            if isinstance(a, str) and a.strip():
                SentinelDB.closealerte(a.strip(), today_str)

        log.info("[MEMORY] SentinelDB mise à jour avec succès")

    except Exception as db_err:
        log.warning(
            f"[MEMORY] SentinelDB update impossible ({db_err}) — écriture JSON seule"
        )

    # ── 3. Dual-write JSON legacy (rollback / compatibilité dashboard) ─────
    try:
        memory = load_memory()

        memory.setdefault("compressed_reports", []).append(compressed_dict)
        memory["compressed_reports"] = memory["compressed_reports"][-90:]

        for t in delta_updates.get("nouvelles_tendances", []):
            if isinstance(t, str) and t.strip() and t not in memory.get("tendances", []):
                memory.setdefault("tendances", []).append(t.strip())
        memory["tendances"] = memory.get("tendances", [])[-50:]  # [MM-FIX-5]

        for a in delta_updates.get("alertes_ouvertes", []):
            if isinstance(a, str) and a.strip() and a not in memory.get("alertes_actives", []):
                memory.setdefault("alertes_actives", []).append(a.strip())
        memory["alertes_actives"] = memory.get("alertes_actives", [])[-30:]  # [MM-FIX-5]

        # [MM-FIX-4] Matching flou pour clôture des alertes
        for a_close in delta_updates.get("alertes_closes", []):
            if not isinstance(a_close, str) or not a_close.strip():
                continue
            key = a_close.strip()[:40].lower()
            memory["alertes_actives"] = [
                a for a in memory.get("alertes_actives", [])
                if key not in a.lower()
            ]

        save_memory(memory)
        log.info("[MEMORY] JSON legacy mis à jour (dual-write)")

    except Exception as json_err:
        log.warning(f"[MEMORY] Dual-write JSON échoué ({json_err}) — DB seule active")
