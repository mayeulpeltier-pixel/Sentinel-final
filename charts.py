# charts.py --- SENTINEL v3.40 --- Graphiques PNG
# =============================================================================
# CHANGELOG v3.40 vs v3.37 :
#   [CH-FIX-1]  Source primaire → SentinelDB.getmetrics() / getacteurs()
#               Plus de lecture JSON pour activite30j / geo / acteurs
#   [CH-FIX-2]  chart_activite_30j : axe X = dates réelles depuis metrics
#               (plus range(len(history)) — BUG v2.0 corrigé)
#   [CH-FIX-3]  chart_repartition_geo : moyennes metrics.geousa/geoeurope…
#   [CH-FIX-4]  chart_radar_techno : colonnes metrics.terrestre/maritime/
#               transverse/contractuel (moyennes 30j)
#   [CH-FIX-5]  chart_top_acteurs : SentinelDB.getacteurs() (scoreactivite)
#               + fallback comptage texte
#   [CH-FIX-6]  chart_evolution_alertes : table alertes (dateouverture/
#               datecloture) via getdb() — A11-FIX guard non-liste préservé
#   [CH-FIX-7]  chart_acteurs_geo : scores dynamiques depuis getacteurs()
#               + mapping ISO-3 (R5A3-NEW-4)
#   [CH-FIX-8]  Plotly+kaleido → PNG avec fallback matplotlib par chart
#   [CH-FIX-9]  Shewhart 2σ : variance population N (A06-FIX / MATHS-M5)
#               SENTINEL_ALERT_SIGMA configurable .env
#   [CH-FIX-10] Heatmap co-occurrence LOCALE fenêtre 250 chars (A12-FIX)
#   [CH-FIX-11] PPMI matrix helper intégré (MATH-5)
#   [CH-FIX-12] guard all-zeros radar → return None (R5A3-NEW-1)
#   [CH-FIX-13] safe_wrap individuel par chart dans generate_all_charts
#               (R5A3-NEW-6) — une exception n'arrête pas les autres
#   [CH-FIX-14] normalisation pie sum→100 (MATH-2)
#   [CH-FIX-15] Colormap YlOrRd heatmap (A22-FIX)
#   [CH-FIX-16] extract_chart_data re.DOTALL (BUG-04) conservé comme
#               fallback si metrics DB vides
#   [CH-FIX-17] dpi=150 (était 300) — 2× plus rapide sur Raspberry Pi
#               qualité suffisante pour les rapports HTML
# =============================================================================

from __future__ import annotations

import logging
import math
import os
import re
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")          # Backend non-interactif — obligatoire en mode serveur
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

log = logging.getLogger("sentinel.charts")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
_VERSION = "3.40"

CHARTS_DIR = Path("output/charts")
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# Palette SENTINEL — cohérente avec reportbuilder.py
COULEURS: list[str] = [
    "#1F3864",  # [0] Bleu marine  — activité principale / radar fill
    "#C00000",  # [1] Rouge        — alertes ouvertes / seuil Shewhart
    "#2E75B6",  # [2] Bleu clair   — moy.7j / alertes closes
    "#70AD47",  # [3] Vert         — succès
    "#FF8C00",  # [4] Orange       — contractuel
    "#7030A0",  # [5] Violet       — signaux faibles
    "#404040",  # [6] Gris sombre  — texte secondaire
    "#00B0F0",  # [7] Cyan         — maritime
]

# ─────────────────────────────────────────────────────────────────────────────
# IMPORT DB — fallback gracieux (CH-FIX-1)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from db_manager import SentinelDB, getdb  # type: ignore
    _DB_AVAILABLE = True
    log.info("CHARTS SentinelDB connecté — source primaire SQLite active")
except ImportError:
    _DB_AVAILABLE = False
    log.warning("CHARTS db_manager absent — fallback memory JSON uniquement")

# ─────────────────────────────────────────────────────────────────────────────
# IMPORT PLOTLY + KALEIDO — fallback gracieux (CH-FIX-8 / A25-FIX)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import plotly.graph_objects as _go
    import plotly.express as _px
    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    log.info("CHARTS plotly absent — matplotlib uniquement (pip install plotly)")

try:
    import kaleido as _kaleido_test  # noqa: F401
    _KALEIDO_AVAILABLE = True
except ImportError:
    _KALEIDO_AVAILABLE = False

_USE_PLOTLY = _PLOTLY_AVAILABLE and _KALEIDO_AVAILABLE

# ─────────────────────────────────────────────────────────────────────────────
# ACTEURS SURVEILLÉS (CH-FIX-5 / B9-FIX — constante globale)
# ─────────────────────────────────────────────────────────────────────────────
ACTEURS_SURVEILLES: list[str] = [
    "Milrem", "Ghost Robotics", "Textron", "Elbit", "Rafael", "IAI", "DARPA",
    "Kongsberg", "Exail", "ECA Group", "Baykar", "STM", "Saab", "Rheinmetall",
    "Leonardo", "Thales", "Dassault", "MBDA", "Arquus", "KNDS", "Oshkosh",
    "QinetiQ", "Shield AI", "Anduril", "Boston Dynamics", "Palantir",
    "L3Harris", "FLIR", "DroneShield", "Northrop Grumman", "Naval Group",
    "BAE Systems", "Hanwha", "Hyundai Rotem", "KAIST",
]

# Mapping acteur → pays ISO-3 (CH-FIX-7 / R5A3-NEW-4)
_ACTEUR_ISO: dict[str, str] = {
    "Milrem": "EST",         "Ghost Robotics": "USA",  "Textron": "USA",
    "Elbit": "ISR",          "Rafael": "ISR",           "IAI": "ISR",
    "DARPA": "USA",          "Kongsberg": "NOR",        "Exail": "FRA",
    "ECA Group": "FRA",      "Baykar": "TUR",           "STM": "TUR",
    "Saab": "SWE",           "Rheinmetall": "DEU",      "Leonardo": "ITA",
    "Thales": "FRA",         "Dassault": "FRA",         "MBDA": "FRA",
    "Arquus": "FRA",         "KNDS": "FRA",             "Oshkosh": "USA",
    "QinetiQ": "GBR",        "Shield AI": "USA",        "Anduril": "USA",
    "Boston Dynamics": "USA","Palantir": "USA",         "L3Harris": "USA",
    "FLIR": "USA",           "DroneShield": "AUS",      "Northrop Grumman": "USA",
    "Naval Group": "FRA",    "BAE Systems": "GBR",      "Hanwha": "KOR",
    "Hyundai Rotem": "KOR",  "KAIST": "KOR",
}


# ─────────────────────────────────────────────────────────────────────────────
# V1-FIX — EXPORT SVG INLINE (réduit rapport HTML de 5-8 Mo à ~400 Ko)
# Chaque chart matplotlib peut être exporté en SVG string au lieu de PNG base64.
# Compatible tous clients email modernes. Qualité vectorielle parfaite.
# ─────────────────────────────────────────────────────────────────────────────

def _fig_to_svg(fig) -> Optional[str]:
    """
    V1-FIX : convertit une figure matplotlib en string SVG inline.
    Retourne None si la conversion échoue (fallback PNG géré par report_builder).
    """
    try:
        import io
        buf = io.BytesIO()
        fig.savefig(buf, format="svg", bbox_inches="tight")
        buf.seek(0)
        svg_str = buf.read().decode("utf-8")
        # Supprimer le header XML pour injection inline
        if "<?xml" in svg_str:
            svg_str = svg_str[svg_str.index("<svg"):]
        return svg_str
    except Exception as _e:
        log.debug(f"CHARTS _fig_to_svg échoué : {_e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS MATHÉMATIQUES
# ─────────────────────────────────────────────────────────────────────────────

def _seuil_shewhart(values: list[float]) -> float:
    """
    Seuil alerte Shewhart — CH-FIX-9 / A06-FIX / MATHS-M5.
    Formule : µ + k·σ  (variance POPULATION N, non Bessel N-1 pour N ≤ 20).
    k configurable via SENTINEL_ALERT_SIGMA (défaut 2.0 = quantile 97.7%).
    """
    n  = max(len(values), 1)
    mu = sum(values) / n
    sigma = math.sqrt(sum((v - mu) ** 2 for v in values) / n)
    k = float(os.environ.get("SENTINEL_ALERT_SIGMA", "2.0"))
    return mu + k * sigma


def _ppmi_matrix(cooc: dict, actors: list) -> dict:
    """
    PPMI (Positive Pointwise Mutual Information) — MATH-5.
    Évite le biais des acteurs ultra-fréquents (DARPA, USA…).
    PPMIi,j = max(0, log2( P(i,j) / P(i)·P(j) ))
    """
    total = sum(cooc.get(a, {}).get(b, 0) for a in actors for b in actors) or 1
    pi = {a: sum(cooc.get(a, {}).values()) / total for a in actors}
    pj = {b: sum(cooc.get(a, {}).get(b, 0) for a in actors) / total for b in actors}
    out: dict = {}
    for a in actors:
        out[a] = {}
        for b in actors:
            pij   = cooc.get(a, {}).get(b, 0) / total
            denom = pi.get(a, 1e-9) * pj.get(b, 1e-9)
            out[a][b] = max(0.0, math.log2(pij / denom + 1e-9)) if pij > 0 else 0.0
    return out

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DONNÉES
# ─────────────────────────────────────────────────────────────────────────────

def _load_memory_json(memory_file: str = "data/sentinel_memory.json") -> list[dict]:
    """Fallback legacy — charge compressed_reports depuis sentinel_memory.json."""
    import json
    try:
        data = json.loads(Path(memory_file).read_text(encoding="utf-8"))
        history = data.get("compressedreports", data.get("compressed_reports", []))
        return list(history)[-30:]
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return []


def _get_metrics_30j() -> list[dict]:
    """
    Source primaire : table metrics (30j) — triée ASC par date.
    Retourne [] si DB indisponible ou table vide.
    """
    if not _DB_AVAILABLE:
        return []
    try:
        rows = SentinelDB.getmetrics(ndays=30)
        return list(reversed(rows))           # getmetrics() retourne DESC → on inverse en ASC
    except Exception as e:
        log.warning(f"CHARTS _get_metrics_30j : {e}")
        return []


def _get_alertes_timeline(ndays: int = 30) -> tuple[list[str], list[int], list[int]]:
    """
    Construit (dates, ouvertes_par_jour, closes_par_jour) depuis la table alertes.
    Utilise getdb() directement — SentinelDB n'expose pas de méthode timeline.
    CH-FIX-6.
    """
    if not _DB_AVAILABLE:
        return [], [], []
    try:
        with getdb() as db:
            rows_ouv = db.execute(
                """
                SELECT DATE(dateouverture) AS d, COUNT(*) AS n
                FROM alertes
                WHERE dateouverture >= date('now', ?)
                GROUP BY DATE(dateouverture)
                ORDER BY d ASC
                """,
                (f"-{ndays} days",),
            ).fetchall()
            rows_cls = db.execute(
                """
                SELECT DATE(datecloture) AS d, COUNT(*) AS n
                FROM alertes
                WHERE datecloture >= date('now', ?)
                  AND datecloture IS NOT NULL
                GROUP BY DATE(datecloture)
                ORDER BY d ASC
                """,
                (f"-{ndays} days",),
            ).fetchall()

        ouv_map = {r["d"]: r["n"] for r in rows_ouv}
        cls_map = {r["d"]: r["n"] for r in rows_cls}
        all_dates = sorted(set(ouv_map) | set(cls_map))
        if not all_dates:
            return [], [], []
        return (
            all_dates,
            [ouv_map.get(d, 0) for d in all_dates],
            [cls_map.get(d, 0) for d in all_dates],
        )
    except Exception as e:
        log.warning(f"CHARTS _get_alertes_timeline : {e}")
        return [], [], []

# ─────────────────────────────────────────────────────────────────────────────
# CHART 1 — ACTIVITÉ SECTORIELLE 30J (CH-FIX-2)
# ─────────────────────────────────────────────────────────────────────────────

def chart_activite_30j(memory_file: str = "data/sentinel_memory.json") -> Optional[str]:
    """
    Courbe activité sectorielle 30 jours.
    Source primaire  : metrics.indice + metrics.date via SentinelDB.getmetrics().
    Fallback         : memory JSON (legacy v3.37).
    Plotly PNG si kaleido disponible, sinon matplotlib.

    CH-FIX-2 : axe X = dates réelles depuis DB (plus range(len()) — BUG v2.0).
    CH-FIX-9 : seuil Shewhart 2σ variance population.
    """
    metrics = _get_metrics_30j()
    if metrics:
        dates   = [r["date"][-5:] for r in metrics]           # MM-DD
        indices = [float(r.get("indice") or 5.0) for r in metrics]
    else:
        history = _load_memory_json(memory_file)
        if not history:
            log.warning("CHARTS activite30j : DB vide + JSON absent — chart non généré")
            return None
        dates   = [h.get("date", "")[-5:] for h in history]
        indices = [float(h.get("indice", 5.0)) for h in history]

    if not indices:
        return None

    x     = list(range(len(indices)))
    seuil = _seuil_shewhart(indices)
    step  = max(1, len(x) // 8)

    return (
        _chart_activite_30j_plotly(x, dates, indices, seuil, step)
        if _USE_PLOTLY
        else _chart_activite_30j_mpl(x, dates, indices, seuil, step)
    )


def _chart_activite_30j_plotly(
    x: list, dates: list, indices: list, seuil: float, step: int
) -> Optional[str]:
    try:
        ma7 = np.convolve(indices, np.ones(7) / 7.0, mode="valid").tolist() if len(indices) >= 7 else []
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=x, y=indices,
            mode="lines+markers",
            fill="tozeroy",
            name="Activité",
            line=dict(color=COULEURS[0], width=2.5),
            marker=dict(size=5),
        ))
        if ma7:
            fig.add_trace(_go.Scatter(
                x=x[6:], y=ma7,
                mode="lines",
                name="Moy.7j",
                line=dict(color=COULEURS[2], dash="dash", width=1.5),
            ))
        fig.add_hline(
            y=seuil,
            line_dash="dash",
            line_color=COULEURS[1],
            line_width=1,
            annotation_text=f"Seuil {os.environ.get('SENTINEL_ALERT_SIGMA','2')}σ = {seuil:.1f}",
            annotation_position="bottom right",
        )
        fig.update_layout(
            title="Activité sectorielle — Robotique Défense (30 derniers jours)",
            xaxis=dict(tickvals=x[::step], ticktext=dates[::step], tickangle=35),
            yaxis=dict(title="Indice activité /10", range=[0, 10]),
            template="plotly_white",
            legend=dict(x=0.01, y=0.99),
        )
        out = CHARTS_DIR / "activite30j.png"
        # V-01-FIX : Plotly exporte aussi en SVG (10x plus léger que PNG)
        svg_out = CHARTS_DIR / "activite30j.svg"
        try:
            import io as _io
            svg_bytes = fig.to_image(format="svg", width=1200, height=400)
            svg_out.write_bytes(svg_bytes)
            return str(svg_out)
        except Exception:
            fig.write_image(str(out), width=1200, height=400)
            return str(out)
    except Exception as e:
        log.warning(f"CHARTS activite30j Plotly : {e} — fallback matplotlib")
        return _chart_activite_30j_mpl(x, dates, indices, seuil, step)


def _chart_activite_30j_mpl(
    x: list, dates: list, indices: list, seuil: float, step: int
) -> Optional[str]:
    try:
        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(x, indices, color=COULEURS[0], lw=2.5, zorder=3)
        ax.fill_between(x, indices, alpha=0.25, color=COULEURS[0])
        ax.scatter(x, indices, color=COULEURS[0], s=30, zorder=4)
        if len(indices) >= 7:
            ma7 = np.convolve(indices, np.ones(7) / 7.0, mode="valid").tolist()
            ax.plot(x[6:], ma7, color=COULEURS[2], lw=1.5, ls="--", alpha=0.85, label="Moy.7j")
        ax.axhline(
            y=seuil, color=COULEURS[1], lw=1, linestyle="--", alpha=0.7,
            label=f"Seuil {os.environ.get('SENTINEL_ALERT_SIGMA','2')}σ = {seuil:.1f}",
        )
        ax.set_xticks(x[::step])
        ax.set_xticklabels(dates[::step], rotation=35, ha="right", fontsize=8)
        ax.set_ylabel("Indice activité /10", fontsize=10)
        ax.set_ylim(0, 10)
        ax.set_title(
            "Activité sectorielle — Robotique Défense (30 derniers jours)",
            fontsize=12, fontweight="bold",
        )
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=9)
        fig.tight_layout()
        # V1-FIX: SVG inline first (smaller file), fallback PNG
        svg = _fig_to_svg(fig)
        if svg:
            out = CHARTS_DIR / "activite30j.svg"
            out.write_text(svg, encoding="utf-8")
        else:
            out = CHARTS_DIR / "activite30j.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return str(out)
    except Exception as e:
        log.error(f"CHARTS activite30j matplotlib : {e}")
        plt.close("all")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# CHART 2 — RÉPARTITION GÉOGRAPHIQUE (CH-FIX-3 / MATH-2)
# ─────────────────────────────────────────────────────────────────────────────

def chart_repartition_geo(geodata: Optional[dict] = None) -> Optional[str]:
    """
    Camembert répartition géographique des événements.
    Source primaire  : moyenne metrics.geousa/geoeurope/geoasie/geomo/georussie (30j).
    Fallback         : geodata dict passé en argument, puis valeurs par défaut.
    MATH-2 : normalisation sum→100 active (BUG B13 corrigé).
    """
    if geodata is None:
        metrics = _get_metrics_30j()
        if metrics:
            col_map = {
                "USA":       "geousa",
                "Europe":    "geoeurope",
                "Asie-Pac.": "geoasie",
                "MO":        "geomo",
                "Russie":    "georussie",
            }
            avgs: dict[str, float] = {}
            for lbl, col in col_map.items():
                vals = [float(r.get(col) or 0.0) for r in metrics]
                avgs[lbl] = round(sum(vals) / len(vals), 1) if vals else 0.0
            geodata = {lbl: v for lbl, v in avgs.items() if v > 0}

    if not geodata:
        # Valeurs par défaut si aucune donnée DB
        geodata = {"USA": 40.0, "Europe": 30.0, "Asie-Pac.": 18.0, "Russie": 6.0, "Autres": 6.0}

    labels = list(geodata.keys())
    raw    = list(geodata.values())
    tot    = sum(raw) or 100
    values = [v / tot * 100 for v in raw]   # MATH-2 normalisation

    fig = None  # P2-FIX: initialiser pour try/finally
    try:
        colors = (COULEURS * math.ceil(len(labels) / len(COULEURS)))[:len(labels)]
        fig, ax = plt.subplots(figsize=(7, 5))
        if not any(v > 0 for v in values):  # guard camembert vide
            log.warning("CHARTS repartitiongeo : toutes valeurs à 0 — chart ignoré")
            return None
        _, _, autotexts = ax.pie(
            values,
            labels=labels,
            autopct="%1.1f%%",
            colors=colors,
            startangle=140,
            wedgeprops=dict(edgecolor="white", linewidth=2),
        )
        for at in autotexts:
            at.set_fontsize(9)
        ax.set_title("Répartition géographique des événements", fontsize=12, fontweight="bold")
        fig.tight_layout()
        svg = _fig_to_svg(fig)
        if svg:
            out = CHARTS_DIR / "repartitiongeo.svg"
            out.write_text(svg, encoding="utf-8")
        else:
            out = CHARTS_DIR / "repartitiongeo.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
        return str(out)
    except Exception as e:
        log.error(f"CHARTS repartitiongeo : {e}")
        return None
    finally:
        if fig is not None:
            plt.close(fig)  # P2-FIX

# ─────────────────────────────────────────────────────────────────────────────
# CHART 3 — RADAR TECHNOLOGIQUE (CH-FIX-4 / R5A3-NEW-1)
# ─────────────────────────────────────────────────────────────────────────────

def chart_radar_techno(scores: Optional[dict] = None, report_text: str = "") -> Optional[str]:
    """
    Radar technologique par domaine (8 axes).
    Source primaire  : moyenne metrics.terrestre/maritime/transverse/contractuel (30j).
    R5A3-NEW-1 : guard all-zeros → return None (pas de radar vide trompeur).
    """
    if scores is None:
        metrics = _get_metrics_30j()
        if metrics:
            dom_cols = {
                "UGV":       "terrestre",
                "USV":       "maritime",
                "Transverse": "transverse",
                "Contrats":  "contractuel",
            }
            avgs: dict[str, float] = {}
            for label, col in dom_cols.items():
                vals = [float(r.get(col) or 0.0) for r in metrics]
                avgs[label] = min(10.0, round(sum(vals) / len(vals), 1)) if vals else 0.0
            # M2-FIX: axes manquants calculés depuis keywords rapport
            def _kw_score(keywords, text, base=5.0):
                """
                M-04-FIX : Score 0-10 avec pondération par position.
                Keywords dans MODULE 3-5 (corps analytique) : poids ×2.
                Keywords dans MODULE 9 (sources) : poids ×0.5.
                """
                if not text:
                    return base
                text_low = text.lower()
                # Identifier les zones du rapport pour pondération
                core_start = text_low.find("module 3") if "module 3" in text_low else 0
                core_end   = text_low.find("module 6") if "module 6" in text_low else len(text_low)
                src_start  = text_low.find("module 9") if "module 9" in text_low else len(text_low)

                score_add = 0.0
                for kw in keywords:
                    kw_low = kw.lower()
                    idx_kw = text_low.find(kw_low)
                    if idx_kw == -1:
                        continue
                    # Pondération selon la zone
                    if core_start <= idx_kw <= core_end:
                        score_add += 0.5 * 2.0   # corps analytique : ×2
                    elif idx_kw >= src_start:
                        score_add += 0.5 * 0.5   # sources §9 : ×0.5
                    else:
                        score_add += 0.5          # reste : poids normal
                return min(10.0, base + score_add) if score_add > 0 else base

            txt = report_text
            scores = {
                "UGV":        avgs.get("UGV", 5.0),
                "USV":        avgs.get("USV", 5.0),
                "UUV":        _kw_score(["uuv","autonomous underwater","submarine drone","auv","orca","manta"], txt),
                "Essaims":    _kw_score(["swarm","essaim","drone swarm","collaborative drone","multi-uav"], txt),
                "IA Emb.":    avgs.get("Transverse", _kw_score(["ai","artificial intelligence","machine learning","neural","autonomy"], txt)),
                "Arm.Auto":   _kw_score(["loitering munition","kamikaze","autonomous weapon","laws","lethal autonomous"], txt),
                "C2-Cyber":   _kw_score(["c2","command and control","electronic warfare","cyber","ew","jamming","spoofing"], txt),
                "Logistique": avgs.get("Contrats", _kw_score(["logistics","supply chain","maintenance","sustainment"], txt)),
            }

    if scores is None:
        scores = {k: 0.0 for k in ["UGV","USV","UUV","Essaims","IA Emb.","Arm.Auto","C2-Cyber","Logistique"]}

    # R5A3-NEW-1 : guard all-zeros — pas de surface sans signal
    if not any(v > 0 for v in scores.values()):
        return None

    # M-05-FIX : guard all-fives — radar octogone trompeur si toutes les valeurs = base
    non_base = [v for v in scores.values() if abs(v - 5.0) > 0.1]
    if len(non_base) < 2:
        log.debug("CHARTS radar : moins de 2 axes avec données réelles — chart ignoré")
        return None

    cats   = list(scores.keys())
    N      = len(cats)
    vals   = list(scores.values()) + [list(scores.values())[0]]   # fermer le polygone
    angles = [n / N * 2 * math.pi for n in range(N)] + [0]

    fig = None  # P2-FIX\n    try:\n        fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(projection="polar"))\n        ax.plot(angles, vals, "o-", lw=2, color=COULEURS[0])\n        ax.fill(angles, vals, alpha=0.20, color=COULEURS[0])\n        ax.set_xticks(angles[:-1])\n        ax.set_xticklabels(cats, size=10)\n        ax.set_ylim(0, 10)\n        ax.set_title(\n            "Radar technologique — Intensité d'activité\n(0 = aucune → 10 = maximale)",\n            pad=20, fontsize=11, fontweight="bold",\n        )\n        ax.grid(True, alpha=0.4)\n        fig.tight_layout()\n        svg = _fig_to_svg(fig)\n        if svg:\n            out = CHARTS_DIR / "radartechno.svg"\n            out.write_text(svg, encoding="utf-8")\n        else:\n            out = CHARTS_DIR / "radartechno.png"\n            fig.savefig(out, dpi=150, bbox_inches="tight")\n        return str(out)\n    except Exception as e:\n        log.error(f"CHARTS radartechno : {e}")\n        return None\n    finally:\n        if fig is not None:\n            plt.close(fig)  # P2-FIX

# ─────────────────────────────────────────────────────────────────────────────
# CHART 4 — TOP ACTEURS (CH-FIX-5)
# ─────────────────────────────────────────────────────────────────────────────

def chart_top_acteurs(report_text: str = "") -> Optional[str]:
    """
    Top 10 acteurs par score d'activité.
    Source primaire  : SentinelDB.getacteurs(limit=15) → scoreactivite.
    Fallback         : comptage de mentions dans report_text.
    """
    top10: list[tuple[str, float]] = []

    if _DB_AVAILABLE:
        try:
            acteurs = SentinelDB.getacteurs(limit=15)
            top10 = [
                (a["nom"], float(a.get("scoreactivite") or 0.0))
                for a in acteurs
                if (a.get("scoreactivite") or 0.0) > 0
            ]
        except Exception as e:
            log.warning(f"CHARTS top_acteurs DB : {e}")

    # Fallback : comptage mentions dans texte rapport
    if not top10 and report_text:
        text = report_text.lower()
        counts = [(a, text.count(a.lower())) for a in ACTEURS_SURVEILLES]
        top10 = [
            (k, float(v))
            for k, v in sorted(counts, key=lambda kv: -kv[1])
            if v > 0
        ][:10]

    if not top10:
        return None

    top10 = top10[:10]
    labels   = [k for k, _ in reversed(top10)]
    vals     = [v for _, v in reversed(top10)]
    x_label  = "Score activité (DB)" if _DB_AVAILABLE and top10 else "Mentions (texte)"

    try:
        fig, ax = plt.subplots(figsize=(9, max(3, len(top10) * 0.5)))
        bars = ax.barh(labels, vals, color=COULEURS[0], edgecolor="white")
        ax.bar_label(bars, padding=3, fontsize=9)
        ax.set_xlabel(x_label, fontsize=10)
        lgd = mpatches.Patch(
            color=COULEURS[0],
            label=f"Top acteurs ({len(ACTEURS_SURVEILLES)} surveillés)",
        )
        ax.legend(handles=[lgd], fontsize=8, loc="lower right")
        ax.set_title(
            "Top acteurs — Fréquence de mention / score activité",
            fontsize=11, fontweight="bold",
        )
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        # V1-FIX: SVG inline first (smaller file), fallback PNG
        svg = _fig_to_svg(fig)
        if svg:
            out = CHARTS_DIR / "topacteurs.svg"
            out.write_text(svg, encoding="utf-8")
        else:
            out = CHARTS_DIR / "topacteurs.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return str(out)
    except Exception as e:
        log.error(f"CHARTS topacteurs : {e}")
        plt.close("all")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# CHART 5 — ÉVOLUTION ALERTES 30J (CH-FIX-6 / A11-FIX)
# ─────────────────────────────────────────────────────────────────────────────

def chart_evolution_alertes(memory_file: str = "data/sentinel_memory.json") -> Optional[str]:
    """
    Histogramme alertes ouvertes / closes sur 30 jours.
    Source primaire  : table alertes (dateouverture/datecloture) via getdb().
    Fallback         : compressed_reports depuis memory JSON.
    A11-FIX          : guard alertes_ouvertes non-liste préservé.
    """
    dates, ouv, cls = _get_alertes_timeline(ndays=30)

    # Fallback memory JSON si table alertes vide
    if not dates:
        history = _load_memory_json(memory_file)
        if not history:
            return None
        dates = [h.get("date", "")[-5:] for h in history]
        # A11-FIX : alertes_ouvertes peut être une liste, un int, ou absent\n        ouv = [\n            len(v) if isinstance((v := h.get("alertes_ouvertes"), list) else 1\n            for h in history\n        ]
        cls = [0] * len(dates)

    if not dates:
        return None

    try:
        fig, ax = plt.subplots(figsize=(11, 4))
        x = list(range(len(dates)))
        ax.bar(x, ouv, color=COULEURS[1], alpha=0.8, label="Alertes ouvertes")
        ax.bar(x, [-c for c in cls], color=COULEURS[2], alpha=0.8, label="Alertes closes")
        ax.axhline(0, color=COULEURS[6], linewidth=0.8)
        ax.set_xticks(x[::max(1, len(x) // 8)])
        ax.set_xticklabels(dates[::max(1, len(x) // 8)], rotation=35, ha="right", fontsize=8)
        ax.set_ylabel("Nombre d'alertes", fontsize=10)
        ax.set_title("Évolution des alertes — 30 derniers jours", fontsize=11, fontweight="bold")
        ax.legend(fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        # V1-FIX: SVG inline first (smaller file), fallback PNG
        svg = _fig_to_svg(fig)
        if svg:
            out = CHARTS_DIR / "evolutionalertes.svg"
            out.write_text(svg, encoding="utf-8")
        else:
            out = CHARTS_DIR / "evolutionalertes.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return str(out)
    except Exception as e:
        log.error(f"CHARTS evolutionalertes : {e}")
        plt.close("all")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CHART 6 — HEATMAP CO-OCCURRENCE ACTEURS (PPMI activé — G2/M1-FIX)
# ─────────────────────────────────────────────────────────────────────────────

def chart_heatmap_acteurs(report_text: str = "") -> Optional[str]:
    """
    Heatmap de co-occurrence acteurs × fenêtre 250 chars.
    G2/M1-FIX : utilise _ppmi_matrix() au lieu des comptages bruts.
    CH-FIX-10  : co-occurrence LOCALE (fenêtre 250 chars, A12-FIX).
    CH-FIX-15  : Colormap YlOrRd pour contraste (A22-FIX).
    """
    if not report_text:
        return None

    text_low = report_text.lower()

    # Sélectionner les acteurs présents dans ce rapport
    present = [a for a in ACTEURS_SURVEILLES if a.lower() in text_low]
    if len(present) < 2:
        return None
    top_actors = present[:15]  # max 15 pour lisibilité

    # Construction matrice co-occurrence LOCALE (fenêtre 250 chars)
    from collections import defaultdict
    cooc: dict = {a: defaultdict(int) for a in top_actors}
    window = 250
    half_win = window // 2  # M-03-FIX : fenêtre symétrique centrée sur l'acteur
    for i, a in enumerate(top_actors):
        pos = 0
        a_low = a.lower()
        a_len = len(a_low)
        while True:
            idx = text_low.find(a_low, pos)
            if idx == -1:
                break
            # M-03-FIX : fenêtre [idx - half_win, idx + a_len + half_win]
            # Symétrique quel que soit la position dans le texte
            start  = max(0, idx - half_win)
            end    = min(len(text_low), idx + a_len + half_win)
            snippet = text_low[start:end]
            for b in top_actors:
                if b != a and b.lower() in snippet:
                    cooc[a][b] += 1
            pos = idx + 1

    # G2/M1-FIX : appliquer PPMI au lieu des comptages bruts
    ppmi = _ppmi_matrix(cooc, top_actors)

    # Construire matrice numpy depuis PPMI
    n = len(top_actors)
    matrix = np.zeros((n, n))
    for i, a in enumerate(top_actors):
        for j, b in enumerate(top_actors):
            matrix[i, j] = ppmi.get(a, {}).get(b, 0.0)

    if matrix.max() == 0:
        log.warning("CHARTS heatmap : matrice PPMI all-zeros — chart non généré")
        return None

    # M-02-FIX : normalisation [0,1] — comparaison cohérente inter-rapports
    matrix_max = matrix.max()
    if matrix_max > 0:
        matrix = matrix / matrix_max

    try:
        fig, ax = plt.subplots(figsize=(max(7, n * 0.6), max(6, n * 0.55)))
        im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto", vmin=0.0, vmax=1.0)
        fig.colorbar(im, ax=ax, label="Score PPMI normalisé [0-1]")
        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels([a[:12] for a in top_actors], rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels([a[:12] for a in top_actors], fontsize=8)
        ax.set_title(
            "Heatmap co-occurrence acteurs défense (PPMI)",
            fontsize=11, fontweight="bold",
        )
        fig.tight_layout()
        # V1-FIX: SVG inline first (smaller file), fallback PNG
        svg = _fig_to_svg(fig)
        if svg:
            out = CHARTS_DIR / "heatmapacteurs.svg"
            out.write_text(svg, encoding="utf-8")
        else:
            out = CHARTS_DIR / "heatmapacteurs.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return str(out)
    except Exception as e:
        log.error(f"CHARTS heatmapacteurs : {e}")
        plt.close("all")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE ALL CHARTS — orchestrateur principal (CH-FIX-13 safe_wrap)
# Chaque chart est isolé — une exception n'arrête pas les suivants.
# ─────────────────────────────────────────────────────────────────────────────

def generate_all_charts(report_text: str = "") -> list:
    """
    Génère tous les graphiques SENTINEL et retourne la liste des chemins.
    CH-FIX-13 : chaque chart est dans un try/except individuel.
    P2-FIX    : matplotlib fig fermé via try/finally dans chaque fonction.
    """
    results = []

    def _safe(fn, *args, **kwargs):
        try:
            path = fn(*args, **kwargs)
            if path:
                results.append(path)
                log.info(f"CHARTS {fn.__name__} → {path}")
            else:
                log.debug(f"CHARTS {fn.__name__} → None (données insuffisantes)")
        except Exception as exc:
            log.error(f"CHARTS {fn.__name__} échec : {exc}")

    _safe(chart_activite_30j)
    _safe(chart_repartition_geo)
    _safe(chart_radar_techno, report_text=report_text)
    _safe(chart_top_acteurs, report_text=report_text)
    _safe(chart_evolution_alertes)
    _safe(chart_heatmap_acteurs, report_text=report_text)

    log.info(f"CHARTS {len(results)}/{6} graphiques générés")
    return results
