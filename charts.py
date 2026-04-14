#!/usr/bin/env python3
# charts.py — SENTINEL v3.52 — Génération des graphiques du rapport
# ─────────────────────────────────────────────────────────────────────────────
# Génère les charts PNG injectés dans le rapport HTML par report_builder.
# Appelé depuis sentinel_main.py :
#   _timed("charts", generate_all_charts, report_text)
#
# Corrections v3.40 appliquées :
#   FIX-OBS1     Logging structuré via logger nommé, plus de print()
#   CHK-1        DPI configurable via ENV (SENTINEL_CHART_DPI, défaut 150)
#   CHK-2        optimize=True sur tous les PNG Matplotlib (zlib level 9)
#   CHK-3        Dimensions max configurables (SENTINEL_CHART_MAX_W/H)
#   CHK-4        Sous-échantillonnage séries temporelles > 60 points
#   CHK-5        _check_charts_budget() après génération
#   CHK-6        plt.close(fig) systématique — libération mémoire
#   CHK-7        Palette Nexus cohérente avec le design SENTINEL
#   CHK-8        Sauvegarde dans output/charts/ avec nommage daté
#   CHK-9        Fallback gracieux si données insuffisantes (< 3 points)
#   CHK-10       Métriques extraites depuis SentinelDB (source de vérité)
#
# CORRECTIONS v3.52 — Audit inter-scripts (SENTINEL audit 2026-04) :
#
# [CHT-52-PATH]  CHEMIN ABSOLU — OUTPUT_DIR utilisait Path("output/charts")
#                relatif au répertoire de travail. En cron (cd /), les charts
#                étaient créés dans /output/charts/ au lieu de ~/sentinel/output/.
#                Fix : _ROOT = Path(__file__).resolve().parent
#                      OUTPUT_DIR = _ROOT / "output" / "charts"
#
# [CHT-52-ENV]   load_dotenv() CONDITIONNELLE — appelée inconditionnellement
#                à l'import, même quand charts.py est importé par sentinel_main
#                qui a déjà chargé le .env. Désormais uniquement au lancement
#                CLI direct (if __name__ == "__main__"). Aucun effet fonctionnel
#                négatif : load_dotenv() est idempotent, mais le pattern est
#                incohérent avec les autres scripts du pipeline.
#
# [CHT-52-TEND]  CORRECTION CRITIQUE — chart_tendances_actives() utilisait
#                des champs inexistants dans le schéma DB :
#                  ❌ t.get("date_ouverture") → ✅ t.get("datepremiere")
#                  ❌ t.get("score", 5.0)     → ✅ count normalisé (0–10)
#                  ❌ t.get("tendance", "")   → ✅ t.get("texte", "")
#                  ❌ t.get("categorie")      → ✅ supprimé (inexistant en DB)
#                Résultat v3.40 : dates vides "2026-01-01", scores figés à 5.0,
#                labels vides, catégories "Autre" partout — chart inutilisable.
#                Fix : tous les champs correspondent au schéma tendances v3.52.
#                Axe Y rebaptisé "Occurrences" (count = nombre de fois vues).
#                Colorscale basée sur count normalisé (plus = plus persistant).
#                Marker size proportionnel à count (persistance visuelle).
#                Tooltip enrichi (date première + dernière apparition).
#
# [CHT-52-ACT]   chart_top_acteurs() utilisait a.get("score", 0) mais le champ
#                DB est "scoreactivite" (cf. DDL acteurs). Score retournait
#                toujours 0 → toutes les barres vides, tri incorrect.
#                Fix : a.get("scoreactivite", 0)
#
# [CHT-52-DB]    generate_all_charts() appelait SentinelDB.gettendances()
#                qui n'existait pas en v3.51 (AttributeError absorbé en
#                log.error et données vides). Corrigé dans db_manager v3.52 —
#                gettendances(actives_only=True) est maintenant disponible.
#
# [CHT-52-PAT]   NOUVEAU — chart_brevets() : courbe Matplotlib des métriques
#                brevets (nb_patents, nb_patents_new, nb_patents_critical) sur
#                30 jours. Données issues de getmetrics() (désormais incluses
#                grâce à DB-52-PAT). Généré uniquement si nb_patents > 0.
#
# [CHT-52-NLP]   NOUVEAU — chart_nlp_performance() : courbe double axe
#                Matplotlib (nb_articles + avg_nlp_score) sur 30 jours.
#                Données issues de SentinelDB.get_nlp_metrics(). Généré
#                uniquement si des métriques NLP sont disponibles en base.
#
# ─────────────────────────────────────────────────────────────────────────────
# Variables d'environnement optionnelles :
#   SENTINEL_CHART_DPI        défaut : 150   (300 = impression, 150 = écran/email)
#   SENTINEL_CHART_MAX_W      défaut : 900   (pixels largeur max)
#   SENTINEL_CHART_MAX_H      défaut : 500   (pixels hauteur max)
#   SENTINEL_CHART_SUBSAMPLE  défaut : 60    (sous-échantillonnage si > N points)
#   SMTP_ATTACH_WARN_MB       défaut : 10    (budget partagé avec mailer.py)
#   SENTINEL_CHARTS_PATENTS   défaut : 1     (0 = désactive chart brevets)
#   SENTINEL_CHARTS_NLP       défaut : 1     (0 = désactive chart NLP)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import datetime
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger("sentinel.charts")

# ── [CHT-52-PATH] Chemin absolu depuis la position de charts.py ───────────────
_ROOT = Path(__file__).resolve().parent

# ── Configuration charts (CHK-1 / CHK-3) — lus au runtime (pas à l'import) ──
CHART_DPI       = int(os.environ.get("SENTINEL_CHART_DPI",       "150"))
CHART_MAX_W     = int(os.environ.get("SENTINEL_CHART_MAX_W",     "900"))
CHART_MAX_H     = int(os.environ.get("SENTINEL_CHART_MAX_H",     "500"))
CHART_SUBSAMPLE = int(os.environ.get("SENTINEL_CHART_SUBSAMPLE", "60"))
SMTP_WARN_MB    = int(os.environ.get("SMTP_ATTACH_WARN_MB",      "10"))
CHARTS_PATENTS  = os.environ.get("SENTINEL_CHARTS_PATENTS", "1") != "0"
CHARTS_NLP      = os.environ.get("SENTINEL_CHARTS_NLP",     "1") != "0"

# ── [CHT-52-PATH] Dossier de sortie absolu ────────────────────────────────────
OUTPUT_DIR = _ROOT / "output" / "charts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Palette Nexus SENTINEL (CHK-7) ────────────────────────────────────────────
_C_PRIMARY       = "#01696f"   # Hydra Teal — courbe principale, barres accent
_C_WARNING       = "#964219"   # Terra Brown — alertes ORANGE
_C_SUCCESS       = "#437a22"   # Gridania Green — positif, VERT
_C_MUTED         = "#7a7974"   # Sylph Gray muted — grille, axes, texte secondaire
_C_FAINT         = "#bab9b4"   # Sylph Gray faint — éléments tertiaires
_C_BG            = "#f7f6f2"   # Nexus Beige — fond des charts
_C_SURFACE       = "#f9f8f5"   # Surface légèrement plus claire
_C_TEXT          = "#28251d"   # Texte principal
_C_ALERTE_ROUGE  = "#a12c7b"   # Jenova Maroon — alerte ROUGE
_C_ALERTE_ORANGE = "#da7101"   # Costa Orange — alerte ORANGE
_C_ALERTE_VERT   = "#437a22"   # Gridania Green — alerte VERT
_C_BLUE          = "#006494"   # Limsa Blue — second axe, séries secondaires
_C_GOLD          = "#d19900"   # Altana Gold — brevets critiques

# Mapping couleur alerte
_ALERTE_COLOR: dict[str, str] = {
    "ROUGE":  _C_ALERTE_ROUGE,
    "ORANGE": _C_ALERTE_ORANGE,
    "VERT":   _C_ALERTE_VERT,
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _today_prefix() -> str:
    return datetime.date.today().isoformat()


def _chart_path(name: str) -> Path:
    """Retourne le chemin absolu du PNG avec préfixe daté (CHK-8)."""
    return OUTPUT_DIR / f"{_today_prefix()}_{name}.png"


def _subsample(data: list, max_points: int = CHART_SUBSAMPLE) -> list:
    """
    CHK-4 : sous-échantillonnage si > max_points.
    1 point sur N conservé — imperceptible pour des tendances sur 30-90 jours.
    Conserve toujours le premier et le dernier point pour éviter les décalages.
    """
    if len(data) <= max_points:
        return data
    step = max(1, len(data) // max_points)
    sampled = data[::step]
    # Garantir le dernier point
    if sampled[-1] is not data[-1]:
        sampled = sampled + [data[-1]]
    return sampled


def _save_matplotlib(fig, path: Path) -> None:
    """
    CHK-1 + CHK-2 + CHK-6 : sauvegarde optimisée pour Matplotlib.
    - DPI configurable (défaut 150)
    - optimize=True : compression PNG zlib level 9 (~20% de gain)
    - metadata vide : supprime les métadonnées inutiles (~5 Ko/fichier)
    - plt.close() systématique : libération mémoire (CHK-6)
    """
    import matplotlib.pyplot as plt
    try:
        fig.savefig(
            str(path),
            dpi=CHART_DPI,
            bbox_inches="tight",
            format="png",
            optimize=True,
            metadata={"Comment": "", "Software": ""},
            facecolor=_C_BG,
            edgecolor="none",
        )
        size_kb = path.stat().st_size // 1024
        log.info(f"CHARTS Sauvegardé : {path.name} ({size_kb} Ko, {CHART_DPI} dpi)")
    except Exception as e:
        log.error(f"CHARTS Erreur sauvegarde {path.name} : {e}", exc_info=True)
    finally:
        plt.close(fig)  # CHK-6 : toujours libérer, même en cas d'erreur


def _save_plotly(fig, path: Path) -> None:
    """
    CHK-1 + CHK-3 : sauvegarde optimisée pour Plotly.
    - scale calculé depuis CHART_DPI (référence Plotly = 96 dpi)
    - width/height explicites pour éviter les charts surdimensionnés
    """
    try:
        scale = max(1.0, CHART_DPI / 96)
        fig.write_image(
            str(path),
            format="png",
            width=CHART_MAX_W,
            height=CHART_MAX_H,
            scale=scale,
        )
        size_kb = path.stat().st_size // 1024
        log.info(f"CHARTS Sauvegardé : {path.name} ({size_kb} Ko, scale={scale:.1f})")
    except Exception as e:
        log.error(f"CHARTS Erreur sauvegarde Plotly {path.name} : {e}", exc_info=True)


def _matplotlib_base_style(ax, title: str) -> None:
    """Style commun pour tous les charts Matplotlib (CHK-7)."""
    ax.set_facecolor(_C_BG)
    ax.figure.patch.set_facecolor(_C_BG)
    ax.set_title(title, color=_C_TEXT, fontsize=11, fontweight="bold", pad=12)
    ax.tick_params(colors=_C_MUTED, labelsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(_C_MUTED)
    ax.spines["bottom"].set_color(_C_MUTED)
    ax.grid(axis="y", color=_C_MUTED, alpha=0.2, linewidth=0.5)
    ax.xaxis.label.set_color(_C_MUTED)
    ax.yaxis.label.set_color(_C_MUTED)


def _chart_vide_matplotlib(title: str, message: str, path: Path) -> None:
    """
    CHK-9 : chart vide avec message explicite si données insuffisantes.
    Garantit un PNG valide en toutes circonstances pour report_builder.
    """
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    ax.text(
        0.5, 0.5, message,
        transform=ax.transAxes,
        ha="center", va="center",
        fontsize=10, color=_C_MUTED,
        style="italic",
    )
    ax.set_axis_off()
    _matplotlib_base_style(ax, title)
    _save_matplotlib(fig, path)


# ─────────────────────────────────────────────────────────────────────────────
# CHART 1 — Indice d'activité sur 30 jours
# ─────────────────────────────────────────────────────────────────────────────

def chart_indice_activite(metrics: list[dict]) -> Path:
    """
    Courbe Matplotlib : indice d'activité (0-10) sur les 30 derniers jours.
    Bandes colorées indiquant le niveau d'alerte (VERT/ORANGE/ROUGE).
    Moyenne mobile 7j superposée si assez de points.
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    path = _chart_path("indice_activite")

    if len(metrics) < 3:
        _chart_vide_matplotlib(
            "Indice d'activité — 30 jours",
            "Données insuffisantes (< 3 rapports)
Relancer après quelques cycles.",
            path,
        )
        return path

    metrics = _subsample(metrics, CHART_SUBSAMPLE)

    dates   = [datetime.date.fromisoformat(m["date"]) for m in metrics]
    scores  = [float(m.get("indice", 5.0)) for m in metrics]
    alertes = [m.get("alerte", "VERT") for m in metrics]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))

    # Bandes d'alerte en arrière-plan
    ax.axhspan(0, 4,  color=_C_ALERTE_VERT,   alpha=0.06)
    ax.axhspan(4, 7,  color=_C_ALERTE_ORANGE,  alpha=0.06)
    ax.axhspan(7, 10, color=_C_ALERTE_ROUGE,   alpha=0.06)

    # Courbe principale avec aire sous la courbe
    ax.plot(dates, scores, color=_C_PRIMARY, linewidth=1.8, zorder=3)
    ax.fill_between(dates, scores, alpha=0.08, color=_C_PRIMARY)

    # Points colorés selon niveau d'alerte
    for d, s, a in zip(dates, scores, alertes):
        ax.scatter(d, s, color=_ALERTE_COLOR.get(a, _C_PRIMARY),
                   s=22, zorder=4, linewidths=0)

    # Moyenne mobile 7j
    if len(scores) >= 7:
        window = 7
        ma = [
            sum(scores[max(0, i - window):i + 1]) / len(scores[max(0, i - window):i + 1])
            for i in range(len(scores))
        ]
        ax.plot(dates, ma, color=_C_MUTED, linewidth=1.0,
                linestyle="--", alpha=0.6, label="Moy. mobile 7j")
        ax.legend(fontsize=7, framealpha=0, labelcolor=_C_MUTED)

    ax.set_ylim(0, 10)
    ax.set_ylabel("Indice (0–10)", fontsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    import matplotlib.pyplot as plt_ref
    plt_ref.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=7)

    _matplotlib_base_style(ax, "Indice d'activité — 30 jours")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 2 — Distribution des niveaux d'alerte
# ─────────────────────────────────────────────────────────────────────────────

def chart_distribution_alertes(metrics: list[dict]) -> Path:
    """
    Barres Matplotlib : répartition VERT / ORANGE / ROUGE sur 30 jours.
    """
    import matplotlib.pyplot as plt

    path = _chart_path("distribution_alertes")

    if len(metrics) < 3:
        _chart_vide_matplotlib(
            "Distribution des alertes",
            "Données insuffisantes (< 3 rapports)",
            path,
        )
        return path

    counts = {"VERT": 0, "ORANGE": 0, "ROUGE": 0}
    for m in metrics:
        alerte = m.get("alerte", "VERT").upper()
        if alerte in counts:
            counts[alerte] += 1

    labels = list(counts.keys())
    values = list(counts.values())
    colors = [_ALERTE_COLOR[lbl] for lbl in labels]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    bars = ax.bar(labels, values, color=colors, width=0.5,
                  edgecolor="none", zorder=3)

    for bar, val in zip(bars, values):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                str(val),
                ha="center", va="bottom",
                fontsize=9, color=_C_TEXT, fontweight="bold",
            )

    ax.set_ylabel("Nombre de jours", fontsize=8)
    ax.set_ylim(0, max(values) * 1.25 + 1)
    _matplotlib_base_style(ax, "Distribution des niveaux d'alerte — 30 jours")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 3 — Volume d'articles par jour
# ─────────────────────────────────────────────────────────────────────────────

def chart_volume_articles(metrics: list[dict]) -> Path:
    """
    Barres groupées Matplotlib : nb_articles et nb_pertinents par jour sur 30 jours.
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np

    path = _chart_path("volume_articles")

    if len(metrics) < 3:
        _chart_vide_matplotlib(
            "Volume d'articles collectés",
            "Données insuffisantes (< 3 rapports)",
            path,
        )
        return path

    metrics = _subsample(metrics, CHART_SUBSAMPLE)

    dates         = [datetime.date.fromisoformat(m["date"]) for m in metrics]
    nb_articles   = [int(m.get("nb_articles",   0)) for m in metrics]
    nb_pertinents = [int(m.get("nb_pertinents", 0)) for m in metrics]

    x     = np.arange(len(dates))
    width = 0.4

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    ax.bar(x - width / 2, nb_articles,   width, color=_C_MUTED,   alpha=0.5,
           label="Total collectés",       zorder=3, edgecolor="none")
    ax.bar(x + width / 2, nb_pertinents, width, color=_C_PRIMARY, alpha=0.85,
           label="Pertinents (filtrés)",  zorder=3, edgecolor="none")

    ax.set_xticks(x)
    ax.set_xticklabels(
        [d.strftime("%d/%m") for d in dates],
        rotation=30, ha="right", fontsize=7,
    )
    ax.set_ylabel("Nombre d'articles", fontsize=8)
    ax.legend(fontsize=7, framealpha=0, labelcolor=_C_MUTED)
    _matplotlib_base_style(ax, "Volume d'articles collectés — 30 jours")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 4 — Acteurs les plus actifs (Top 10)
# ─────────────────────────────────────────────────────────────────────────────

def chart_top_acteurs(acteurs: list[dict]) -> Path:
    """
    Barres horizontales Matplotlib : top 10 acteurs par score d'activité.

    [CHT-52-ACT] CORRECTION : a.get("score") → a.get("scoreactivite")
    Le champ DB est "scoreactivite" (cf. DDL acteurs + getacteurs()).
    La v3.40 retournait 0 pour tous les acteurs → barres vides, tri KO.
    """
    import matplotlib.pyplot as plt

    path = _chart_path("top_acteurs")

    if not acteurs:
        _chart_vide_matplotlib(
            "Acteurs les plus actifs",
            "Aucun acteur en base
Lancer quelques cycles de collecte.",
            path,
        )
        return path

    # [CHT-52-ACT] Champ corrigé : scoreactivite (pas score)
    top = sorted(
        acteurs,
        key=lambda x: float(x.get("scoreactivite", 0)),
        reverse=True,
    )[:10]

    noms   = [a["nom"][:30] for a in reversed(top)]
    scores = [float(a.get("scoreactivite", 0)) for a in reversed(top)]

    max_s  = max(scores) if scores else 1
    colors = [
        _C_PRIMARY if s / max_s > 0.6 else
        _C_MUTED   if s / max_s > 0.3 else
        _C_FAINT
        for s in scores
    ]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    bars = ax.barh(noms, scores, color=colors, edgecolor="none", zorder=3)

    for bar, val in zip(bars, scores):
        ax.text(
            bar.get_width() + max_s * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}",
            va="center", fontsize=7, color=_C_MUTED,
        )

    ax.set_xlabel("Score d'activité", fontsize=8)
    ax.set_xlim(0, max_s * 1.15)
    _matplotlib_base_style(ax, "Top 10 acteurs les plus mentionnés")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 5 — Tendances actives (Plotly scatter)
# ─────────────────────────────────────────────────────────────────────────────

def chart_tendances_actives(tendances: list[dict]) -> Path:
    """
    Scatter Plotly : tendances actives positionnées par date de première
    apparition (X) et nombre d'occurrences (Y = persistance).

    [CHT-52-TEND] CORRECTIONS CRITIQUES des champs DB :
      ❌ t.get("date_ouverture") → ✅ t.get("datepremiere")
      ❌ t.get("score", 5.0)     → ✅ float(t.get("count", 1)) normalisé 0-10
      ❌ t.get("tendance", "")   → ✅ t.get("texte", "")
      ❌ t.get("categorie")      → ✅ supprimé (inexistant dans le schéma)

    Colorscale basée sur count normalisé : vert (peu fréquent) →
    teal (fréquent) → rouge (très persistant).
    Marker size proportionnel à count pour la lisibilité visuelle.
    Tooltip enrichi avec date première + dernière apparition.
    """
    path = _chart_path("tendances_actives")

    if not tendances:
        _chart_vide_matplotlib(
            "Tendances actives",
            "Aucune tendance en base",
            path,
        )
        return path

    try:
        import plotly.graph_objects as go

        tendances = _subsample(tendances, CHART_SUBSAMPLE)

        # [CHT-52-TEND] Champs corrigés : datepremiere, count, texte
        dates_premiere = [
            t.get("datepremiere", datetime.date.today().isoformat())
            for t in tendances
        ]
        dates_derniere = [
            t.get("datederniere", datetime.date.today().isoformat())
            for t in tendances
        ]
        counts = [int(t.get("count", 1)) for t in tendances]
        labels = [t.get("texte", "")[:40] for t in tendances]

        # Normalisation count → 0.0-10.0 pour l'axe Y et colorscale
        max_count = max(counts) if counts else 1
        scores_norm = [round(c / max_count * 10, 2) for c in counts]

        # Marker size proportionnel à count (min 8, max 24)
        sizes = [max(8, min(24, 8 + c / max_count * 16)) for c in counts]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates_premiere,
            y=scores_norm,
            mode="markers+text",
            text=labels,
            textposition="top center",
            textfont=dict(size=7, color=_C_MUTED),
            marker=dict(
                size=sizes,
                color=scores_norm,
                colorscale=[
                    [0.0, _C_ALERTE_VERT],
                    [0.5, _C_PRIMARY],
                    [1.0, _C_ALERTE_ROUGE],
                ],
                cmin=0,
                cmax=10,
                showscale=True,
                colorbar=dict(
                    title=dict(
                        text="Persistance",
                        font=dict(size=9, color=_C_MUTED),
                    ),
                    thickness=10,
                    tickfont=dict(size=8, color=_C_MUTED),
                    tickvals=[0, 5, 10],
                    ticktext=["Rare", "Fréquent", "Très persistant"],
                ),
                line=dict(width=0),
            ),
            # [CHT-52-TEND] Tooltip enrichi avec les vrais champs
            customdata=list(zip(counts, dates_premiere, dates_derniere)),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Occurrences : %{customdata[0]}<br>"
                "Première apparition : %{customdata[1]}<br>"
                "Dernière apparition : %{customdata[2]}"
                "<extra></extra>"
            ),
        ))

        fig.update_layout(
            title=dict(
                text="Tendances actives — persistance et chronologie",
                font=dict(size=12, color=_C_TEXT),
                x=0.05,
            ),
            paper_bgcolor=_C_BG,
            plot_bgcolor=_C_SURFACE,
            xaxis=dict(
                title=dict(
                    text="Première apparition",
                    font=dict(size=9, color=_C_MUTED),
                ),
                tickfont=dict(size=8, color=_C_MUTED),
                gridcolor=_C_MUTED,
                gridwidth=0.3,
                showline=False,
            ),
            yaxis=dict(
                title=dict(
                    text="Occurrences normalisées (0–10)",
                    font=dict(size=9, color=_C_MUTED),
                ),
                tickfont=dict(size=8, color=_C_MUTED),
                gridcolor=_C_MUTED,
                gridwidth=0.3,
                range=[0, 11],
            ),
            margin=dict(l=60, r=40, t=55, b=55),
            width=CHART_MAX_W,
            height=CHART_MAX_H,
        )

        _save_plotly(fig, path)

    except ImportError:
        log.warning("CHARTS plotly absent — chart tendances ignoré (pip install plotly kaleido)")
        _chart_vide_matplotlib(
            "Tendances actives",
            "plotly non installé
pip install plotly kaleido",
            path,
        )

    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 6 — Métriques brevets (nouveau — CHT-52-PAT)
# ─────────────────────────────────────────────────────────────────────────────

def chart_brevets(metrics: list[dict]) -> Path | None:
    """
    [CHT-52-PAT] Courbe multi-axes Matplotlib : nb_patents total, nouveaux,
    et critiques sur 30 jours.

    Généré uniquement si au moins un jour a nb_patents > 0.
    Retourne None si pas de données brevets disponibles.

    Source : SentinelDB.getmetrics() qui inclut désormais
    nb_patents, nb_patents_new, nb_patents_critical (DB-52-PAT).
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    path = _chart_path("brevets")

    # Filtrer uniquement les métriques avec au moins 1 brevet
    metrics_pat = [m for m in metrics if int(m.get("nb_patents", 0)) > 0]

    if len(metrics_pat) < 2:
        log.info("CHARTS chart_brevets : données insuffisantes — chart ignoré")
        return None

    metrics_pat = _subsample(metrics_pat, CHART_SUBSAMPLE)

    dates     = [datetime.date.fromisoformat(m["date"]) for m in metrics_pat]
    total     = [int(m.get("nb_patents",          0)) for m in metrics_pat]
    nouveaux  = [int(m.get("nb_patents_new",      0)) for m in metrics_pat]
    critiques = [int(m.get("nb_patents_critical", 0)) for m in metrics_pat]

    fig, ax1 = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))

    # Axe principal : total et nouveaux brevets
    ax1.fill_between(dates, total, alpha=0.10, color=_C_PRIMARY)
    ax1.plot(dates, total,    color=_C_PRIMARY, linewidth=1.8,
             label="Total brevets", zorder=3)
    ax1.plot(dates, nouveaux, color=_C_SUCCESS, linewidth=1.4,
             linestyle="--", label="Nouveaux", zorder=3)

    ax1.set_ylabel("Nombre de brevets", fontsize=8, color=_C_PRIMARY)
    ax1.tick_params(axis="y", colors=_C_PRIMARY, labelsize=8)

    # Axe secondaire : brevets critiques (couleur or)
    ax2 = ax1.twinx()
    ax2.bar(dates, critiques, color=_C_GOLD, alpha=0.5,
            width=0.6, label="Critiques", zorder=2)
    ax2.set_ylabel("Brevets critiques", fontsize=8, color=_C_GOLD)
    ax2.tick_params(axis="y", colors=_C_GOLD, labelsize=8)
    ax2.spines["right"].set_color(_C_GOLD)
    ax2.yaxis.label.set_color(_C_GOLD)

    # Légende combinée des deux axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    bars2,  labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + bars2, labels1 + labels2,
               fontsize=7, framealpha=0, labelcolor=_C_MUTED, loc="upper left")

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    import matplotlib.pyplot as plt_ref
    plt_ref.setp(ax1.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=7)

    _matplotlib_base_style(ax1, "Métriques brevets — 30 jours")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 7 — Performance NLP scorer (nouveau — CHT-52-NLP)
# ─────────────────────────────────────────────────────────────────────────────

def chart_nlp_performance(nlp_metrics: list[dict]) -> Path | None:
    """
    [CHT-52-NLP] Courbe double-axe Matplotlib :
      - Axe gauche  : nb_articles + nb_reranked (barres groupées)
      - Axe droit   : avg_nlp_score (courbe, 0.0-1.0)

    Visualise l'efficacité du NLP scorer : combien d'articles sont reranked
    et quelle est la qualité moyenne du scoring.

    Retourne None si pas de métriques NLP disponibles.
    Source : SentinelDB.get_nlp_metrics() (DB-52-NLP).
    """
    import matplotlib.pyplot as plt
    import numpy as np

    path = _chart_path("nlp_performance")

    if len(nlp_metrics) < 2:
        log.info("CHARTS chart_nlp_performance : données insuffisantes — chart ignoré")
        return None

    nlp_metrics = _subsample(nlp_metrics, CHART_SUBSAMPLE)

    dates      = [m["date"] for m in nlp_metrics]
    nb_arts    = [int(m.get("nb_articles",  0)) for m in nlp_metrics]
    nb_reranked= [int(m.get("nb_reranked",  0)) for m in nlp_metrics]
    avg_scores = [float(m.get("avg_nlp_score", 0.0)) for m in nlp_metrics]

    x     = np.arange(len(dates))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))

    # Barres groupées (articles + reranked)
    ax1.bar(x - width / 2, nb_arts,     width, color=_C_MUTED,   alpha=0.5,
            label="Articles traités", edgecolor="none", zorder=3)
    ax1.bar(x + width / 2, nb_reranked, width, color=_C_PRIMARY, alpha=0.85,
            label="Reranked NLP",     edgecolor="none", zorder=3)
    ax1.set_ylabel("Nombre d'articles", fontsize=8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(
        [d[:7] if len(d) >= 7 else d for d in dates],
        rotation=30, ha="right", fontsize=7,
    )
    ax1.tick_params(axis="y", labelsize=8)

    # Axe secondaire : score NLP moyen
    ax2 = ax1.twinx()
    ax2.plot(x, avg_scores, color=_C_BLUE, linewidth=1.6,
             marker="o", markersize=4, label="Score NLP moy.", zorder=4)
    ax2.set_ylabel("Score NLP moyen (0–1)", fontsize=8, color=_C_BLUE)
    ax2.tick_params(axis="y", colors=_C_BLUE, labelsize=8)
    ax2.set_ylim(0, 1.1)
    ax2.spines["right"].set_color(_C_BLUE)
    ax2.yaxis.label.set_color(_C_BLUE)

    # Légende combinée
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2,
               fontsize=7, framealpha=0, labelcolor=_C_MUTED, loc="upper left")

    _matplotlib_base_style(ax1, "Performance NLP scorer — 30 jours")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# VÉRIFICATION BUDGET POIDS — CHK-5
# ─────────────────────────────────────────────────────────────────────────────

def _check_charts_budget(generated_paths: list[Path]) -> None:
    """
    CHK-5 : Vérifie le poids total des charts générés.

    Alerte si > 60% de SMTP_ATTACH_WARN_MB pour anticiper le dépassement
    de la limite SMTP une fois le PDF ajouté.

    Seuil à 60% (non 100%) car :
      - PDF WeasyPrint ajoute typiquement 2-5 Mo
      - Encode base64 MIME ajoute ~33% de surcharge
      - Alerter AVANT l'envoi, pas après déclenchement du light mode
    """
    existing = [p for p in generated_paths if p is not None and p.exists()]
    if not existing:
        return

    total_bytes = sum(p.stat().st_size for p in existing)
    total_mb    = total_bytes / (1024 * 1024)
    budget_mb   = SMTP_WARN_MB * 0.6
    sizes       = {p.name: f"{p.stat().st_size // 1024} Ko" for p in existing}

    if total_mb > budget_mb:
        log.warning(
            f"CHARTS Budget poids : {total_mb:.1f} Mo pour {len(existing)} charts "
            f"({sizes}). "
            f"Risque de déclencher le light mode mailer.py une fois le PDF ajouté "
            f"({total_mb:.1f} Mo charts + ~3 Mo PDF + 33% base64 MIME ≈ "
            f"{total_mb * 1.33 + 3:.1f} Mo). "
            f"Action : réduire SENTINEL_CHART_DPI (actuellement {CHART_DPI} → essayer 100) "
            f"ou augmenter SMTP_ATTACH_WARN_MB dans .env."
        )
    else:
        log.info(
            f"CHARTS Budget OK : {total_mb:.1f} Mo / {budget_mb:.1f} Mo seuil "
            f"({len(existing)} charts, {CHART_DPI} dpi) ✓"
        )


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def generate_all_charts(report_text: str) -> dict[str, Path]:
    """
    Génère tous les charts du rapport et retourne un dict {nom: Path}.
    Appelé par sentinel_main.py : _timed("charts", generate_all_charts, report_text)

    Architecture :
      1. Charge les métriques depuis SentinelDB (CHK-10 — source de vérité)
      2. Génère chaque chart avec les optimisations DPI/taille/compression
      3. Génère les charts optionnels (brevets, NLP) si données disponibles
      4. Vérifie le budget poids total (CHK-5)

    En cas d'erreur sur un chart individuel, les autres continuent
    (log.error + fallback chart vide via CHK-9).

    [CHT-52-DB] SentinelDB.gettendances(actives_only=True) est maintenant
    disponible en db_manager v3.52 — l'AttributeError v3.51 est corrigé.
    """
    log.info(
        f"CHARTS Démarrage génération — DPI={CHART_DPI}, "
        f"max={CHART_MAX_W}×{CHART_MAX_H}px, subsample={CHART_SUBSAMPLE}pts, "
        f"output={OUTPUT_DIR}"
    )

    # ── Chargement des données depuis SentinelDB (CHK-10) ────────────────────
    metrics_30j: list[dict] = []
    acteurs:     list[dict] = []
    tendances:   list[dict] = []
    nlp_metrics: list[dict] = []

    try:
        from db_manager import SentinelDB

        metrics_30j = SentinelDB.getmetrics(ndays=30)
        acteurs     = SentinelDB.getacteurs(limit=20)

        # [CHT-52-DB] gettendances(actives_only=True) — corrigé en db_manager v3.52
        tendances   = SentinelDB.gettendances(actives_only=True)

        # [CHT-52-NLP] Métriques NLP (nouvelles en DB v3.52)
        if CHARTS_NLP:
            try:
                nlp_metrics = SentinelDB.get_nlp_metrics(ndays=30)
            except AttributeError:
                log.debug("CHARTS get_nlp_metrics() indisponible (db_manager < v3.52)")
                nlp_metrics = []

        log.info(
            f"CHARTS Données chargées : {len(metrics_30j)} métriques, "
            f"{len(acteurs)} acteurs, {len(tendances)} tendances, "
            f"{len(nlp_metrics)} métriques NLP"
        )

    except ImportError:
        log.warning("CHARTS db_manager absent — charts générés sans données historiques")
    except Exception as e:
        log.error(f"CHARTS Erreur chargement SentinelDB : {e}", exc_info=True)

    # ── Charts obligatoires (toujours générés, fallback si vide) ─────────────
    generated: dict[str, Path] = {}

    _chart_funcs_obligatoires = [
        ("indice_activite",      lambda: chart_indice_activite(metrics_30j)),
        ("distribution_alertes", lambda: chart_distribution_alertes(metrics_30j)),
        ("volume_articles",      lambda: chart_volume_articles(metrics_30j)),
        ("top_acteurs",          lambda: chart_top_acteurs(acteurs)),
        ("tendances_actives",    lambda: chart_tendances_actives(tendances)),
    ]

    for name, fn in _chart_funcs_obligatoires:
        try:
            path = fn()
            generated[name] = path
        except Exception as e:
            log.error(f"CHARTS Erreur chart '{name}' : {e}", exc_info=True)
            fallback = _chart_path(name)
            try:
                _chart_vide_matplotlib(
                    name.replace("_", " ").title(),
                    f"Erreur génération :
{str(e)[:80]}",
                    fallback,
                )
                generated[name] = fallback
            except Exception as fe:
                log.error(f"CHARTS Fallback chart vide échoué pour '{name}' : {fe}")

    # ── Charts optionnels (ignorés si pas de données) ─────────────────────────

    # [CHT-52-PAT] Chart brevets — seulement si données disponibles
    if CHARTS_PATENTS:
        try:
            pat_path = chart_brevets(metrics_30j)
            if pat_path is not None:
                generated["brevets"] = pat_path
                log.info("CHARTS chart_brevets généré ✓")
            else:
                log.info("CHARTS chart_brevets ignoré (aucune métrique brevet en base)")
        except Exception as e:
            log.error(f"CHARTS Erreur chart 'brevets' : {e}", exc_info=True)

    # [CHT-52-NLP] Chart NLP — seulement si données disponibles
    if CHARTS_NLP and nlp_metrics:
        try:
            nlp_path = chart_nlp_performance(nlp_metrics)
            if nlp_path is not None:
                generated["nlp_performance"] = nlp_path
                log.info("CHARTS chart_nlp_performance généré ✓")
            else:
                log.info("CHARTS chart_nlp_performance ignoré (métriques NLP insuffisantes)")
        except Exception as e:
            log.error(f"CHARTS Erreur chart 'nlp_performance' : {e}", exc_info=True)

    # ── CHK-5 : vérification budget poids ────────────────────────────────────
    _check_charts_budget(list(generated.values()))

    log.info(
        f"CHARTS {len(generated)} charts générés "
        f"({len(_chart_funcs_obligatoires)} obligatoires + optionnels) → {OUTPUT_DIR}/"
    )
    return generated


# ─────────────────────────────────────────────────────────────────────────────
# CLI — test rapide en développement
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # [CHT-52-ENV] load_dotenv() uniquement au lancement CLI direct
    try:
        from dotenv import load_dotenv
        load_dotenv(_ROOT / ".env")
    except ImportError:
        pass

    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    log.info(
        f"CHARTS CLI — DPI={CHART_DPI}, max={CHART_MAX_W}×{CHART_MAX_H}px, "
        f"output={OUTPUT_DIR}"
    )

    # Vérification des dépendances
    if "--check-deps" in sys.argv:
        missing = []
        for pkg in ["matplotlib", "plotly", "kaleido", "numpy"]:
            try:
                __import__(pkg)
                log.info(f"  ✓ {pkg}")
            except ImportError:
                log.warning(f"  ✗ {pkg} — pip install {pkg}")
                missing.append(pkg)
        sys.exit(1 if missing else 0)

    # Test avec données synthétiques si --test-data
    if "--test-data" in sys.argv:
        log.info("CHARTS mode test avec données synthétiques")
        import random
        today = datetime.date.today()

        fake_metrics = [
            {
                "date":             (today - datetime.timedelta(days=i)).isoformat(),
                "indice":           round(4.0 + random.gauss(0, 1.5), 1),
                "alerte":           random.choice(["VERT", "VERT", "ORANGE", "ROUGE"]),
                "nb_articles":      random.randint(80, 200),
                "nb_pertinents":    random.randint(20, 80),
                "nb_patents":       random.randint(0, 15),
                "nb_patents_new":   random.randint(0, 5),
                "nb_patents_critical": random.randint(0, 3),
                "top_patent_score": round(random.uniform(4.0, 9.5), 1),
            }
            for i in range(30)
        ]
        fake_metrics[0]["indice"] = max(0, min(10, fake_metrics[0]["indice"]))

        fake_acteurs = [
            {
                "nom":              f"Acteur Demo {i:02d}",
                "pays":             random.choice(["USA", "GBR", "FRA", "DEU", "ISR"]),
                "scoreactivite":    round(random.uniform(1.0, 10.0), 1),
                "derniereactivite": today.isoformat(),
            }
            for i in range(15)
        ]

        fake_tendances = [
            {
                "texte":        f"Tendance de démonstration n°{i:02d}",
                "count":        random.randint(1, 25),
                "datepremiere": (today - datetime.timedelta(days=random.randint(1, 60))).isoformat(),
                "datederniere": today.isoformat(),
                "active":       1,
            }
            for i in range(20)
        ]

        fake_nlp = [
            {
                "date":          (today - datetime.timedelta(days=i)).isoformat(),
                "nb_articles":   random.randint(80, 200),
                "nb_reranked":   random.randint(50, 150),
                "avg_nlp_score": round(random.uniform(0.50, 0.95), 3),
                "backend":       "tfidf",
            }
            for i in range(15)
        ]

        generated: dict[str, Path] = {}

        for name, fn in [
            ("indice_activite",      lambda: chart_indice_activite(fake_metrics)),
            ("distribution_alertes", lambda: chart_distribution_alertes(fake_metrics)),
            ("volume_articles",      lambda: chart_volume_articles(fake_metrics)),
            ("top_acteurs",          lambda: chart_top_acteurs(fake_acteurs)),
            ("tendances_actives",    lambda: chart_tendances_actives(fake_tendances)),
        ]:
            try:
                p = fn()
                generated[name] = p
            except Exception as e:
                log.error(f"  ✗ {name} : {e}", exc_info=True)

        # Charts optionnels
        try:
            p = chart_brevets(fake_metrics)
            if p:
                generated["brevets"] = p
        except Exception as e:
            log.error(f"  ✗ brevets : {e}")

        try:
            p = chart_nlp_performance(fake_nlp)
            if p:
                generated["nlp_performance"] = p
        except Exception as e:
            log.error(f"  ✗ nlp_performance : {e}")

        _check_charts_budget(list(generated.values()))

        print(f"
{'='*65}")
        print(f"✅  {len(generated)} charts générés (mode --test-data)")
        for name, path in generated.items():
            size_kb = path.stat().st_size // 1024 if path.exists() else 0
            print(f"  {name:<25} → {path.name} ({size_kb} Ko)")
        print(f"{'='*65}")
        sys.exit(0)

    # Génération réelle depuis SentinelDB
    result = generate_all_charts("")
    log.info(f"CHARTS Résultat : {len(result)} charts générés")
    for name, path in result.items():
        size_kb = path.stat().st_size // 1024 if path.exists() else 0
        print(f"  {name:<25} → {path.name} ({size_kb} Ko)")

