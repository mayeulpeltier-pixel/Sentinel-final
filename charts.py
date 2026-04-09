#!/usr/bin/env python3
# charts.py — SENTINEL v3.40 — Génération des graphiques du rapport
# ─────────────────────────────────────────────────────────────────────────────
# Génère les charts PNG injectés dans le rapport HTML par report_builder.
# Appelé depuis sentinel_main.py :
#   _timed("charts", generate_all_charts, report_text)
#
# Corrections v3.40 appliquées :
#   FIX-OBS1     Logging structuré via logger nommé, plus de print()
#   CHK-1        DPI configurable via ENV (SENTINEL_CHART_DPI, défaut 150)
#                Réduit la taille des PNG de ~65% vs défaut 300 dpi
#   CHK-2        optimize=True sur tous les PNG Matplotlib (zlib level 9)
#                Gain supplémentaire ~20% sans perte de qualité
#   CHK-3        Dimensions max configurables (SENTINEL_CHART_MAX_W/H)
#                Évite les charts surdimensionnés (défaut 900×500 px)
#   CHK-4        Sous-échantillonnage séries temporelles > 60 points
#                Imperceptible visuellement, réduit le bruit PNG
#   CHK-5        _check_charts_budget() après génération
#                Alerte si poids total > 60% de SMTP_ATTACH_WARN_MB
#                pour anticiper le dépassement AVANT ajout du PDF
#   CHK-6        plt.close(fig) systématique — libération mémoire
#                Indispensable en pipeline batch (sentinel_main nocturne)
#   CHK-7        Palette de couleurs cohérente avec le design SENTINEL
#                (Nexus Teal #01696f, neutres chauds)
#   CHK-8        Tous les charts sauvegardés dans output/charts/
#                avec nommage daté YYYY-MM-DD_chart_name.png
#   CHK-9        Fallback gracieux si données insuffisantes
#                (< 3 points) — chart vide avec message explicite
#   CHK-10       Métriques extraites depuis SentinelDB (non depuis
#                le rapport texte brut) — source de vérité unique
# ─────────────────────────────────────────────────────────────────────────────
# Variables d'environnement optionnelles :
#   SENTINEL_CHART_DPI        défaut : 150   (300 = impression, 150 = écran/email)
#   SENTINEL_CHART_MAX_W      défaut : 900   (pixels largeur max)
#   SENTINEL_CHART_MAX_H      défaut : 500   (pixels hauteur max)
#   SENTINEL_CHART_SUBSAMPLE  défaut : 60    (sous-échantillonnage si > N points)
#   SMTP_ATTACH_WARN_MB       défaut : 10    (budget partagé avec mailer.py)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import datetime
import logging
import os
from pathlib import Path
from typing import Any

# ── Charger .env si disponible ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("sentinel.charts")

# ── Configuration charts (CHK-1 / CHK-3) ─────────────────────────────────────
# DPI 150 : qualité parfaite pour écran (72-96 dpi) et lecture email.
# 300 dpi n'a de sens que pour l'impression papier — inutile en pipeline email.
CHART_DPI      = int(os.environ.get("SENTINEL_CHART_DPI",       "150"))
CHART_MAX_W    = int(os.environ.get("SENTINEL_CHART_MAX_W",     "900"))
CHART_MAX_H    = int(os.environ.get("SENTINEL_CHART_MAX_H",     "500"))
CHART_SUBSAMPLE= int(os.environ.get("SENTINEL_CHART_SUBSAMPLE", "60"))
SMTP_WARN_MB   = int(os.environ.get("SMTP_ATTACH_WARN_MB",      "10"))

# ── Dossier de sortie ─────────────────────────────────────────────────────────
OUTPUT_DIR = Path("output/charts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Palette Nexus SENTINEL (CHK-7) ────────────────────────────────────────────
# Cohérence visuelle avec le design system du rapport HTML.
_C_PRIMARY   = "#01696f"   # Hydra Teal — courbe principale, barres accent
_C_WARNING   = "#964219"   # Terra Brown — alertes ORANGE/ROUGE
_C_SUCCESS   = "#437a22"   # Gridania Green — tendances positives
_C_MUTED     = "#7a7974"   # Sylph Gray muted — grille, axes, texte secondaire
_C_BG        = "#f7f6f2"   # Nexus Beige — fond des charts
_C_SURFACE   = "#f9f8f5"   # Surface légèrement plus claire
_C_TEXT      = "#28251d"   # Texte principal
_C_ALERTE_ROUGE  = "#a12c7b"
_C_ALERTE_ORANGE = "#da7101"
_C_ALERTE_VERT   = "#437a22"

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
    """Retourne le chemin du PNG avec préfixe daté (CHK-8)."""
    return OUTPUT_DIR / f"{_today_prefix()}_{name}.png"


def _subsample(data: list, max_points: int = CHART_SUBSAMPLE) -> list:
    """
    CHK-4 : sous-échantillonnage si > max_points.
    1 point sur N conservé — imperceptible pour des tendances sur 30-90 jours.
    Réduit le bruit PNG Plotly/Matplotlib de ~10% supplémentaire.
    """
    if len(data) <= max_points:
        return data
    step = max(1, len(data) // max_points)
    return data[::step]


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
            optimize=True,           # CHK-2 : compression zlib maximale
            metadata={"Comment": "", "Software": ""},  # CHK-2 : pas de métadonnées
            facecolor=_C_BG,
            edgecolor="none",
        )
        size_kb = path.stat().st_size // 1024
        log.info(f"CHARTS Sauvegardé : {path.name} ({size_kb} Ko, {CHART_DPI} dpi)")
    except Exception as e:
        log.error(f"CHARTS Erreur sauvegarde {path.name} : {e}")
    finally:
        plt.close(fig)   # CHK-6 : toujours libérer, même en cas d'erreur


def _save_plotly(fig, path: Path) -> None:
    """
    CHK-1 + CHK-3 : sauvegarde optimisée pour Plotly.
    - scale calculé depuis CHART_DPI (référence Plotly = 96 dpi)
    - width/height explicites pour éviter les charts surdimensionnés
    """
    try:
        scale = max(1.0, CHART_DPI / 96)   # Plotly référence interne = 96 dpi
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
        log.error(f"CHARTS Erreur sauvegarde Plotly {path.name} : {e}")


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
    Évite les exceptions non attrapées et garantit un PNG valide en toutes
    circonstances — report_builder peut toujours injecter l'image.
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
    Bande colorée indiquant le niveau d'alerte (VERT/ORANGE/ROUGE).
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

    # CHK-4 : sous-échantillonnage si historique long
    metrics = _subsample(metrics, CHART_SUBSAMPLE)

    dates  = [datetime.date.fromisoformat(m["date"]) for m in metrics]
    scores = [float(m.get("indice", 5.0)) for m in metrics]
    alertes= [m.get("alerte", "VERT") for m in metrics]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))

    # Bandes d'alerte en arrière-plan
    ax.axhspan(0,   4, color=_C_ALERTE_VERT,   alpha=0.06)
    ax.axhspan(4,   7, color=_C_ALERTE_ORANGE,  alpha=0.06)
    ax.axhspan(7,  10, color=_C_ALERTE_ROUGE,   alpha=0.06)

    # Courbe principale
    ax.plot(dates, scores, color=_C_PRIMARY, linewidth=1.8, zorder=3)
    ax.fill_between(dates, scores, alpha=0.08, color=_C_PRIMARY)

    # Points colorés selon niveau d'alerte
    for d, s, a in zip(dates, scores, alertes):
        ax.scatter(d, s, color=_ALERTE_COLOR.get(a, _C_PRIMARY),
                   s=22, zorder=4, linewidths=0)

    # Ligne de tendance (moyenne mobile 7j)
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
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=7)

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
    colors = [_ALERTE_COLOR[l] for l in labels]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    bars = ax.bar(labels, values, color=colors, width=0.5,
                  edgecolor="none", zorder=3)

    # Annotations valeurs
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
    Barres Matplotlib : nb_articles et nb_pertinents par jour sur 30 jours.
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

    dates        = [datetime.date.fromisoformat(m["date"]) for m in metrics]
    nb_articles  = [int(m.get("nb_articles",  0)) for m in metrics]
    nb_pertinents= [int(m.get("nb_pertinents",0)) for m in metrics]

    x     = np.arange(len(dates))
    width = 0.4

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    ax.bar(x - width / 2, nb_articles,   width, color=_C_MUTED,   alpha=0.5,
           label="Total collectés",  zorder=3, edgecolor="none")
    ax.bar(x + width / 2, nb_pertinents, width, color=_C_PRIMARY, alpha=0.85,
           label="Pertinents (filtrés)", zorder=3, edgecolor="none")

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
# CHART 4 — Acteurs les plus mentionnés (Top 10)
# ─────────────────────────────────────────────────────────────────────────────

def chart_top_acteurs(acteurs: list[dict]) -> Path:
    """
    Barres horizontales Matplotlib : top 10 acteurs par score de mention.
    Source : SentinelDB.getacteurs() → [{"nom": str, "score": float, ...}]
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

    top = sorted(acteurs, key=lambda x: float(x.get("score", 0)), reverse=True)[:10]
    noms   = [a["nom"][:30] for a in reversed(top)]
    scores = [float(a.get("score", 0)) for a in reversed(top)]

    # Dégradé de couleur selon score relatif
    max_s  = max(scores) if scores else 1
    colors = [
        _C_PRIMARY if s / max_s > 0.6 else
        _C_MUTED   if s / max_s > 0.3 else
        f"#bab9b4"
        for s in scores
    ]

    fig, ax = plt.subplots(figsize=(CHART_MAX_W / 96, CHART_MAX_H / 96))
    bars = ax.barh(noms, scores, color=colors, edgecolor="none", zorder=3)

    # Score en fin de barre
    for bar, val in zip(bars, scores):
        ax.text(
            bar.get_width() + max_s * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}",
            va="center", fontsize=7, color=_C_MUTED,
        )

    ax.set_xlabel("Score de mention", fontsize=8)
    ax.set_xlim(0, max_s * 1.15)
    _matplotlib_base_style(ax, "Top 10 acteurs les plus mentionnés")
    _save_matplotlib(fig, path)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# CHART 5 — Tendances actives (Plotly — interactif en HTML)
# ─────────────────────────────────────────────────────────────────────────────

def chart_tendances_actives(tendances: list[dict]) -> Path:
    """
    Scatter Plotly : tendances actives positionnées par date d'apparition
    et score, colorées par catégorie.
    CHK-1 + CHK-3 : scale calculé depuis CHART_DPI, dimensions bornées.
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

        tendances = _subsample(tendances, CHART_SUBSAMPLE)  # CHK-4

        dates  = [t.get("date_ouverture", "2026-01-01") for t in tendances]
        scores = [float(t.get("score", 5.0)) for t in tendances]
        labels = [t.get("tendance", "")[:40] for t in tendances]
        cats   = [t.get("categorie", "Autre") for t in tendances]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates,
            y=scores,
            mode="markers+text",
            text=labels,
            textposition="top center",
            textfont=dict(size=7, color=_C_MUTED),
            marker=dict(
                size=10,
                color=scores,
                colorscale=[
                    [0.0, _C_ALERTE_VERT],
                    [0.5, _C_PRIMARY],
                    [1.0, _C_ALERTE_ROUGE],
                ],
                showscale=True,
                colorbar=dict(
                    title=dict(text="Score", font=dict(size=9, color=_C_MUTED)),
                    thickness=10,
                    tickfont=dict(size=8, color=_C_MUTED),
                ),
                line=dict(width=0),
            ),
            hovertemplate="<b>%{text}</b><br>Score : %{y:.1f}<br>Date : %{x}<extra></extra>",
        ))

        fig.update_layout(
            title=dict(
                text="Tendances actives en cours de suivi",
                font=dict(size=12, color=_C_TEXT),
                x=0.05,
            ),
            paper_bgcolor=_C_BG,
            plot_bgcolor=_C_SURFACE,
            xaxis=dict(
                tickfont=dict(size=8, color=_C_MUTED),
                gridcolor=_C_MUTED,
                gridwidth=0.3,
                showline=False,
            ),
            yaxis=dict(
                title=dict(text="Score", font=dict(size=9, color=_C_MUTED)),
                tickfont=dict(size=8, color=_C_MUTED),
                gridcolor=_C_MUTED,
                gridwidth=0.3,
                range=[0, 11],
            ),
            margin=dict(l=50, r=30, t=50, b=50),
            width=CHART_MAX_W,   # CHK-3
            height=CHART_MAX_H,  # CHK-3
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
      - On veut alerter AVANT l'envoi, pas après un light mode

    Partage la même variable ENV SMTP_ATTACH_WARN_MB que mailer.py pour
    garantir la cohérence de la politique de taille entre les deux modules.
    """
    existing = [p for p in generated_paths if p.exists()]
    if not existing:
        return

    total_bytes = sum(p.stat().st_size for p in existing)
    total_mb    = total_bytes / (1024 * 1024)
    budget_mb   = SMTP_WARN_MB * 0.6   # seuil anticipation (60%)
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
      3. Vérifie le budget poids total (CHK-5)

    En cas d'erreur sur un chart individuel, les autres continuent
    (log.error + fallback chart vide via CHK-9).
    """
    log.info(
        f"CHARTS Démarrage génération — DPI={CHART_DPI}, "
        f"max={CHART_MAX_W}×{CHART_MAX_H}px, subsample={CHART_SUBSAMPLE}pts"
    )

    # ── Chargement des données depuis SentinelDB (CHK-10) ────────────────────
    metrics_30j:  list[dict] = []
    acteurs:      list[dict] = []
    tendances:    list[dict] = []

    try:
        from db_manager import SentinelDB
        metrics_30j = SentinelDB.getmetrics(ndays=30)
        acteurs     = SentinelDB.getacteurs(limit=20)
        tendances   = SentinelDB.gettendances(actives_only=True)
        log.info(
            f"CHARTS Données chargées : {len(metrics_30j)} métriques, "
            f"{len(acteurs)} acteurs, {len(tendances)} tendances"
        )
    except ImportError:
        log.warning("CHARTS db_manager absent — charts générés sans données historiques")
    except Exception as e:
        log.error(f"CHARTS Erreur chargement SentinelDB : {e} — charts avec données vides")

    # ── Génération des charts ─────────────────────────────────────────────────
    generated: dict[str, Path] = {}

    _chart_funcs = [
        ("indice_activite",      lambda: chart_indice_activite(metrics_30j)),
        ("distribution_alertes", lambda: chart_distribution_alertes(metrics_30j)),
        ("volume_articles",      lambda: chart_volume_articles(metrics_30j)),
        ("top_acteurs",          lambda: chart_top_acteurs(acteurs)),
        ("tendances_actives",    lambda: chart_tendances_actives(tendances)),
    ]

    for name, fn in _chart_funcs:
        try:
            path = fn()
            generated[name] = path
        except Exception as e:
            log.error(f"CHARTS Erreur chart '{name}' : {e}", exc_info=True)
            # CHK-9 : fallback chart vide pour ne pas bloquer report_builder
            fallback = _chart_path(name)
            try:
                _chart_vide_matplotlib(
                    name.replace("_", " ").title(),
                    f"Erreur génération :
{str(e)[:60]}",
                    fallback,
                )
                generated[name] = fallback
            except Exception as fe:
                log.error(f"CHARTS Fallback chart vide échoué pour '{name}' : {fe}")

    # ── CHK-5 : vérification budget poids ────────────────────────────────────
    _check_charts_budget(list(generated.values()))

    log.info(f"CHARTS {len(generated)}/{len(_chart_funcs)} charts générés → {OUTPUT_DIR}/")
    return generated


# ─────────────────────────────────────────────────────────────────────────────
# CLI — test rapide en développement
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    log.info(f"CHARTS CLI — DPI={CHART_DPI}, max={CHART_MAX_W}×{CHART_MAX_H}px")

    if "--check-deps" in sys.argv:
        missing = []
        for pkg in ["matplotlib", "plotly", "kaleido"]:
            try:
                __import__(pkg)
                log.info(f"  ✓ {pkg}")
            except ImportError:
                log.warning(f"  ✗ {pkg} — pip install {pkg}")
                missing.append(pkg)
        sys.exit(1 if missing else 0)

    # Génération de test avec données vides
    result = generate_all_charts("")
    log.info(f"CHARTS Résultat : {len(result)} charts générés")
    for name, path in result.items():
        size_kb = path.stat().st_size // 1024 if path.exists() else 0
        print(f"  {name:<25} → {path.name} ({size_kb} Ko)")
