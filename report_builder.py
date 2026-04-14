#!/usr/bin/env python3
# report_builder.py — SENTINEL v3.52
# =============================================================================
# Corrections v3.40–v3.44 (historique condensé) :
#   A13         Badge alerte VERT/ORANGE/ROUGE parsé dynamiquement
#   A14         ToC HTML avec ancres module-N
#   A23         alt unique par image (WCAG 2.1)
#   A24         Dark mode CSS natif prefers-color-scheme
#   B3          img_to_base64 avec try/except FileNotFoundError
#   B10         MIME text/html + application/pdf (non octet-stream)
#   F-5         OUTPUT_DIR via Path(__file__).parent — safe en cron
#   VIS-3       WeasyPrint PDF optionnel
#   VIS-6       Tables Markdown -> HTML
#   VISUAL-R1   metrics lues depuis SentinelDB
#   VISUAL-R3   @media print CSS complet
#   RB-41-FIX1  img_to_svg_or_b64 définie (NameError en production)
#   RB-41-FIX2  Signature build_html_report + compatibilité sentinel_main.py
#   RB-41-FIX3..RB-41-FIX11  Correctifs regex / SQLite Row / chart paths
#   RB-42-FIX1..3  regex re.M, Unicode invisible, orphans/widows print
#   RB-43-WARN1/2  build_light_mode_warning_part() MIMEText HTML + styles inline
#   RB-44-FIX1..9  Audit complet régression regex (\\1, \\s+, \\d+, etc.)
#
# CORRECTIONS v3.52 — Compatibilité charts.py v3.52 + améliorations :
#
# [RB-52-DICT]  CRITIQUE — build_html_report() traitait isinstance(chart_paths, dict)
#               comme une ERREUR (log warning + reset []). Or charts.py v3.52 retourne
#               MAINTENANT dict[str, Path] depuis generate_all_charts(). Ce guard
#               supprimait silencieusement TOUS les graphiques. Fix : chart_paths
#               accepte dict[str, Path] | list | None avec détection automatique
#               du format. Rétrocompatibilité list maintenue pour sentinel_main < v3.52.
#
# [RB-52-SECTION]  _build_charts_section() réécrite pour accepter dict[str, Path].
#               Layout nommé et sémantique (plus positionnel/fragile) :
#                 indice_activite       → pleine largeur (section principale)
#                 distribution_alertes    côte à côte
#                 volume_articles       /  (chart-grid 2 colonnes)
#                 top_acteurs           → pleine largeur
#                 tendances_actives     → pleine largeur
#                 brevets               → pleine largeur (sous-section "Veille Brevets")
#                 nlp_performance       → pleine largeur (sous-section "Performance NLP")
#               Mode list (ancien) : layout positionnel conservé inchangé.
#
# [RB-52-ALT]   _ALT_MAP mis à jour avec les 7 noms de charts de charts.py v3.52 :
#               indice_activite, distribution_alertes, volume_articles,
#               top_acteurs, tendances_actives, brevets, nlp_performance.
#
# [RB-52-BREVETS]  Nouvelle sous-section HTML "Veille Brevets" avec h3, description
#               contextuelle, et chart_brevets injecté si disponible.
#               Compatible avec report_builder sans modification de sentinel_main.
#
# [RB-52-NLP]   Nouvelle sous-section HTML "Performance NLP" avec h3 et
#               chart_nlp_performance injecté si disponible.
#
# [RB-52-METRICS]  _inject_metrics_banner() enrichi :
#               - Ajout nb_patents + top_patent_score si nb_patents > 0
#               - Ajout nb_reranked (NLP scorer) si disponible
#               - Format float corrigé pour terrestre/maritime/transverse/contractuel
#                 (utilisation de int() au lieu de :.0f sur des str potentiels)
#
# [RB-52-CSS]   HTML template — nouvelles classes :
#               .chart-section-brevets, .chart-section-nlp : sous-sections visuelles
#               .kpi-grid : layout flex responsive des KPIs
#               .kpi-item : item KPI individuel avec couleur accent
#               Amélioration mobile : padding réduit, charts empilés
#
# [RB-52-PURGE]  purge_old_reports() utilise maintenant le chemin absolu
#               de charts.py v3.52 (_ROOT / "output" / "charts") en plus
#               de OUTPUT_DIR local — évite les orphelins PNG.
#
# [RB-52-ENV]   load_dotenv() déplacé dans if __name__ == "__main__"
#               (cohérence avec charts.py v3.52 [CHT-52-ENV]).
#
# =============================================================================
# APPEL CORRECT depuis sentinel_main.py (inchangé depuis v3.44) :
#
#   chart_paths = _timed("charts", generate_all_charts, report_text)
#   # chart_paths est désormais dict[str, Path] — report_builder l'accepte
#   html_report = _timed("report_builder", build_html_report,
#                        report_text, chart_paths or {}, date_obj)
#
#   Rapport mensuel :
#   html_report = _timed("report_builder_monthly", build_html_report,
#                        report_text, {}, date_obj)
# =============================================================================

from __future__ import annotations

import base64
import datetime
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional, Union

log = logging.getLogger("sentinel.reportbuilder")

# ── Version ───────────────────────────────────────────────────────────────────
_VERSION = "3.52"

# ── Chemins absolus (F-5 : safe en cron) ─────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent
OUTPUT_DIR = _ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Chemin absolu charts (identique à charts.py v3.52 [CHT-52-PATH])
_CHARTS_DIR = _ROOT / "output" / "charts"

# ── Type alias ────────────────────────────────────────────────────────────────
ChartPaths = Union[dict[str, Path], list, None]


# =============================================================================
# HELPERS IMAGES
# =============================================================================

# [RB-52-ALT] ALT map mise à jour avec les 7 charts de charts.py v3.52
_ALT_MAP: dict[str, str] = {
    # Nouveaux noms charts.py v3.52
    "indice_activite":      "Courbe indice d'activité sectorielle — 30 derniers jours",
    "distribution_alertes": "Histogramme distribution niveaux d'alerte VERT/ORANGE/ROUGE",
    "volume_articles":      "Barres volume articles collectés et pertinents — 30 jours",
    "top_acteurs":          "Classement top 10 acteurs défense par score d'activité",
    "tendances_actives":    "Scatter tendances actives — persistance et chronologie",
    "brevets":              "Courbe métriques brevets — total, nouveaux, critiques — 30 jours",
    "nlp_performance":      "Courbe performance NLP scorer — articles traités et score moyen",
    # Anciens noms (rétrocompatibilité)
    "activite30j":          "Courbe activité sectorielle — robotique défense 30 derniers jours",
    "activite30j_plotly":   "Courbe activité sectorielle interactive — 30 derniers jours",
    "repartition_geo":      "Camembert répartition géographique des événements",
    "radar_techno":         "Radar technologique — intensité par domaine UGV USV UUV IA Essaims",
    "evolution_alertes":    "Histogramme évolution alertes ouvertes/closes sur 30 jours",
    "heatmap_acteurs":      "Heatmap co-occurrence acteurs × domaines défense",
    "contrats_montants":    "Graphique top contrats et financements détectés",
    "acteurs_geo":          "Carte choroplèthe activité mondiale robotique de défense par pays",
}


def chart_alt(path: Optional[Union[str, Path]]) -> str:
    """Alt text descriptif depuis le nom de fichier PNG (VIS-2 / A23)."""
    if not path:
        return "Graphique SENTINEL"
    if isinstance(path, str) and path.strip().startswith("<"):
        return "Graphique interactif SENTINEL"
    # Extraire le nom sans date (format YYYY-MM-DD_nom_chart.png)
    stem = Path(str(path)).stem
    # Supprimer le préfixe de date éventuel YYYY-MM-DD_
    clean = re.sub(r"^d{4}-d{2}-d{2}_", "", stem)
    return _ALT_MAP.get(clean, _ALT_MAP.get(stem, f"Graphique SENTINEL — {clean}"))


def img_to_base64(path: Union[str, Path]) -> str:
    """Encode un PNG en base64 string. Retourne '' si fichier absent (B3)."""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except (FileNotFoundError, IOError) as e:
        log.warning(f"BUILDER img_to_base64 : fichier absent {path} — {e}")
        return ""


def img_to_svg_or_b64(path: Union[str, Path]) -> tuple[str, str]:
    """
    RB-41-FIX1 : Retourne (contenu, format) :
      - SVG inline si le fichier est .svg
      - PNG base64 si le fichier est .png
      - ('', 'png') si le fichier est absent ou illisible
    """
    try:
        p = Path(str(path))
        if p.suffix.lower() == ".svg":
            return p.read_text(encoding="utf-8"), "svg"
        return img_to_base64(p), "png"
    except Exception as e:
        log.warning(f"BUILDER img_to_svg_or_b64 : erreur {path} — {e}")
        return "", "png"


# =============================================================================
# MARKDOWN -> HTML
# =============================================================================

def _md_table(m: re.Match) -> str:
    """
    Convertit un bloc tableau Markdown en <table> HTML (VIS-6).
    RB-44-FIX4 : [s|:=-] avec s pour whitespace (pas 's' littéral).
    """
    rows = [
        r.strip() for r in m.group(0).strip().split("
")
        if r.strip() and not re.match(r"^[s|:=-]+$", r.strip())
    ]
    if not rows:
        return ""
    html_rows: list[str] = []
    for k, row in enumerate(rows):
        cells = [c.strip() for c in row.strip("|").split("|")]
        tag   = "th" if k == 0 else "td"
        html_rows.append(
            "<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>"
        )
    return (
        '<table class="md-table">'
        f"<thead>{html_rows[0]}</thead>"
        f"<tbody>{''.join(html_rows[1:])}</tbody>"
        "</table>"
    )


def markdown_to_html(text: str) -> str:  # noqa: C901
    """
    Conversion Markdown basique → HTML.
    RB-44-FIX1 : \u0001 backreferences correctes
    RB-44-FIX2 : ** et * échappés pour gras/italique
    RB-44-FIX3 : | échappé pour tables Markdown
    RB-44-FIX5 : s+ et d+ dans les patterns numériques
    RB-42-FIX1 : re.M sur <ul> wrapping
    RB-42-FIX2 : nettoyage caractères invisibles Unicode
    """
    # RB-42-FIX2 : suppression des caractères invisibles
    text = (
        text
        .replace("​", "")    # zero-width space
        .replace("‌", "")    # zero-width non-joiner
        .replace("‍", "")    # zero-width joiner
        .replace(" ", " ")   # espace insécable → espace normale
        .replace("﻿", "")    # BOM UTF-8
    )

    # ── Titres avec ancres module-N ───────────────────────────────────────────
    text = re.sub(r"^# (.+)$",   r"<h1>\u0001</h1>",   text, flags=re.M)
    text = re.sub(r"^### (.+)$", r"<h3>\u0001</h3>",   text, flags=re.M)

    def _h2_with_anchor(m: re.Match) -> str:
        title = m.group(1)
        mod_m = re.match(r"MODULEs+(d+)", title, re.IGNORECASE)
        if mod_m:
            return f'<h2 id="module-{mod_m.group(1)}">{title}</h2>'
        slug = re.sub(r"s+", "-", title.lower()[:30])
        slug = re.sub(r"[^w-]", "", slug)
        return f'<h2 id="{slug}">{title}</h2>'

    text = re.sub(r"^## (.+)$", _h2_with_anchor, text, flags=re.M)

    # ── Gras / italique ───────────────────────────────────────────────────────
    text = re.sub(r"**(.+?)**", r"<strong>\u0001</strong>", text)
    text = re.sub(r"*(.+?)*",     r"<em>\u0001</em>",         text)

    # ── Tables Markdown → HTML ────────────────────────────────────────────────
    text = re.sub(r"(|.+|
?)+", _md_table, text, flags=re.M)

    # ── Listes à puces ────────────────────────────────────────────────────────
    text = re.sub(r"^[-*] (.+)$", r"<li>\u0001</li>", text, flags=re.M)
    text = re.sub(r"((?:<li>.*?</li>
?)+)", r"<ul>\u0001</ul>", text, flags=re.M)

    # ── Badge NON VÉRIFIÉ ─────────────────────────────────────────────────────
    text = re.sub(
        r"NONs+VÉRIFIÉ[^.
]*",
        '<span style="color:#C00000;font-weight:bold">&#9888; NON VERIFIÉ</span>',
        text,
    )

    # ── Sauts de paragraphe ───────────────────────────────────────────────────
    text = text.replace("

", "</p><p>")
    text = re.sub(r"(?<!</p>)
", "<br>", text)
    return f"<p>{text}</p>"


# =============================================================================
# TEMPLATE HTML
# str.replace() uniquement — V-2 safe sur JSON Claude avec accolades {}
# =============================================================================

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="dark light">
<meta name="generator" content="SENTINEL v%%VERSION%%">
<meta name="robots" content="noindex, nofollow">
<meta property="og:title" content="SENTINEL — Veille Robotique Défense">
<meta property="og:description" content="Rapport de veille automatisé — Robotique défense">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><path fill='%231F3864' d='M16 2L4 7v9c0 7.7 5.2 14.9 12 17z'/><text x='7' y='24' font-size='14' fill='white'>S</text></svg>">
<title>SENTINEL — %%DATE%%</title>
<style>
/* ── Reset & base ─────────────────────────────────────────────────────── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth;-webkit-font-smoothing:antialiased;
  scroll-padding-top:64px;hanging-punctuation:first last}
body{font-family:system-ui,'Segoe UI','Helvetica Neue',Arial,sans-serif;
  max-width:1160px;margin:0 auto;padding:28px 20px;
  color:#2d2d2d;background:#f7f6f2;font-size:15px;line-height:1.70}

/* ── Titres ───────────────────────────────────────────────────────────── */
h1{background:#1F3864;color:#fff;padding:16px 26px;border-radius:8px;
  font-size:20px;letter-spacing:.03em;margin-bottom:12px;line-height:1.3}
h2{color:#1F3864;border-bottom:3px solid #1F3864;padding-bottom:5px;
  margin-top:40px;margin-bottom:16px;font-size:17px;line-height:1.3}
h3{color:#2E75B6;margin-top:26px;margin-bottom:10px;font-size:15px;
  font-weight:600}
p{margin-bottom:10px;max-width:85ch}

/* ── Badge alerte ─────────────────────────────────────────────────────── */
.alerte-banner{padding:13px 22px;font-size:1.08em;font-weight:700;
  text-align:center;border-radius:7px;margin-bottom:20px;
  letter-spacing:.06em;color:#fff}
.alerte-banner.vert  {background:#2D6A2D}
.alerte-banner.orange{background:#9C6500}
.alerte-banner.rouge {background:#C00000}

/* ── KPI Banner ───────────────────────────────────────────────────────── */
.exec-summary{background:#D6E8F7;border-left:6px solid #1F3864;
  padding:14px 20px;margin:18px 0;border-radius:5px}
.kpi-grid{display:flex;gap:18px;flex-wrap:wrap;font-size:.88em;
  align-items:flex-start}
.kpi-item{display:flex;flex-direction:column;gap:2px;min-width:120px}
.kpi-label{color:#555;font-size:.8em;text-transform:uppercase;
  letter-spacing:.04em}
.kpi-value{font-weight:700;font-size:1.1em;color:#1F3864}
.kpi-value.vert  {color:#2D6A2D}
.kpi-value.orange{color:#9C6500}
.kpi-value.rouge {color:#C00000}

/* ── Navigation ToC ───────────────────────────────────────────────────── */
nav.toc{background:#f0f4f8;border:1px solid #c8d4e4;border-radius:7px;
  padding:11px 20px;margin:16px 0;font-size:.87em;line-height:2.1}
nav.toc a{color:#1F3864;text-decoration:none;margin-right:10px}
nav.toc a:hover{text-decoration:underline}

/* ── Tables Markdown ──────────────────────────────────────────────────── */
table.md-table{border-collapse:collapse;width:100%;margin:14px 0;
  font-size:.9em;table-layout:auto}
table.md-table th,table.md-table td{border:1px solid #ccc;
  padding:7px 10px;text-align:left;vertical-align:top}
table.md-table th{background:#1F3864;color:#fff;cursor:pointer;
  user-select:none;white-space:nowrap}
table.md-table th:hover{background:#2E5896}
table.md-table tr:nth-child(even) td{background:#f4f7fb}
table.md-table tr:hover td{background:#e8f0fa}

/* ── Charts PNG ───────────────────────────────────────────────────────── */
.chart-grid{display:flex;gap:18px;flex-wrap:wrap;margin:18px 0;
  align-items:flex-start}
.chart-grid img{max-width:calc(50% - 9px);flex:1 1 300px;
  border:1px solid #ddd;border-radius:5px;height:auto}
.chart-full{margin:16px 0}
.chart-full img{width:100%;border:1px solid #ddd;border-radius:5px;height:auto}

/* ── Charts Plotly HTML ───────────────────────────────────────────────── */
.chart-plotly{margin:16px 0;border:1px solid #ddd;border-radius:5px;
  overflow:hidden;background:#fff}
.chart-plotly-pair{display:flex;gap:18px;margin:18px 0;flex-wrap:wrap}
.chart-plotly-pair .chart-plotly{flex:1 1 300px}

/* ── [RB-52-CSS] Sous-sections spécialisées ──────────────────────────── */
.chart-section-brevets{background:#fdfaf2;border:1px solid #e8dfc0;
  border-radius:7px;padding:16px 20px;margin:20px 0}
.chart-section-brevets h3{color:#9C6500;margin-top:0;margin-bottom:10px}
.chart-section-nlp{background:#f2f7fd;border:1px solid #c0d8e8;
  border-radius:7px;padding:16px 20px;margin:20px 0}
.chart-section-nlp h3{color:#006494;margin-top:0;margin-bottom:10px}

/* ── Code ─────────────────────────────────────────────────────────────── */
pre{background:#f5f5f5;padding:12px;border-radius:5px;
  overflow-x:auto;font-size:12px;line-height:1.5}

/* ── Divers ───────────────────────────────────────────────────────────── */
a{color:#2E75B6}a:hover{color:#1F3864}
ul{padding-left:22px;margin:8px 0 10px}li{margin-bottom:4px}

/* ── Footer ───────────────────────────────────────────────────────────── */
.footer{text-align:center;color:#999;font-size:11px;margin-top:48px;
  border-top:1px solid #eee;padding-top:16px}

/* ── Responsive mobile ────────────────────────────────────────────────── */
@media(max-width:768px){
  body{padding:12px 10px;font-size:14px}
  .chart-grid img,.chart-plotly-pair .chart-plotly{
    max-width:100%!important;flex:1 1 100%}
  .kpi-grid{gap:12px}
  .kpi-item{min-width:100px}
  h1{font-size:15px;padding:12px 14px}
  nav.toc{font-size:.8em;line-height:1.9}
  .chart-section-brevets,.chart-section-nlp{padding:12px 14px}
}

/* ── Dark mode natif ──────────────────────────────────────────────────── */
@media(prefers-color-scheme:dark){
  body{background:#1a1a2e!important;color:#e0e0e0!important}
  h1{background:linear-gradient(135deg,#0f3460,#16213e)!important}
  h2{color:#7eb8f7!important;border-bottom-color:#0f3460!important}
  h3{color:#a8d1f7!important}
  p{color:#d4d4d4}
  .exec-summary{background:#1e3a5f!important;border-color:#0f3460!important;
    color:#d4d8e0!important}
  .kpi-label{color:#8899aa!important}
  .kpi-value{color:#7eb8f7!important}
  .kpi-value.vert  {color:#6daa45!important}
  .kpi-value.orange{color:#fdab43!important}
  .kpi-value.rouge {color:#dd6974!important}
  .alerte-banner{filter:brightness(.88)}
  nav.toc{background:#16213e!important;border-color:#0f3460!important}
  nav.toc a{color:#7eb8f7!important}
  table.md-table th{background:#0f3460!important}
  table.md-table td{border-color:#2a3a5a;color:#d4d4d4}
  table.md-table tr:nth-child(even) td{background:#1e2f4a!important}
  table.md-table tr:hover td{background:#263a5a!important}
  pre{background:#0d1b2e!important;color:#a8d1f7!important}
  .footer{color:#555!important;border-top-color:#2a3a5a!important}
  .chart-grid img,.chart-full img{border-color:#2a3a5a!important}
  .chart-plotly{background:#16213e!important;border-color:#2a3a5a!important}
  .chart-section-brevets{background:#1e1c10!important;border-color:#3a3420!important}
  .chart-section-brevets h3{color:#fdab43!important}
  .chart-section-nlp{background:#101c2a!important;border-color:#1e3a5a!important}
  .chart-section-nlp h3{color:#5591c7!important}
  a{color:#7eb8f7}
}

/* ── Print / PDF ──────────────────────────────────────────────────────── */
@media print{
  nav.toc,.no-print,.footer{display:none!important}
  body{font-size:11pt;color:#000;background:#fff;max-width:none;padding:0}
  h1{-webkit-print-color-adjust:exact;print-color-adjust:exact;
    background:#1F3864!important;color:#fff!important;font-size:14pt}
  h2{page-break-before:always;font-size:13pt;
    -webkit-print-color-adjust:exact;print-color-adjust:exact}
  h3{font-size:12pt}
  .alerte-banner{-webkit-print-color-adjust:exact;
    print-color-adjust:exact;margin-bottom:10pt}
  img{max-width:100%!important;page-break-inside:avoid}
  pre{white-space:pre-wrap;border:1px solid #ccc;font-size:9pt}
  table.md-table{page-break-inside:avoid;font-size:9pt}
  a[href]::after{content:" (" attr(href) ")";font-size:.75em;color:#555}
  .chart-plotly{display:none}
  p,li{orphans:3;widows:3}
  .chart-section-brevets,.chart-section-nlp{
    -webkit-print-color-adjust:exact;print-color-adjust:exact;
    border:1px solid #ccc!important;background:#fff!important}
}
</style>
</head>
<body>

%%ALERTE_BANNER%%

<h1>SENTINEL v%%VERSION%% &mdash; Rapport du %%DATE%%</h1>

%%TOC%%

%%CHARTS_SECTION%%

%%REPORT_BODY%%

<div class="footer">
  SENTINEL v%%VERSION%% &bull; Généré le %%TIMESTAMP%% &bull; Budget ~5 &euro;/mois
</div>

<script>
/* Tri des colonnes tables Markdown */
document.querySelectorAll("table.md-table th").forEach(function(h, col) {
  h.title = "Cliquer pour trier";
  h.addEventListener("click", function() {
    var tbl  = h.closest("table");
    var rows = Array.from(tbl.tBodies[0].rows);
    var asc  = tbl.dataset.sc == col && tbl.dataset.da === "1";
    tbl.dataset.sc = col; tbl.dataset.da = asc ? "0" : "1";
    rows.sort(function(a, b) {
      var ta = a.cells[col] ? a.cells[col].innerText : "";
      var tb = b.cells[col] ? b.cells[col].innerText : "";
      return asc ? tb.localeCompare(ta, "fr") : ta.localeCompare(tb, "fr");
    });
    rows.forEach(function(r) { tbl.tBodies[0].appendChild(r); });
  });
});

/* Scroll spy ToC (highlight ancre active) */
(function(){
  var links = document.querySelectorAll("nav.toc a");
  if (!links.length) return;
  var io = new IntersectionObserver(function(entries) {
    entries.forEach(function(e) {
      if (e.isIntersecting) {
        links.forEach(function(l){ l.style.fontWeight=""; });
        var active = document.querySelector("nav.toc a[href='#"+e.target.id+"']");
        if (active) active.style.fontWeight = "700";
      }
    });
  }, {threshold:0.4});
  document.querySelectorAll("h2[id],h3[id]").forEach(function(h){ io.observe(h); });
})();
</script>
</body>
</html>"""


# =============================================================================
# BADGE ALERTE (A13-FIX)
# =============================================================================

_ALERTE_ICONS  = {"VERT": "&#x1F7E2;", "ORANGE": "&#x1F7E0;", "ROUGE": "&#x1F534;"}
_ALERTE_LABELS = {
    "VERT":   "SURVEILLANCE NORMALE",
    "ORANGE": "ACTIVITÉ ÉLEVÉE",
    "ROUGE":  "ALERTE CRITIQUE",
}


def _parse_alerte_niveau(report_text: str) -> str:
    """
    Extrait le niveau d'alerte depuis le texte Claude.
    Retourne 'VERT', 'ORANGE' ou 'ROUGE'. Défaut : 'VERT'.
    RB-44-FIX5 : s* et d+ corrects (pas s* littéral).
    """
    m = re.search(
        r"(?:Alerte|ALERTE)s*[:-]?s*(VERT|ORANGE|ROUGE)",
        report_text,
        re.IGNORECASE,
    )
    return m.group(1).upper() if m else "VERT"


def _build_alerte_banner(report_text: str) -> str:
    niv   = _parse_alerte_niveau(report_text)
    cls   = niv.lower()
    icon  = _ALERTE_ICONS.get(niv, "&#x1F7E2;")
    label = _ALERTE_LABELS.get(niv, niv)
    return f'<div class="alerte-banner {cls}">{icon}&nbsp; NIVEAU ALERTE : {label}</div>'


# =============================================================================
# TABLE DES MATIÈRES (A14-FIX)
# =============================================================================

def _build_toc(report_text: str = "") -> str:
    """
    Construit la ToC depuis les ## MODULE N détectés dans report_text.
    RB-44-FIX5 : d+ et s+ corrects.
    """
    items: list[tuple[str, str]] = []
    if report_text:
        for m in re.finditer(
            r"^##s+((?:RÉSUMÉ|RESUME|MODULEs+d+)[^
]*)",
            report_text, re.MULTILINE | re.IGNORECASE
        ):
            title = m.group(1).strip()
            num_m = re.search(r"MODULEs+(d+)", title, re.IGNORECASE)
            if num_m:
                anchor = f"module-{num_m.group(1)}"
            else:
                anchor = "resume-executif"
            label = title[:48] + ("…" if len(title) > 48 else "")
            items.append((anchor, label))

    if not items:
        items = [
            ("resume-executif", "Résumé"),
            ("module-1", "1.Tableau"), ("module-2", "2.Faits"),
            ("module-3", "3.Terrestre"), ("module-4", "4.Maritime"),
            ("module-5", "5.IA"), ("module-6", "6.Acteurs"),
            ("module-7", "7.Contrats"), ("module-8", "8.Signaux"),
            ("module-9", "9.Sources"),
        ]
    links = " &middot; ".join(f'<a href="#{a}">{lbl}</a>' for a, lbl in items)
    return f'<nav class="toc no-print"><strong>Navigation :</strong> {links}</nav>'


# =============================================================================
# SECTION GRAPHIQUES — [RB-52-SECTION]
# =============================================================================

def _is_plotly_html(p: Union[str, Path]) -> bool:
    """True si p est un div HTML Plotly plutôt qu'un chemin PNG."""
    return isinstance(p, str) and p.strip().startswith("<")


def _render_chart(
    p: Union[str, Path, None],
    full: bool = True,
    extra_cls: str = "",
) -> str:
    """
    Rend un chart soit en <img> base64/SVG soit en div Plotly (HTML).
    Retourne chaîne vide si chart invalide ou image manquante.
    RB-41-FIX1 : utilise img_to_svg_or_b64.
    """
    if not p:
        return ""
    p_str = str(p)
    if _is_plotly_html(p_str):
        cls = f"chart-plotly {extra_cls}".strip()
        return f'<div class="{cls}">{p_str}</div>'

    content_data, fmt = img_to_svg_or_b64(p_str)
    if not content_data:
        log.debug(f"BUILDER chart absent ou vide : {p_str}")
        return ""

    alt      = chart_alt(p_str)
    wrap_cls = (f"chart-full {extra_cls}").strip() if full else extra_cls.strip()

    if fmt == "svg":
        return f'<div class="{wrap_cls}" aria-label="{alt}">{content_data}</div>'
    return (
        f'<div class="{wrap_cls}">'
        f'<img src="data:image/png;base64,{content_data}" alt="{alt}" loading="lazy">'
        f"</div>"
    )


def _build_charts_section(chart_paths: ChartPaths) -> str:  # noqa: C901
    """
    [RB-52-SECTION] Assemble la section graphiques.
    Accepte dict[str, Path] (charts.py v3.52) OU list (format legacy).

    Layout dict (nommé, sémantique) :
      indice_activite           → pleine largeur
      distribution_alertes  \
      volume_articles        /  → côte à côte (chart-grid 2 colonnes)
      top_acteurs               → pleine largeur
      tendances_actives         → pleine largeur
      brevets                   → sous-section .chart-section-brevets
      nlp_performance           → sous-section .chart-section-nlp

    Layout list (positionnel, backward compat) :
      [0] → pleine largeur
      [1] + [2] → côte à côte
      [3+] → pleine largeur chacun
    """
    if not chart_paths:
        return (
            '<div class="charts-empty" style="background:#f8f9fa;border:1px solid #dee2e6;'
            'border-radius:6px;padding:20px;margin:16px 0;text-align:center;color:#6c757d;">'
            "<p><strong>Graphiques non disponibles</strong></p>"
            "<p>Base de données insuffisante (premier lancement) ou erreur de génération.<br>"
            "Les graphiques apparaîtront à partir du 2e rapport.</p>"
            "</div>"
        )

    parts: list[str] = ['<h2 id="visualisations">Visualisations SENTINEL</h2>']

    # ── FORMAT DICT (charts.py v3.52) ─────────────────────────────────────────
    if isinstance(chart_paths, dict):
        cp = chart_paths  # alias

        # 1. Indice d'activité — pleine largeur
        if r := _render_chart(cp.get("indice_activite"), full=True):
            parts.append(r)

        # 2. Distribution alertes + Volume articles — côte à côte
        r_dist = _render_chart(cp.get("distribution_alertes"), full=False)
        r_vol  = _render_chart(cp.get("volume_articles"),      full=False)
        if r_dist or r_vol:
            pair_cls = "chart-plotly-pair" if (
                _is_plotly_html(str(cp.get("distribution_alertes", ""))) or
                _is_plotly_html(str(cp.get("volume_articles", "")))
            ) else "chart-grid"
            parts.append(f'<div class="{pair_cls}">{r_dist}{r_vol}</div>')

        # 3. Top acteurs — pleine largeur
        if r := _render_chart(cp.get("top_acteurs"), full=True):
            parts.append(r)

        # 4. Tendances actives — pleine largeur
        if r := _render_chart(cp.get("tendances_actives"), full=True):
            parts.append(r)

        # 5. [RB-52-BREVETS] Sous-section Veille Brevets
        r_brev = _render_chart(cp.get("brevets"), full=True)
        if r_brev:
            parts.append(
                '<div class="chart-section-brevets">'
                '<h3>&#x1F4DC;&nbsp; Veille Brevets — Métriques 30 jours</h3>'
                "<p>Suivi du flux de dépôts brevets détectés via EPO OPS et USPTO. "
                "La courbe <em>Total brevets</em> indique le volume de veille ; "
                "<em>Nouveaux</em> les dépôts récents ; "
                "<em>Critiques</em> (score ≥ 8.0) les priorités d'analyse.</p>"
                f"{r_brev}"
                "</div>"
            )

        # 6. [RB-52-NLP] Sous-section Performance NLP
        r_nlp = _render_chart(cp.get("nlp_performance"), full=True)
        if r_nlp:
            parts.append(
                '<div class="chart-section-nlp">'
                '<h3>&#x1F9E0;&nbsp; Performance NLP Scorer — 30 jours</h3>'
                "<p>Volume d'articles traités par le scorer TF-IDF / cosinus "
                "(barres) et score NLP moyen (courbe). Un score ≥ 0.70 indique "
                "une bonne précision du reranking.</p>"
                f"{r_nlp}"
                "</div>"
            )

        # Aucun chart rendu du tout
        if len(parts) == 1:  # uniquement le h2
            return (
                '<div class="charts-empty" style="background:#f8f9fa;border:1px solid #dee2e6;'
                'border-radius:6px;padding:20px;margin:16px 0;text-align:center;color:#6c757d;">'
                "<p><strong>Graphiques non disponibles</strong></p>"
                "<p>Fichiers PNG absents ou vides. Vérifier output/charts/.</p>"
                "</div>"
            )

    # ── FORMAT LIST (positionnel — backward compat) ───────────────────────────
    else:
        valid = [p for p in (chart_paths or []) if p]
        if not valid:
            return ""

        for idx, p in enumerate(valid):
            if idx == 0:
                parts.append(_render_chart(p, full=True))
            elif idx == 1:
                p2       = valid[2] if len(valid) > 2 else None
                pair_cls = "chart-plotly-pair" if (
                    _is_plotly_html(str(p)) or (p2 and _is_plotly_html(str(p2)))
                ) else "chart-grid"
                c1 = _render_chart(p,  full=False)
                c2 = _render_chart(p2, full=False) if p2 else ""
                parts.append(f'<div class="{pair_cls}">{c1}{c2}</div>')
            elif idx == 2:
                pass   # déjà traité dans idx == 1
            else:
                parts.append(_render_chart(p, full=True))

    return "
".join(s for s in parts if s)


# =============================================================================
# KPI BANNER DEPUIS SENTINELDB (VISUAL-R1 + [RB-52-METRICS])
# =============================================================================

def _inject_metrics_banner(report_text: str) -> str:
    """
    [RB-52-METRICS] Lit les métriques du jour depuis SentinelDB et génère
    une grille de KPIs HTML.

    Améliorations v3.52 :
    - Layout .kpi-grid / .kpi-item pour meilleure lisibilité
    - Couleur dynamique sur l'indice (vert/orange/rouge)
    - Ajout nb_patents + top_patent_score si nb_patents > 0
    - Ajout nb_reranked si disponible (NLP scorer)
    - int() pour les compteurs (évite :.0f sur des str potentiels)
    """
    try:
        from db_manager import SentinelDB
        rows = SentinelDB.getmetrics(ndays=1)
        if not rows:
            return ""

        raw = rows[0]
        m   = dict(raw) if not isinstance(raw, dict) else raw

        indice      = m.get("indice",            None)
        alerte      = (m.get("alerte",           "VERT") or "VERT").upper()
        nb_src      = m.get("nb_articles",       "-")
        nb_pert     = m.get("nb_pertinents",     "-")
        terrestre   = int(m.get("terrestre",     0) or 0)
        maritime    = int(m.get("maritime",      0) or 0)
        transverse  = int(m.get("transverse",    0) or 0)
        contractuel = int(m.get("contractuel",   0) or 0)
        nb_patents  = int(m.get("nb_patents",    0) or 0)
        top_pat_sc  = m.get("top_patent_score",  None)
        nb_reranked = m.get("nb_reranked",       None)

        # RB-44-FIX8 : formatage float propre
        indice_str = f"{indice:.1f}" if isinstance(indice, (int, float)) else (indice or "–")
        alerte_cls = alerte.lower()

        kpi_items = [
            f'<div class="kpi-item">'
            f'<span class="kpi-label">Indice activité</span>'
            f'<span class="kpi-value {alerte_cls}">{indice_str}/10</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Sources analysées</span>'
            f'<span class="kpi-value">{nb_src}</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Articles pertinents</span>'
            f'<span class="kpi-value">{nb_pert}</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Terrestre</span>'
            f'<span class="kpi-value">{terrestre}</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Maritime</span>'
            f'<span class="kpi-value">{maritime}</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Transverse</span>'
            f'<span class="kpi-value">{transverse}</span>'
            f'</div>',

            f'<div class="kpi-item">'
            f'<span class="kpi-label">Contrats</span>'
            f'<span class="kpi-value">{contractuel}</span>'
            f'</div>',
        ]

        # [RB-52-METRICS] KPIs brevets (optionnels)
        if nb_patents > 0:
            pat_score_str = (
                f" (top : {float(top_pat_sc):.1f})" if top_pat_sc else ""
            )
            kpi_items.append(
                f'<div class="kpi-item">'
                f'<span class="kpi-label">Brevets détectés</span>'
                f'<span class="kpi-value">{nb_patents}{pat_score_str}</span>'
                f'</div>'
            )

        # [RB-52-METRICS] KPI NLP scorer (optionnel)
        if nb_reranked is not None:
            kpi_items.append(
                f'<div class="kpi-item">'
                f'<span class="kpi-label">Reranked NLP</span>'
                f'<span class="kpi-value">{int(nb_reranked or 0)}</span>'
                f'</div>'
            )

        return (
            '<div class="exec-summary">'
            '<div class="kpi-grid">'
            + "".join(kpi_items)
            + "</div></div>"
        )

    except Exception as e:
        log.debug(f"BUILDER metrics_banner non disponible : {e}")
        return ""


# =============================================================================
# BANDEAU AVERTISSEMENT LIGHT MODE — RB-43-WARN1 / RB-43-WARN2
# =============================================================================

def build_light_mode_warning_part(
    stripped_types: list[str],
    limit_mb:       int  = 24,
    output_path:    str  = "output/",
) -> MIMEText:
    """
    Génère un MIMEText HTML d'avertissement pour le mode allégé mailer.py.
    RB-43-WARN1 : MIMEText HTML de dégradation.
    RB-43-WARN2 : styles inline isolés, aucune dépendance à _HTML_TEMPLATE.

    Usage :
        from report_builder import build_light_mode_warning_part
        light_msg.attach(build_light_mode_warning_part(["CSV", "PDF"], limit_mb))
    """
    _LABELS: dict[str, str] = {
        "CSV": "fichiers CSV analytiques (.csv)",
        "PDF": "rapport PDF complet (.pdf)",
    }
    stripped_labels = [_LABELS.get(t.upper(), t) for t in stripped_types]
    items_html      = "".join(f"<li>{lbl}</li>" for lbl in stripped_labels)

    has_pdf    = any(t.upper() == "PDF" for t in stripped_types)
    bg_color   = "#C00000" if has_pdf else "#9C6500"
    border_clr = "#8B0000" if has_pdf else "#7A5000"
    icon       = "&#x1F534;" if has_pdf else "&#x1F7E0;"

    html = f"""\
<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"></head>
<body style="font-family:system-ui,'Segoe UI',Arial,sans-serif;
             background:#f8f9fa;padding:0;margin:0;">
  <div style="background:{bg_color};border-left:6px solid {border_clr};
    border-radius:6px;padding:14px 20px;margin:0 0 16px 0;
    color:#ffffff;font-size:14px;line-height:1.6;">
    <p style="margin:0 0 8px 0;font-size:15px;font-weight:700;letter-spacing:.03em;">
      {icon}&nbsp; SENTINEL &mdash; Email allégé (limite SMTP {limit_mb} Mo dépassée)
    </p>
    <p style="margin:0 0 6px 0;">
      La taille de cet email dépassait la limite configurée.
      Les pièces jointes suivantes ont été <strong>automatiquement retirées</strong> :
    </p>
    <ul style="margin:4px 0 8px 24px;padding:0;">{items_html}</ul>
    <p style="margin:0;font-size:13px;opacity:.92;">
      Récupérez-les sur le serveur dans
      <code style="background:rgba(0,0,0,.25);padding:2px 6px;
        border-radius:3px;font-family:monospace;font-size:12px;">{output_path}</code>
      &mdash; tous les fichiers du cycle sont conservés 30 jours.
    </p>
    <p style="margin:6px 0 0 0;font-size:12px;opacity:.80;">
      Pour envoyer les pièces jointes complètes : augmentez
      <code style="background:rgba(0,0,0,.25);padding:1px 4px;border-radius:3px;">
      SMTP_ATTACH_LIMIT_MB</code> dans
      <code style="background:rgba(0,0,0,.25);padding:1px 4px;border-radius:3px;">.env</code>.
    </p>
  </div>
</body>
</html>"""

    return MIMEText(html, "html", "utf-8")


# =============================================================================
# BUILD HTML REPORT — FONCTION PRINCIPALE
# =============================================================================

def build_html_report(
    report_text:  str,
    chart_paths:  ChartPaths = None,
    date_obj:     Optional[datetime.date] = None,
    output_dir:   Optional[Path] = None,
) -> str:
    """
    Génère le rapport HTML complet, le sauvegarde, retourne son chemin absolu.

    RB-41-FIX2 : compatibilité sentinel_main.py.
    RB-44-FIX6 : date_obj=None avec fallback today().
    [RB-52-DICT] : chart_paths accepte dict[str, Path] (charts.py v3.52)
                   ET list (backward compat). Plus de guard erroné.

    APPEL CORRECT depuis sentinel_main.py :
        chart_paths = _timed("charts", generate_all_charts, report_text)
        html_report = _timed("report_builder", build_html_report,
                             report_text, chart_paths or {}, date_obj)
    """
    # RB-44-FIX6 : fallback date si non fourni
    if date_obj is None:
        date_obj = datetime.date.today()
        log.warning(
            "BUILDER build_html_report : date_obj manquant, fallback today(). "
            "Corriger l'appel dans sentinel_main.py (RB-44-FIX6)."
        )

    # [RB-52-DICT] chart_paths : dict (v3.52), list (legacy), ou None → acceptés tous
    # Plus de guard "dict = erreur" — charts.py v3.52 retourne CORRECTEMENT un dict
    if chart_paths is None:
        chart_paths = {}
    # Sanity check : si c'est un objet inattendu, on le vide proprement
    if not isinstance(chart_paths, (dict, list)):
        log.warning(
            f"BUILDER chart_paths type inattendu : {type(chart_paths).__name__} — "
            "attendu dict[str, Path] ou list. Remis à {}."
        )
        chart_paths = {}

    out_dir   = Path(output_dir) if output_dir else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    date_str  = date_obj.strftime("%d/%m/%Y")
    date_file = date_obj.strftime("%Y-%m-%d")
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    alerte_banner  = _build_alerte_banner(report_text)
    metrics_banner = _inject_metrics_banner(report_text)
    toc            = _build_toc(report_text)
    charts_section = _build_charts_section(chart_paths)
    report_body    = metrics_banner + markdown_to_html(report_text)

    html = (
        _HTML_TEMPLATE
        .replace("%%VERSION%%",        _VERSION)
        .replace("%%DATE%%",           date_str)
        .replace("%%ALERTE_BANNER%%",  alerte_banner)
        .replace("%%TOC%%",            toc)
        .replace("%%CHARTS_SECTION%%", charts_section)
        .replace("%%REPORT_BODY%%",    report_body)
        .replace("%%TIMESTAMP%%",      timestamp)
    )

    out_path = out_dir / f"SENTINEL_{date_file}_rapport.html"
    out_path.write_text(html, encoding="utf-8")
    log.info(f"BUILDER Rapport HTML → {out_path.resolve()}")

    # PDF WeasyPrint (VIS-3) — thread isolé ; on attend avant de retourner
    pdf_path = out_dir / f"SENTINEL_{date_file}_rapport.pdf"

    def _generate_pdf() -> None:
        try:
            from weasyprint import HTML as WH  # type: ignore
            WH(string=html, base_url=str(out_dir)).write_pdf(str(pdf_path))
            log.info(f"BUILDER Rapport PDF  → {pdf_path.resolve()}")
        except ImportError:
            log.debug("BUILDER WeasyPrint absent — PDF désactivé. pip install weasyprint")
        except Exception as e:
            log.warning(f"BUILDER WeasyPrint erreur — PDF non généré : {e}")

    with ThreadPoolExecutor(max_workers=1) as _pdf_pool:
        _pdf_pool.submit(_generate_pdf).result()

    return str(out_path)


# =============================================================================
# BUILD EMAIL MESSAGE (B10-FIX)
# =============================================================================

def build_email_message(
    date_str:   str,
    html_body:  str,
    from_addr:  str,
    to_addr:    str,
    output_dir: Optional[Path] = None,
) -> MIMEMultipart:
    """
    Construit le message MIME avec pièces jointes HTML + PDF.
    B10-FIX : MIME text/html pour HTML, application/pdf pour PDF.
    """
    msg = MIMEMultipart("mixed")
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg["Subject"] = f"SENTINEL — Rapport robotique défense du {date_str}"

    body = (
        f"Bonjour,

"
        f"Veuillez trouver ci-joint le rapport SENTINEL du {date_str}.
"
        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la défense.
"
        f"Le rapport HTML peut être ouvert directement dans un navigateur.
"
    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    out_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    try:
        date_file = datetime.datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        date_file = date_str

    for suffix, m_type, m_sub in [
        (".html", "text",        "html"),
        (".pdf",  "application", "pdf"),
    ]:
        att_path = out_dir / f"SENTINEL_{date_file}_rapport{suffix}"
        if att_path.exists():
            try:
                with open(att_path, "rb") as f:
                    part = MIMEBase(m_type, m_sub)
                    part.set_payload(f.read())
                encode_base64(part)
                part.add_header(
                    "Content-Disposition", "attachment",
                    filename=att_path.name,
                )
                msg.attach(part)
            except OSError as e:
                log.warning(f"BUILDER pièce jointe {att_path.name} ignorée : {e}")

    return msg


# =============================================================================
# PURGE ANCIENS RAPPORTS (R5-NEW-3 + [RB-52-PURGE])
# =============================================================================

def purge_old_reports(days: int = 30) -> int:
    """
    Supprime HTML, PDF et PNG charts de plus de N jours.
    [RB-52-PURGE] : purge aussi dans _CHARTS_DIR (chemin absolu charts.py v3.52)
    pour éviter les orphelins PNG qui s'accumulent en cas de rechargement.
    Retourne le nombre de fichiers supprimés.
    """
    cutoff  = time.time() - days * 86400
    removed = 0

    # HTML + PDF dans OUTPUT_DIR
    for pattern in ("*.html", "*.pdf"):
        for f in OUTPUT_DIR.glob(pattern):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
                    log.debug(f"PURGE supprimé : {f.name}")
            except OSError as e:
                log.debug(f"PURGE impossible {f.name} : {e}")

    # PNG dans _CHARTS_DIR (output/charts/)
    if _CHARTS_DIR.exists():
        for pattern in ("*.png", "*.json"):
            for f in _CHARTS_DIR.glob(pattern):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                        removed += 1
                        log.debug(f"PURGE supprimé chart : {f.name}")
                except OSError as e:
                    log.debug(f"PURGE impossible chart {f.name} : {e}")

    if removed:
        log.info(f"PURGE {removed} fichier(s) > {days}j supprimé(s)")
    return removed


# =============================================================================
# CLI — test rapide en développement
# =============================================================================

if __name__ == "__main__":
    import sys

    # [RB-52-ENV] load_dotenv() uniquement au lancement CLI direct
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

    log.info(f"BUILDER CLI — v{_VERSION}, output={OUTPUT_DIR}")

    # Test minimal avec texte factice
    _FAKE_REPORT = """\
## RÉSUMÉ EXÉCUTIF

**Alerte : ORANGE** — Activité élevée détectée dans le secteur drones terrestres.

## MODULE 1 — Tableau de Bord

| Indicateur | Valeur | Tendance |
|---|---|---|
| Indice activité | 6.8/10 | ↑ +1.2 |
| Articles analysés | 147 | = |
| Contrats détectés | 3 | ↑ |

## MODULE 2 — Faits Saillants

- General Atomics annonce un contrat UGV avec le DoD : **380 M$**
- BAE Systems dépose 4 brevets drone essaim en 7 jours
- *NON VÉRIFIÉ* : Anduril aurait levé 500 M$ supplémentaires

## MODULE 3 — Robotique Terrestre

Activité soutenue sur les UGV de reconnaissance. Les programmes Ghost Robotics
et Spot continuent leur déploiement en Ukraine orientale.
"""

    # Test sans charts (dict vide)
    out = build_html_report(
        report_text = _FAKE_REPORT,
        chart_paths = {},
        date_obj    = datetime.date.today(),
    )
    log.info(f"BUILDER Rapport test généré : {out}")

    # Afficher taille
    p = Path(out)
    if p.exists():
        print(f"
✅ HTML généré : {p.name} ({p.stat().st_size // 1024} Ko)")
        print(f"   Chemin : {p.resolve()}")
    else:
        print("❌ Erreur : fichier non créé")
        sys.exit(1)

    sys.exit(0)

