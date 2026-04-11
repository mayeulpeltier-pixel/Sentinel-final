#!/usr/bin/env python3
# report_builder.py — SENTINEL v3.44
# =============================================================================
# Corrections v3.40 (originales) :
#   A13         Badge alerte VERT/ORANGE/ROUGE parsé dynamiquement
#   A14         ToC HTML avec ancres module-N
#   A23         alt unique par image (WCAG 2.1)
#   A24         Dark mode CSS natif prefers-color-scheme
#   B3          img_to_base64 avec try/except FileNotFoundError
#   B10         MIME text/html + application/pdf (non octet-stream)
#   F-5         OUTPUT_DIR via Path(__file__).parent — safe en cron
#   R5A3-NEW-2  outpath NameError corrigé
#   R5A3-NEW-3  <meta name="color-scheme"> dark light
#   R5A3-NEW-5  ul wrapping dans markdown_to_html
#   V-2         str.replace() au lieu de .format()
#   VIS-3       WeasyPrint PDF optionnel
#   VIS-6       Tables Markdown -> HTML
#   VIS-7       meta generator
#   VISUAL-R1   metrics lues depuis SentinelDB
#   VISUAL-R3   @media print CSS complet
#   R5-NEW-3    purge_old_reports()
#
# Corrections v3.41 :
#   RB-41-FIX1  img_to_svg_or_b64 definie (NameError en production)
#   RB-41-FIX2  Signature build_html_report + compatibilite sentinel_main.py
#   RB-41-FIX3  markdown_to_html : backreferences groupes regex
#   RB-41-FIX4  markdown_to_html : ** et * echappes
#   RB-41-FIX5  markdown_to_html : <ul>groupe</ul>
#   RB-41-FIX6  markdown_to_html : pipe echape pour tables
#   RB-41-FIX7  _build_toc : d+ au lieu de d+
#   RB-41-FIX8  _md_table : [s|:=-] au lieu de [s|:=-]
#   RB-41-FIX9  _build_charts_section : double "if not chart_paths" supprime
#   RB-41-FIX10 _build_toc : import re local supprime
#   RB-41-FIX11 _inject_metrics_banner : dict(raw) pour SQLite Row
#
# Corrections v3.42 :
#   RB-42-FIX1  markdown_to_html : regex <li> re.M
#   RB-42-FIX2  markdown_to_html : nettoyage caracteres invisibles Unicode
#   RB-42-FIX3  CSS @media print : orphans/widows sur p et li
#
# Corrections v3.43 :
#   RB-43-WARN1  build_light_mode_warning_part() — MIMEText HTML dégradation mailer
#   RB-43-WARN2  styles inline isolés
#
# CORRECTIONS v3.44 — Audit complet régression regex :
#   RB-44-FIX1  \u0001 -> \u0001 PARTOUT dans re.sub()
#   RB-44-FIX2  r"**(.+?)**" et r"*(.+?)*" (astérisques échappés)
#   RB-44-FIX3  r"(|.+|
?)+" (pipe échappé pour tables Markdown)
#   RB-44-FIX4  r"^[s|:=-]+$" dans _md_table (s pas s)
#   RB-44-FIX5  d+ et s+ partout (_build_toc, _h2_with_anchor, _parse_alerte_niveau)
#   RB-44-FIX6  build_html_report : date_obj=None avec fallback today()
#   RB-44-FIX7  build_html_report : guard isinstance(dict) pour chart_paths
#   RB-44-FIX8  indice formate en :.1f dans _inject_metrics_banner
#   RB-44-FIX9  _VERSION constante unique injectee via %%VERSION%%
#
# =============================================================================
# CHANGEMENTS REQUIS DANS sentinel_main.py :
#
#   AVANT :
#       _timed("charts", generate_all_charts, report_text)   # retour ignore !
#       html_report = _timed("report_builder", build_html_report, report_text, metrics)
#
#   APRES :
#       chart_paths = _timed("charts", generate_all_charts, report_text)
#       html_report = _timed("report_builder", build_html_report,
#                            report_text, chart_paths or [], date_obj)
#
#   RAPPORT MENSUEL — AVANT :
#       html_report = _timed("report_builder_monthly", build_html_report,
#                            report_text, metrics)
#   APRES :
#       html_report = _timed("report_builder_monthly", build_html_report,
#                            report_text, [], date_obj)
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
from typing import Optional

log = logging.getLogger("sentinel.reportbuilder")

# ── Version unique (RB-44-FIX9) ──────────────────────────────────────────────
_VERSION = "3.44"

# ── Chemins (F-5 : absolu safe en cron) ──────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# HELPERS IMAGES
# =============================================================================

_ALT_MAP: dict[str, str] = {
    "activite30j":        "Courbe activite sectorielle - robotique defense 30 derniers jours",
    "activite30j_plotly": "Courbe activite sectorielle interactive - 30 derniers jours",
    "repartition_geo":    "Camembert repartition geographique des evenements",
    "radar_techno":       "Radar technologique - intensite par domaine UGV USV UUV IA Essaims",
    "top_acteurs":        "Histogramme top acteurs industrie defense - frequence de mention",
    "evolution_alertes":  "Histogramme evolution alertes ouvertes / closes sur 30 jours",
    "heatmap_acteurs":    "Heatmap co-occurrence acteurs x domaines defense",
    "contrats_montants":  "Graphique top contrats et financements detectes",
    "acteurs_geo":        "Carte choroplethe activite mondiale robotique de defense par pays",
}


def chart_alt(path: Optional[str]) -> str:
    """Alt text descriptif depuis le nom de fichier PNG (VIS-2 / A23)."""
    if not path:
        return "Graphique SENTINEL"
    if isinstance(path, str) and path.strip().startswith("<"):
        return "Graphique interactif SENTINEL"
    name = Path(path).stem
    return _ALT_MAP.get(name, f"Graphique SENTINEL - {name}")


def img_to_base64(path: str) -> str:
    """Encode un PNG en base64 string. Retourne '' si fichier absent (B3)."""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except (FileNotFoundError, IOError) as e:
        log.warning(f"BUILDER img_to_base64 : fichier absent {path} - {e}")
        return ""


def img_to_svg_or_b64(path: str) -> tuple[str, str]:
    """
    RB-41-FIX1 : fonction manquante en v3.40.
    Retourne (contenu, format) :
      - SVG inline si le fichier est .svg
      - PNG base64 si le fichier est .png
      - ('', 'png') si le fichier est absent ou illisible
    """
    try:
        p = Path(path)
        if p.suffix.lower() == ".svg":
            return p.read_text(encoding="utf-8"), "svg"
        return img_to_base64(path), "png"
    except Exception as e:
        log.warning(f"BUILDER img_to_svg_or_b64 : erreur {path} - {e}")
        return "", "png"


# =============================================================================
# MARKDOWN -> HTML
# =============================================================================

def _md_table(m: re.Match) -> str:
    """
    Convertit un bloc tableau Markdown en <table> HTML (VIS-6).
    RB-44-FIX4 : [\\s|:=-] avec \\s pour whitespace (pas 's' litteral).
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
    Conversion Markdown basique -> HTML.
    RB-44-FIX1 : \\1 (backreferences correctes)
    RB-44-FIX2 : \\*\\* et \\* echappes pour gras/italique
    RB-44-FIX3 : \\|.+\\| echape pour tables Markdown
    RB-44-FIX5 : \\s+ et \\d+ dans les patterns numeriques
    RB-42-FIX1 : re.M sur <ul> wrapping
    RB-42-FIX2 : nettoyage caracteres invisibles Unicode en entree
    """
    # RB-42-FIX2 : suppression des caracteres invisibles
    text = (
        text
        .replace("​", "")   # zero-width space
        .replace("‌", "")   # zero-width non-joiner
        .replace("‍", "")   # zero-width joiner
        .replace(" ", " ")  # espace insecable -> espace normale
        .replace("﻿", "")   # BOM UTF-8
    )

    # ── Titres avec ancres module-N ───────────────────────────────────────
    # RB-44-FIX1 : \u0001 au lieu de \u0001
    text = re.sub(r"^# (.+)$",   r"<h1>\u0001</h1>",   text, flags=re.M)
    text = re.sub(r"^### (.+)$", r"<h3>\u0001</h3>",   text, flags=re.M)

    def _h2_with_anchor(m: re.Match) -> str:
        title = m.group(1)
        # RB-44-FIX5 : s+ et d+
        mod_m = re.match(r"MODULEs+(d+)", title, re.IGNORECASE)
        if mod_m:
            return f'<h2 id="module-{mod_m.group(1)}">{title}</h2>'
        return f"<h2>{title}</h2>"

    text = re.sub(r"^## (.+)$", _h2_with_anchor, text, flags=re.M)

    # ── Gras / italique ───────────────────────────────────────────────────
    # RB-44-FIX1 : \u0001  |  RB-44-FIX2 : ** et * echappes
    text = re.sub(r"**(.+?)**", r"<strong>\u0001</strong>", text)
    text = re.sub(r"*(.+?)*",     r"<em>\u0001</em>",         text)

    # ── Tables Markdown -> HTML ───────────────────────────────────────────
    # RB-44-FIX3 : | echape
    text = re.sub(r"(|.+|
?)+", _md_table, text, flags=re.M)

    # ── Listes a puces ────────────────────────────────────────────────────
    # RB-44-FIX1 : \u0001  |  RB-42-FIX1 : re.M
    text = re.sub(r"^[-*] (.+)$", r"<li>\u0001</li>", text, flags=re.M)
    text = re.sub(r"((?:<li>.*?</li>
?)+)", r"<ul>\u0001</ul>", text, flags=re.M)

    # ── Badge NON VERIFIE ─────────────────────────────────────────────────
    text = re.sub(
        r"NON VÉRIFIÉ[^.
]*",
        '<span style="color:#C00000;font-weight:bold">&#9888; NON VERIFIE</span>',
        text,
    )

    # ── Sauts de paragraphe ───────────────────────────────────────────────
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
<meta property="og:title" content="SENTINEL - Veille Robotique Defense">
<meta property="og:description" content="Rapport de veille automatise - Robotique defense">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><path fill='%231F3864' d='M16 2L4 7v9c0 7.7 5.2 14.9 12 17z'/><text x='7' y='24' font-size='14' fill='white'>S</text></svg>">
<title>SENTINEL - %%DATE%%</title>
<style>
/* Base */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth;-webkit-font-smoothing:antialiased;scroll-padding-top:64px}
body{font-family:system-ui,'Segoe UI',Arial,sans-serif;max-width:1140px;
  margin:0 auto;padding:28px 20px;color:#2d2d2d;background:#f8f9fa;
  font-size:15px;line-height:1.68}
/* Titres */
h1{background:#1F3864;color:#fff;padding:16px 26px;border-radius:8px;
  font-size:20px;letter-spacing:.03em;margin-bottom:10px}
h2{color:#1F3864;border-bottom:3px solid #1F3864;padding-bottom:5px;
  margin-top:38px;margin-bottom:14px;font-size:17px}
h3{color:#2E75B6;margin-top:24px;margin-bottom:8px;font-size:15px}
p{margin-bottom:10px;max-width:85ch}
/* Badge alerte */
.alerte-banner{padding:13px 22px;font-size:1.08em;font-weight:700;
  text-align:center;border-radius:7px;margin-bottom:20px;
  letter-spacing:.06em;color:#fff}
.alerte-banner.vert  {background:#2D6A2D}
.alerte-banner.orange{background:#9C6500}
.alerte-banner.rouge {background:#C00000}
/* Resume executif / KPI */
.exec-summary{background:#D6E8F7;border-left:6px solid #1F3864;
  padding:14px 20px;margin:18px 0;border-radius:5px}
/* Navigation ToC */
nav.toc{background:#f0f4f8;border:1px solid #c8d4e4;border-radius:7px;
  padding:11px 20px;margin:16px 0;font-size:.87em;line-height:2.1}
nav.toc a{color:#1F3864;text-decoration:none;margin-right:10px}
nav.toc a:hover{text-decoration:underline}
/* Tables Markdown */
table.md-table{border-collapse:collapse;width:100%;margin:14px 0;
  font-size:.9em;table-layout:auto}
table.md-table th,table.md-table td{border:1px solid #ccc;
  padding:7px 10px;text-align:left;vertical-align:top}
table.md-table th{background:#1F3864;color:#fff;cursor:pointer;
  user-select:none;white-space:nowrap}
table.md-table th:hover{background:#2E5896}
table.md-table tr:nth-child(even) td{background:#f4f7fb}
table.md-table tr:hover td{background:#e8f0fa}
/* Charts PNG */
.chart-grid{display:flex;gap:18px;flex-wrap:wrap;margin:18px 0;
  align-items:flex-start}
.chart-grid img{max-width:calc(50% - 9px);flex:1 1 300px;
  border:1px solid #ddd;border-radius:5px}
.chart-full{margin:16px 0}
.chart-full img{width:100%;border:1px solid #ddd;border-radius:5px}
/* Charts Plotly HTML */
.chart-plotly{margin:16px 0;border:1px solid #ddd;border-radius:5px;
  overflow:hidden;background:#fff}
.chart-plotly-pair{display:flex;gap:18px;margin:18px 0;flex-wrap:wrap}
.chart-plotly-pair .chart-plotly{flex:1 1 300px}
/* Code */
pre{background:#f5f5f5;padding:12px;border-radius:5px;
  overflow-x:auto;font-size:12px}
/* Divers */
a{color:#2E75B6}a:hover{color:#1F3864}
ul{padding-left:22px;margin:8px 0 10px}li{margin-bottom:4px}
/* Footer */
.footer{text-align:center;color:#999;font-size:11px;margin-top:48px;
  border-top:1px solid #eee;padding-top:16px}
/* Responsive mobile */
@media(max-width:768px){
  body{padding:12px 10px;font-size:14px}
  .chart-grid img,.chart-plotly-pair .chart-plotly{
    max-width:100%!important;flex:1 1 100%}
  h1{font-size:15px;padding:12px 14px}
  nav.toc{font-size:.8em;line-height:1.9}
}
/* Dark mode natif (A24-FIX) */
@media(prefers-color-scheme:dark){
  body{background:#1a1a2e!important;color:#e0e0e0!important}
  h1{background:linear-gradient(135deg,#0f3460,#16213e)!important}
  h2{color:#7eb8f7!important;border-bottom-color:#0f3460!important}
  h3{color:#a8d1f7!important}
  p{color:#d4d4d4}
  .exec-summary{background:#1e3a5f!important;border-color:#0f3460!important;
    color:#d4d8e0!important}
  .alerte-banner{filter:brightness(.92)}
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
  a{color:#7eb8f7}
}
/* Print / PDF (VISUAL-R3 + RB-42-FIX3) */
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
  SENTINEL v%%VERSION%% &bull; Genere le %%TIMESTAMP%% &bull; Budget ~5 &euro;/mois
</div>

<script>
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
      return asc ? tb.localeCompare(ta) : ta.localeCompare(tb);
    });
    rows.forEach(function(r) { tbl.tBodies[0].appendChild(r); });
  });
});
</script>
</body>
</html>"""


# =============================================================================
# BADGE ALERTE (A13-FIX)
# =============================================================================

_ALERTE_ICONS  = {"VERT": "&#x1F7E2;", "ORANGE": "&#x1F7E0;", "ROUGE": "&#x1F534;"}
_ALERTE_LABELS = {
    "VERT":   "SURVEILLANCE NORMALE",
    "ORANGE": "ACTIVITE ELEVEE",
    "ROUGE":  "ALERTE CRITIQUE",
}


def _parse_alerte_niveau(report_text: str) -> str:
    """
    Extrait le niveau d'alerte depuis le texte Claude.
    Retourne 'VERT', 'ORANGE' ou 'ROUGE'. Défaut : 'VERT'.
    RB-44-FIX5 : \\s* corrects (pas s* litteral).
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
# TABLE DES MATIERES (A14-FIX)
# =============================================================================

def _build_toc(report_text: str = "") -> str:
    """
    Construit la TOC depuis les ## MODULE N détectés dans report_text.
    RB-41-FIX10 : import re local supprimé.
    RB-44-FIX5  : \\d+ et \\s+ corrects.
    """
    items = []
    if report_text:
        for m in re.finditer(
            r"^##s+((?:RÉSUMÉ|MODULEs+d+)[^
]*)",
            report_text, re.MULTILINE | re.IGNORECASE
        ):
            title = m.group(1).strip()
            num_m = re.search(r"MODULEs+(d+)", title, re.IGNORECASE)
            if num_m:
                anchor = f"module-{num_m.group(1)}"
            else:
                anchor = "resume-executif"
            label = title[:45] + ("..." if len(title) > 45 else "")
            items.append((anchor, label))

    if not items:
        items = [
            ("resume-executif", "Resume"),
            ("module-1", "1.Tableau"), ("module-2", "2.Faits"),
            ("module-3", "3.Terrestre"), ("module-4", "4.Maritime"),
            ("module-5", "5.IA"), ("module-6", "6.Acteurs"),
            ("module-7", "7.Contrats"), ("module-8", "8.Signaux"),
            ("module-9", "9.Sources"),
        ]
    links = " &middot; ".join(f'<a href="#{a}">{l}</a>' for a, l in items)
    return f'<nav class="toc no-print"><strong>Navigation :</strong> {links}</nav>'


# =============================================================================
# SECTION GRAPHIQUES
# =============================================================================

def _is_plotly_html(p: str) -> bool:
    """True si p est un div HTML Plotly plutôt qu'un chemin PNG."""
    return isinstance(p, str) and p.strip().startswith("<")


def _render_chart(p: str, full: bool = True, extra_cls: str = "") -> str:
    """
    Rend un chart soit en <img> base64/SVG soit en div Plotly (HTML).
    Retourne chaine vide si chart invalide ou image manquante.
    RB-41-FIX1 : utilise img_to_svg_or_b64.
    """
    if not p:
        return ""
    if _is_plotly_html(p):
        cls = f"chart-plotly {extra_cls}".strip()
        return f'<div class="{cls}">{p}</div>'

    content_data, fmt = img_to_svg_or_b64(p)
    if not content_data:
        return ""

    alt      = chart_alt(p)
    wrap_cls = ("chart-full " + extra_cls).strip() if full else extra_cls.strip()

    if fmt == "svg":
        return f'<div class="{wrap_cls}" aria-label="{alt}">{content_data}</div>'
    return (
        f'<div class="{wrap_cls}">'
        f'<img src="data:image/png;base64,{content_data}" alt="{alt}" loading="lazy">'
        f"</div>"
    )


def _build_charts_section(chart_paths: list) -> str:  # noqa: C901
    """
    Assemble la section visuelle.
    Disposition :
      [0] activite 30j        -> pleine largeur
      [1] repartition geo  |
      [2] radar techno     |  -> cote a cote
      [3] top acteurs         -> pleine largeur
      [4+]                    -> pleine largeur chacun
    RB-41-FIX9 : double "if not chart_paths" supprime.
    """
    if not chart_paths:
        return (
            '<div class="charts-empty" style="background:#f8f9fa;border:1px solid #dee2e6;'
            'border-radius:6px;padding:20px;margin:16px 0;text-align:center;color:#6c757d;">'
            '<p><strong>Graphiques non disponibles</strong></p>'
            '<p>Base de donnees insuffisante (premier lancement) ou erreur de generation.<br>'
            'Les graphiques apparaitront a partir du 2e rapport.</p>'
            '</div>'
        )

    valid = [p for p in chart_paths if p]
    if not valid:
        return ""

    parts: list[str] = ["<h2>Visualisations SENTINEL</h2>"]

    for idx, p in enumerate(valid):
        if idx == 0:
            parts.append(_render_chart(p, full=True))
        elif idx == 1:
            p2       = valid[2] if len(valid) > 2 else None
            pair_cls = "chart-plotly-pair" if (
                _is_plotly_html(p) or (p2 and _is_plotly_html(p2))
            ) else "chart-grid"
            c1 = _render_chart(p,  full=False)
            c2 = _render_chart(p2, full=False) if p2 else ""
            parts.append(f'<div class="{pair_cls}">{c1}{c2}</div>')
        elif idx == 2:
            pass  # deja traite dans idx == 1
        else:
            parts.append(_render_chart(p, full=True))

    return "
".join(s for s in parts if s)


# =============================================================================
# KPI BANNER DEPUIS SENTINELDB (VISUAL-R1)
# =============================================================================

def _inject_metrics_banner(report_text: str) -> str:
    """
    Lit les metriques du jour depuis SentinelDB et genere une ligne de KPI HTML.
    VISUAL-R1   : remplace l'extraction regex sur le texte du rapport.
    RB-41-FIX11 : dict(raw) pour SQLite Row -> .get() garanti.
    RB-44-FIX8  : indice formate en :.1f.
    """
    try:
        from db_manager import SentinelDB
        rows = SentinelDB.getmetrics(ndays=1)
        if not rows:
            return ""

        raw = rows[0]
        m   = dict(raw) if not isinstance(raw, dict) else raw

        indice      = m.get("indice",        None)
        alerte      = (m.get("alerte",       "VERT") or "VERT").upper()
        nb_src      = m.get("nb_articles",   "-")
        nb_pert     = m.get("nb_pertinents", "-")
        terrestre   = m.get("terrestre",     0) or 0
        maritime    = m.get("maritime",      0) or 0
        transverse  = m.get("transverse",    0) or 0
        contractuel = m.get("contractuel",   0) or 0

        # RB-44-FIX8 : formatage float propre
        indice_str = f"{indice:.1f}" if isinstance(indice, (int, float)) else (indice or "-")

        color_map = {"VERT": "#2D6A2D", "ORANGE": "#9C6500", "ROUGE": "#C00000"}
        color     = color_map.get(alerte, "#2D6A2D")

        return (
            '<div class="exec-summary" style="display:flex;gap:24px;'
            'flex-wrap:wrap;font-size:.9em;">'
            f'<span><strong>Indice activite :</strong> '
            f'<span style="color:{color};font-weight:700">{indice_str}/10</span></span>'
            f'<span><strong>Sources :</strong> {nb_src} analysees / {nb_pert} pertinentes</span>'
            f'<span><strong>Terrestre</strong> {terrestre:.0f} &bull; '
            f'<strong>Maritime</strong> {maritime:.0f} &bull; '
            f'<strong>Transverse</strong> {transverse:.0f} &bull; '
            f'<strong>Contrats</strong> {contractuel:.0f}</span>'
            "</div>"
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
    Genere un MIMEText HTML d'avertissement pour le mode allege mailer.py.
    RB-43-WARN1 : MIMEText HTML de degradation.
    RB-43-WARN2 : styles inline isoles, aucune dependance a _HTML_TEMPLATE.

    Usage :
        from report_builder import build_light_mode_warning_part
        light_msg.attach(build_light_mode_warning_part(["CSV"], limit_mb))
        light_msg.attach(build_light_mode_warning_part(["PDF"], limit_mb))
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
      {icon}&nbsp; SENTINEL &mdash; Email allege (limite SMTP {limit_mb} Mo depassee)
    </p>
    <p style="margin:0 0 6px 0;">
      La taille de cet email depassait la limite configuree.
      Les pieces jointes suivantes ont ete <strong>automatiquement retirees</strong> :
    </p>
    <ul style="margin:4px 0 8px 24px;padding:0;">{items_html}</ul>
    <p style="margin:0;font-size:13px;opacity:.92;">
      Recuperez-les sur le serveur dans
      <code style="background:rgba(0,0,0,.25);padding:2px 6px;
        border-radius:3px;font-family:monospace;font-size:12px;">{output_path}</code>
      &mdash; tous les fichiers du cycle sont conserves 30 jours.
    </p>
    <p style="margin:6px 0 0 0;font-size:12px;opacity:.80;">
      Pour envoyer les pieces jointes completes : augmentez
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
    chart_paths:  list | None = None,
    date_obj:     Optional[datetime.date] = None,
    output_dir:   Optional[Path] = None,
) -> str:
    """
    Genere le rapport HTML complet, le sauvegarde, retourne son chemin absolu.

    RB-41-FIX2 : compatibilite sentinel_main.py.
    RB-44-FIX6 : date_obj=None avec fallback today().
    RB-44-FIX7 : guard isinstance(dict) pour chart_paths.

    APPEL CORRECT depuis sentinel_main.py :
        chart_paths = _timed("charts", generate_all_charts, report_text)
        html_report = _timed("report_builder", build_html_report,
                             report_text, chart_paths or [], date_obj)
    """
    # RB-44-FIX6 : fallback date si non fourni
    if date_obj is None:
        date_obj = datetime.date.today()
        log.warning(
            "BUILDER build_html_report : date_obj manquant, fallback today(). "
            "Corriger l'appel dans sentinel_main.py (RB-44-FIX6)."
        )

    # RB-44-FIX7 : si metrics (dict) passe par erreur a la place de chart_paths (list)
    if isinstance(chart_paths, dict):
        log.warning(
            "BUILDER build_html_report : chart_paths est un dict (probablement "
            "'metrics' passe par erreur). Ignore -> aucun graphique. "
            "Corriger sentinel_main.py (RB-44-FIX7)."
        )
        chart_paths = []

    safe_chart_paths: list = chart_paths or []

    out_dir   = Path(output_dir) if output_dir else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    date_str  = date_obj.strftime("%d/%m/%Y")
    date_file = date_obj.strftime("%Y-%m-%d")
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    alerte_banner  = _build_alerte_banner(report_text)
    metrics_banner = _inject_metrics_banner(report_text)
    toc            = _build_toc(report_text)
    charts_section = _build_charts_section(safe_chart_paths)
    report_body    = metrics_banner + markdown_to_html(report_text)

    # RB-44-FIX9 : _VERSION injectee partout via %%VERSION%%
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
    log.info(f"BUILDER Rapport HTML -> {out_path.resolve()}")

    # PDF WeasyPrint (VIS-3) — thread isole pour ne pas bloquer le pipeline
    # On attend la fin avant de retourner pour que mailer.py trouve le PDF.
    pdf_path = out_dir / f"SENTINEL_{date_file}_rapport.pdf"

    def _generate_pdf() -> None:
        try:
            from weasyprint import HTML as WH  # type: ignore
            WH(string=html).write_pdf(str(pdf_path))
            log.info(f"BUILDER Rapport PDF  -> {pdf_path.resolve()}")
        except ImportError:
            log.debug("BUILDER WeasyPrint absent - PDF desactive. pip install weasyprint")
        except Exception as e:
            log.warning(f"BUILDER WeasyPrint erreur - PDF non genere : {e}")

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
    Construit le message MIME avec pieces jointes HTML + PDF.
    B10-FIX : MIME text/html pour HTML, application/pdf pour PDF.
    """
    msg = MIMEMultipart("mixed")
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg["Subject"] = f"SENTINEL - Rapport robotique defense du {date_str}"

    body = (
        f"Bonjour,

"
        f"Veuillez trouver ci-joint le rapport SENTINEL du {date_str}.
"
        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la defense.
"
        f"Le rapport HTML peut etre ouvert directement dans un navigateur.
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
                log.warning(f"BUILDER piece jointe {att_path.name} ignoree : {e}")

    return msg


# =============================================================================
# PURGE ANCIENS RAPPORTS (R5-NEW-3 / CDC-4)
# =============================================================================

def purge_old_reports(days: int = 30) -> int:
    """
    Supprime HTML, PDF et PNG charts de plus de N jours dans OUTPUT_DIR.
    Retourne le nombre de fichiers supprimes.
    """
    cutoff  = time.time() - days * 86400
    removed = 0
    for pattern in ("*.html", "*.pdf", "charts/*.png", "charts/*.json"):
        for f in OUTPUT_DIR.glob(pattern):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError as e:
                log.debug(f"PURGE impossible {f.name} : {e}")
    if removed:
        log.info(f"PURGE {removed} fichier(s) > {days}j supprime(s)")
    return removed
