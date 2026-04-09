# report_builder.py — SENTINEL v3.40 — Rapport HTML complet avec graphiques
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   A13         Badge alerte VERT/ORANGE/ROUGE parsé dynamiquement
#   A14         ToC HTML avec ancres module-N
#   A23         alt unique par image (WCAG 2.1)
#   A24         Dark mode CSS natif prefers-color-scheme
#   B3          img_to_base64 avec try/except FileNotFoundError
#   B10         MIME text/html + application/pdf (non octet-stream)
#   F-5         OUTPUT_DIR via Path(__file__).parent — safe en cron
#   R5A3-NEW-2  outpath NameError corrigé (toujours défini avant utilisation)
#   R5A3-NEW-3  <meta name="color-scheme"> dark light
#   R5A3-NEW-5  ul wrapping dans markdown_to_html
#   V-2         str.replace() au lieu de .format() — safe si JSON Claude
#   VIS-3       WeasyPrint PDF optionnel + chemin absolu
#   VIS-6       Tables Markdown → HTML
#   VIS-7       meta generator
#   VISUAL-R1   metrics lues depuis SentinelDB (plus regex HTML)
#   VISUAL-R3   @media print CSS complet
#   R5-NEW-3    purge_old_reports() — CDC-4
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import base64
import logging
import re
import time
from datetime import datetime
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

log = logging.getLogger("sentinel.reportbuilder")

# ── Chemins (F-5 : absolu safe en cron) ──────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS IMAGES
# ─────────────────────────────────────────────────────────────────────────────

# VIS-2 / A23 — map nom fichier → alt text descriptif WCAG
_ALT_MAP: dict[str, str] = {
    "activite30j":        "Courbe activité sectorielle – robotique défense 30 derniers jours",
    "activite30j_plotly": "Courbe activité sectorielle interactive – 30 derniers jours",
    "repartition_geo":    "Camembert répartition géographique des événements",
    "radar_techno":       "Radar technologique – intensité par domaine UGV USV UUV IA Essaims",
    "top_acteurs":        "Histogramme top acteurs industrie défense – fréquence de mention",
    "evolution_alertes":  "Histogramme évolution alertes ouvertes / closes sur 30 jours",
    "heatmap_acteurs":    "Heatmap co-occurrence acteurs × domaines défense",
    "contrats_montants":  "Graphique top contrats et financements détectés",
    "acteurs_geo":        "Carte choroplèthe activité mondiale robotique de défense par pays",
}


def chart_alt(path: Optional[str]) -> str:
    """Alt text descriptif depuis le nom de fichier PNG (VIS-2 / A23)."""
    if not path:
        return "Graphique SENTINEL"
    if isinstance(path, str) and path.strip().startswith("<"):
        return "Graphique interactif SENTINEL"
    name = Path(path).stem
    return _ALT_MAP.get(name, f"Graphique SENTINEL – {name}")


def img_to_base64(path: str) -> str:
    """Encode un PNG en base64 string. Retourne '' si fichier absent (B3)."""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except (FileNotFoundError, IOError) as e:
        log.warning(f"BUILDER img_to_base64 : fichier absent {path} – {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# MARKDOWN → HTML
# ─────────────────────────────────────────────────────────────────────────────\n\ndef _md_table(m: re.Match) -> str:\n    """Convertit un bloc tableau Markdown en <table> HTML (VIS-6)."""\n    rows = [\n        r.strip() for r in m.group(0).strip().split("\n")\n        if r.strip() and not re.match(r"^[s|:=-]+$", r.strip())\n    ]
    if not rows:
        return ""
    html_rows: list[str] = []
    for k, row in enumerate(rows):
        cells = [c.strip() for c in row.strip("|").split("|")]
        tag = "th" if k == 0 else "td"
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
    Gère : h1/h2/h3 avec ancres module-N, gras, listes (ul wrapping),
    tables Markdown, badge NON VÉRIFIÉ, sauts de paragraphe.
    """
    # ── Titres avec ancres module-N (A14-FIX / R5-NEW-1) ──────────────────
    text = re.sub(r"^# (.+)$", r"<h1>\u0001</h1>", text, flags=re.M)

    def _h2_with_anchor(m: re.Match) -> str:
        title = m.group(1)
        mod_m = re.match(r"MODULE (d+)", title)
        if mod_m:
            return f'<h2 id="module-{mod_m.group(1)}">{title}</h2>'
        return f"<h2>{title}</h2>"

    text = re.sub(r"^## (.+)$", _h2_with_anchor, text, flags=re.M)
    text = re.sub(r"^### (.+)$", r"<h3>\u0001</h3>", text, flags=re.M)

    # ── Gras / italique ───────────────────────────────────────────────────
    text = re.sub(r"**(.+?)**", r"<strong>\u0001</strong>", text)
    text = re.sub(r"*(.+?)*",     r"<em>\u0001</em>",         text)

    # ── Tables Markdown → HTML (VIS-6) ────────────────────────────────────\n    text = re.sub(r"(|.+|\n?)+", _md_table, text, flags=re.M)

    # ── Listes à puces (R5A3-NEW-5 ul wrapping) ──────────────────────────\n    text = re.sub(r"^[-*] (.+)$", r"<li>\u0001</li>", text, flags=re.M)\n    text = re.sub(r"(<li>.*?</li>\n?)+", r"<ul>g<0></ul>", text, flags=re.S)

    # ── Badge NON VÉRIFIÉ ─────────────────────────────────────────────────\n    text = re.sub(\n        r"NON VÉRIFIÉ[^.\n]*",\n        '<span style="color:#C00000;font-weight:bold">⚠ NON VÉRIFIÉ</span>',\n        text,\n    )

    # ── Sauts de paragraphe ───────────────────────────────────────────────\n    text = text.replace("\n\n", "</p><p>")\n    text = re.sub(r"(?<!</p>)\n", "<br>", text)
    return f"<p>{text}</p>"


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE HTML
# Marqueurs : %%DATE%% %%ALERTE_BANNER%% %%TOC%% %%CHARTS_SECTION%%
#             %%REPORT_BODY%% %%TIMESTAMP%%
# str.replace() uniquement — V-2 safe sur JSON Claude avec accolades {}
# ─────────────────────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="dark light">
<meta name="generator" content="SENTINEL v3.40">
<meta name="robots" content="noindex, nofollow">
<meta property="og:title" content="SENTINEL – Veille Robotique Défense">
<meta property="og:description" content="Rapport de veille automatisé – Robotique défense">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><path fill='%231F3864' d='M16 2L4 7v9c0 7.7 5.2 14.9 12 17z'/><text x='7' y='24' font-size='14' fill='white'>S</text></svg>">
<title>SENTINEL – %%DATE%%</title>
<style>
/* ── Base ────────────────────────────────────────────────────────── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth;-webkit-font-smoothing:antialiased;scroll-padding-top:64px}
body{font-family:system-ui,'Segoe UI',Arial,sans-serif;max-width:1140px;
  margin:0 auto;padding:28px 20px;color:#2d2d2d;background:#f8f9fa;
  font-size:15px;line-height:1.68}

/* ── Titres ──────────────────────────────────────────────────────── */
h1{background:#1F3864;color:#fff;padding:16px 26px;border-radius:8px;
  font-size:20px;letter-spacing:.03em;margin-bottom:10px}
h2{color:#1F3864;border-bottom:3px solid #1F3864;padding-bottom:5px;
  margin-top:38px;margin-bottom:14px;font-size:17px}
h3{color:#2E75B6;margin-top:24px;margin-bottom:8px;font-size:15px}
p{margin-bottom:10px;max-width:85ch}

/* ── Badge alerte ────────────────────────────────────────────────── */
.alerte-banner{padding:13px 22px;font-size:1.08em;font-weight:700;
  text-align:center;border-radius:7px;margin-bottom:20px;
  letter-spacing:.06em;color:#fff}
.alerte-banner.vert  {background:#2D6A2D}
.alerte-banner.orange{background:#9C6500}
.alerte-banner.rouge {background:#C00000}

/* ── Résumé exécutif / KPI ───────────────────────────────────────── */
.exec-summary{background:#D6E8F7;border-left:6px solid #1F3864;
  padding:14px 20px;margin:18px 0;border-radius:5px}

/* ── Navigation ToC ──────────────────────────────────────────────── */
nav.toc{background:#f0f4f8;border:1px solid #c8d4e4;border-radius:7px;
  padding:11px 20px;margin:16px 0;font-size:.87em;line-height:2.1}
nav.toc a{color:#1F3864;text-decoration:none;margin-right:10px}
nav.toc a:hover{text-decoration:underline}

/* ── Tables Markdown ─────────────────────────────────────────────── */
table.md-table{border-collapse:collapse;width:100%;margin:14px 0;
  font-size:.9em;table-layout:auto}
table.md-table th,table.md-table td{border:1px solid #ccc;
  padding:7px 10px;text-align:left;vertical-align:top}
table.md-table th{background:#1F3864;color:#fff;cursor:pointer;
  user-select:none;white-space:nowrap}
table.md-table th:hover{background:#2E5896}
table.md-table tr:nth-child(even) td{background:#f4f7fb}
table.md-table tr:hover td{background:#e8f0fa}

/* ── Charts PNG ──────────────────────────────────────────────────── */
.chart-grid{display:flex;gap:18px;flex-wrap:wrap;margin:18px 0;
  align-items:flex-start}
.chart-grid img{max-width:calc(50% - 9px);flex:1 1 300px;
  border:1px solid #ddd;border-radius:5px}
.chart-full{margin:16px 0}
.chart-full img{width:100%;border:1px solid #ddd;border-radius:5px}

/* ── Charts Plotly HTML ──────────────────────────────────────────── */
.chart-plotly{margin:16px 0;border:1px solid #ddd;border-radius:5px;
  overflow:hidden;background:#fff}
.chart-plotly-pair{display:flex;gap:18px;margin:18px 0;flex-wrap:wrap}
.chart-plotly-pair .chart-plotly{flex:1 1 300px}

/* ── Code ────────────────────────────────────────────────────────── */
pre{background:#f5f5f5;padding:12px;border-radius:5px;
  overflow-x:auto;font-size:12px}

/* ── Divers ──────────────────────────────────────────────────────── */
a{color:#2E75B6}a:hover{color:#1F3864}
ul{padding-left:22px;margin:8px 0 10px}li{margin-bottom:4px}

/* ── Footer ──────────────────────────────────────────────────────── */
.footer{text-align:center;color:#999;font-size:11px;margin-top:48px;
  border-top:1px solid #eee;padding-top:16px}

/* ── Responsive mobile ───────────────────────────────────────────── */
@media(max-width:768px){
  body{padding:12px 10px;font-size:14px}
  .chart-grid img,.chart-plotly-pair .chart-plotly{
    max-width:100%!important;flex:1 1 100%}
  h1{font-size:15px;padding:12px 14px}
  nav.toc{font-size:.8em;line-height:1.9}
}

/* ── Dark mode natif (A24-FIX) ───────────────────────────────────── */
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

/* ── Print / PDF (VISUAL-R3) ─────────────────────────────────────── */
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
}
</style>
</head>
<body>

%%ALERTE_BANNER%%

<h1>SENTINEL v3.40 &mdash; Rapport du %%DATE%%</h1>

%%TOC%%

%%CHARTS_SECTION%%

%%REPORT_BODY%%

<div class="footer">
  SENTINEL v3.40 &bull; Généré le %%TIMESTAMP%% &bull; Budget ~5 &euro;/mois
</div>

<script>
/* Tri colonnes tableaux MODULE 6 (V2-FIX) */
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


# ─────────────────────────────────────────────────────────────────────────────
# BADGE ALERTE (A13-FIX)
# ─────────────────────────────────────────────────────────────────────────────

_ALERTE_ICONS  = {"VERT": "🟢", "ORANGE": "🟠", "ROUGE": "🔴"}
_ALERTE_LABELS = {
    "VERT":   "SURVEILLANCE NORMALE",
    "ORANGE": "ACTIVITÉ ÉLEVÉE",
    "ROUGE":  "ALERTE CRITIQUE",
}


def _parse_alerte_niveau(report_text: str) -> str:
    """
    Extrait le niveau d'alerte depuis le texte Claude.
    Retourne 'VERT', 'ORANGE' ou 'ROUGE'. Défaut : 'VERT'.
    A13-FIX : regex case-insensitive, indépendante du format exact.
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
    icon  = _ALERTE_ICONS.get(niv, "🟢")
    label = _ALERTE_LABELS.get(niv, niv)
    return f'<div class="alerte-banner {cls}">{icon}&nbsp; NIVEAU ALERTE : {label}</div>'


# ─────────────────────────────────────────────────────────────────────────────
# TABLE DES MATIÈRES (A14-FIX)
# ─────────────────────────────────────────────────────────────────────────────

# V-03-FIX : TOC dynamique construite depuis les headings du rapport réel
def _build_toc(report_text: str = "") -> str:
    """Construit la TOC depuis les ## MODULE N détectés dans report_text."""
    import re as _re
    items = []
    if report_text:
        for m in _re.finditer(
            r"^##\s+((?:RÉSUMÉ|MODULE\s+\d+)[^\n]*)",
            report_text, _re.MULTILINE | _re.IGNORECASE
        ):
            title = m.group(1).strip()
            num_m = _re.search(r"MODULE\s+(\d+)", title, _re.IGNORECASE)
            if num_m:
                anchor = f"module-{num_m.group(1)}"
            else:
                anchor = "resume-executif"
            label = title[:45] + ("…" if len(title) > 45 else "")
            items.append((anchor, label))
    # Fallback statique si rapport vide
    if not items:
        items = [
            ("resume-executif", "Résumé"),
            ("module-1", "1·Tableau"), ("module-2", "2·Faits"),
            ("module-3", "3·Terrestre"), ("module-4", "4·Maritime"),
            ("module-5", "5·IA"), ("module-6", "6·Acteurs"),
            ("module-7", "7·Contrats"), ("module-8", "8·Signaux"),
            ("module-9", "9·Sources"),
        ]
    links = " &middot; ".join(f'<a href="#{a}">{l}</a>' for a, l in items)
    return f'<nav class="toc no-print"><strong>Navigation :</strong> {links}</nav>'



# ─────────────────────────────────────────────────────────────────────────────
# SECTION GRAPHIQUES
# ─────────────────────────────────────────────────────────────────────────────

def _is_plotly_html(p: str) -> bool:
    """True si p est un div HTML Plotly (A25-FIX) plutôt qu'un chemin PNG."""
    return isinstance(p, str) and p.strip().startswith("<")


def _render_chart(p: str, full: bool = True, extra_cls: str = "") -> str:
    """
    Rend un chart soit en <img> base64 (PNG) soit en div Plotly (HTML).
    Retourne chaîne vide si chart invalide ou image manquante (VIS-1 guard).
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
        # V1-FIX: SVG inline — 10x plus léger que PNG base64
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
      [0] activité 30j             → pleine largeur
      [1] répartition géo  ┐
      [2] radar techno     ┘       → côte à côte
      [3] top acteurs              → pleine largeur
      [4] heatmap acteurs          → pleine largeur
      [5] évolution alertes        → pleine largeur
      [6] contrats montants        → pleine largeur
      [7] carte acteurs géo        → pleine largeur

    Accepte PNG paths ET divs HTML Plotly (A25-FIX).
    """
    # V-04-FIX : message explicatif si aucun graphique disponible
    if not chart_paths:
        return (
            '<div class="charts-empty" style="background:#f8f9fa;border:1px solid #dee2e6;'
            'border-radius:6px;padding:20px;margin:16px 0;text-align:center;color:#6c757d;">' 
            '<p><strong>Graphiques non disponibles</strong></p>'
            '<p>Base de données insuffisante (premier lancement) ou erreur de génération.<br>'
            'Les graphiques apparaîtront à partir du 2e rapport.</p>'
            '</div>'
        )

    if not chart_paths:
        return ""
    valid = [p for p in chart_paths if p]
    if not valid:
        return ""

    parts: list[str] = ["<h2>Visualisations SENTINEL</h2>"]

    for idx, p in enumerate(valid):
        if idx == 0:
            parts.append(_render_chart(p, full=True))

        elif idx == 1:
            # Paire géo + radar côte à côte
            p2        = valid[2] if len(valid) > 2 else None
            pair_cls  = "chart-plotly-pair" if (
                _is_plotly_html(p) or (p2 and _is_plotly_html(p2))
            ) else "chart-grid"
            c1 = _render_chart(p,  full=False)
            c2 = _render_chart(p2, full=False) if p2 else ""
            parts.append(f'<div class="{pair_cls}">{c1}{c2}</div>')

        elif idx == 2:
            pass  # déjà traité dans idx == 1\n\n        else:\n            parts.append(_render_chart(p, full=True))\n\n    return "\n".join(s for s in parts if s)


# ─────────────────────────────────────────────────────────────────────────────
# KPI BANNER DEPUIS SENTINELDB (VISUAL-R1)
# ─────────────────────────────────────────────────────────────────────────────

def _inject_metrics_banner(report_text: str) -> str:
    """
    Lit les métriques du jour depuis SentinelDB et génère une ligne de KPI HTML.
    VISUAL-R1 : remplace l'extraction regex sur le texte du rapport.
    Dégradation gracieuse si SentinelDB indisponible.
    """
    try:
        from db_manager import SentinelDB
        rows = SentinelDB.getmetrics(ndays=1)
        if not rows:
            return ""
        m           = rows[0]
        indice      = m.get("indice",       "–")
        alerte      = (m.get("alerte",      "VERT") or "VERT").upper()
        nb_src      = m.get("nb_articles",   "–")
        nb_pert     = m.get("nb_pertinents", "–")
        terrestre   = m.get("terrestre",    0) or 0
        maritime    = m.get("maritime",     0) or 0
        transverse  = m.get("transverse",   0) or 0
        contractuel = m.get("contractuel",  0) or 0
        color_map   = {"VERT": "#2D6A2D", "ORANGE": "#9C6500", "ROUGE": "#C00000"}
        color       = color_map.get(alerte, "#2D6A2D")
        return (
            '<div class="exec-summary" style="display:flex;gap:24px;'
            'flex-wrap:wrap;font-size:.9em;">'
            f'<span><strong>Indice activité :</strong> '
            f'<span style="color:{color};font-weight:700">{indice}/10</span></span>'
            f'<span><strong>Sources :</strong> {nb_src} analysées / {nb_pert} pertinentes</span>'
            f'<span><strong>Terrestre</strong> {terrestre:.0f} &bull; '
            f'<strong>Maritime</strong> {maritime:.0f} &bull; '
            f'<strong>Transverse</strong> {transverse:.0f} &bull; '
            f'<strong>Contrats</strong> {contractuel:.0f}</span>'
            "</div>"
        )
    except Exception as e:
        log.debug(f"BUILDER metrics_banner non disponible : {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# BUILD HTML REPORT — FONCTION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

def build_html_report(
    report_text: str,
    chart_paths: list,
    date_obj,
    output_dir: Optional[Path] = None,
) -> str:
    """
    Génère le rapport HTML complet, le sauvegarde, et retourne son chemin absolu.

    Paramètres
    ----------
    report_text : str   Texte brut Claude (Markdown avec modules).
    chart_paths : list  Chemins PNG ou divs HTML Plotly (peut contenir None).
    date_obj    : date  Date du rapport.
    output_dir  : Path  Répertoire sortie. Défaut : OUTPUT_DIR (absolu F-5).

    Corrections clés
    ----------------
    R5A3-NEW-2  outpath défini systématiquement avant utilisation
    V-2         str.replace() — safe sur JSON Claude avec accolades {}
    """
    out_dir   = Path(output_dir) if output_dir else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    date_str  = date_obj.strftime("%d/%m/%Y")
    date_file = date_obj.strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

    alerte_banner  = _build_alerte_banner(report_text)
    metrics_banner = _inject_metrics_banner(report_text)
    toc            = _build_toc(report_text)  # V-03-FIX : TOC dynamique
    charts_section = _build_charts_section(chart_paths or [])
    report_body    = metrics_banner + markdown_to_html(report_text)

    # Injection via str.replace() — V-2 safe sur accolades JSON Claude
    html = (
        _HTML_TEMPLATE
        .replace("%%DATE%%",           date_str)
        .replace("%%ALERTE_BANNER%%",  alerte_banner)
        .replace("%%TOC%%",            toc)
        .replace("%%CHARTS_SECTION%%", charts_section)
        .replace("%%REPORT_BODY%%",    report_body)
        .replace("%%TIMESTAMP%%",      timestamp)
    )

    # Sauvegarde HTML (R5A3-NEW-2 : outpath toujours défini ici)
    out_path = out_dir / f"SENTINEL_{date_file}_rapport.html"
    out_path.write_text(html, encoding="utf-8")
    log.info(f"BUILDER Rapport HTML → {out_path.resolve()}")

    # Export PDF optionnel WeasyPrint (VIS-3)
    pdf_path = out_dir / f"SENTINEL_{date_file}_rapport.pdf"
    try:
        from weasyprint import HTML as WH  # type: ignore
        WH(string=html).write_pdf(str(pdf_path))
        log.info(f"BUILDER Rapport PDF  → {pdf_path.resolve()}")
    except ImportError:
        log.debug("BUILDER WeasyPrint absent — PDF désactivé. `pip install weasyprint`")
    except Exception as e:
        log.warning(f"BUILDER WeasyPrint erreur — PDF non généré : {e}")

    return str(out_path)


# ─────────────────────────────────────────────────────────────────────────────
# BUILD EMAIL MESSAGE — séparé pour tests unitaires (B10-FIX)
# ─────────────────────────────────────────────────────────────────────────────\n\ndef build_email_message(\n    date_str: str,\n    html_body: str,\n    from_addr: str,\n    to_addr: str,\n    output_dir: Optional[Path] = None,\n) -> MIMEMultipart:\n    """\n    Construit le message MIME avec pièces jointes HTML + PDF.\n    B10-FIX : MIME text/html (non application/octet-stream) pour HTML,\n              application/pdf pour PDF.\n    """\n    msg = MIMEMultipart("mixed")\n    msg["From"]    = from_addr\n    msg["To"]      = to_addr\n    msg["Subject"] = f"SENTINEL — Rapport robotique défense du {date_str}"\n\n    body = (\n        f"Bonjour,\n\n"\n        f"Veuillez trouver ci-joint le rapport SENTINEL du {date_str}.\n"\n        f"Ce rapport couvre la robotique marine et terrestre dans le domaine de la défense.\n"\n        f"Le rapport HTML peut être ouvert directement dans un navigateur.\n"\n    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    out_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    try:
        date_file = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        date_file = date_str

    for suffix, m_type, m_sub in [
        (".html", "text",        "html"),   # B10-FIX : était application/octet-stream
        (".pdf",  "application", "pdf"),    # B10-FIX
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


# ─────────────────────────────────────────────────────────────────────────────
# PURGE ANCIENS RAPPORTS (R5-NEW-3 / CDC-4)
# ─────────────────────────────────────────────────────────────────────────────

def purge_old_reports(days: int = 30) -> int:
    """
    Supprime HTML, PDF et PNG charts de plus de N jours dans OUTPUT_DIR.
    Retourne le nombre de fichiers supprimés. (R5-NEW-3 / CDC-4)
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
        log.info(f"PURGE {removed} fichier(s) > {days}j supprimé(s)")
    return removed