#!/usr/bin/env python3
# dashboard.py — SENTINEL v3.60.1 — Dashboard Streamlit interactif
# =============================================================================
# FIXES v3.60.1 :
#   [REG-FIX]  Regex reconstruites via chr(92) pour survivre au rendu Markdown.
#              Les d et s ne peuvent plus être avalés au copier-coller.
#   [CLUST-FIX] Guard \b dans _cluster_tendances pour variantes <= 3 caractères.
# =============================================================================

import glob
import json
import os
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import streamlit as st
    import plotly.graph_objects as go
    import plotly.express as px
    import pandas as pd
except ImportError:
    raise SystemExit("[DASHBOARD] pip install streamlit plotly pandas")

try:
    from db_manager import SentinelDB, getdb, DBPATH  # type: ignore
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False
    DBPATH = Path("data/sentinel.db")

_VERSION     = "3.60.1"
_ALERT_SIGMA = float(os.environ.get("SENTINEL_ALERT_SIGMA", "1.5"))
_ROOT        = Path(__file__).parent

_DOMAIN_COLORS = {
    "Terrestre":   "#c0392b",
    "Maritime":    "#2980b9",
    "Transverse":  "#27ae60",
    "Contractuel": "#f39c12",
}

_PATENTS_LATEST  = _ROOT / "data" / "patents_latest.json"
_PATENTS_HISTORY = _ROOT / "data" / "patents_history.json"
_PATENTS_USPTO   = _ROOT / "data" / "patents_uspto_latest.json"
_QUOTA_FILE      = _ROOT / "logs"  / "ops_quota.json"

# =============================================================================
# REG-FIX — Caractères spéciaux regex construits via chr() pour immunité
# contre le rendu Markdown qui avale les backslashes au copier-coller.
# chr(92) =  | chr(100) = d | chr(115) = s | chr(98) = b
# =============================================================================
_BS  = chr(92)           # \
_D   = _BS + "d"         # d
_S   = _BS + "s"         # s
_B   = _BS + "b"         # \b
_DOT = _BS + "."         # .

# Patterns compilés une seule fois au chargement du module
_PAT_ALERT = re.compile(
    "Alerte" + _S + "*[:|]?" + _S + "*(VERT|ORANGE|ROUGE)",
    re.IGNORECASE,
)
_PAT_SCORE = re.compile(
    "Indice.{0,40}?(" + _D + "+(?:" + _DOT + _D + "+)?)" + _S + "*/" + _S + "*10",
    re.IGNORECASE,
)
_PAT_GEO = re.compile(
    "USA" + _S + "*(" + _D + "+)%.+?"
    "Europe" + _S + "*(" + _D + "+)%.+?"
    "Asie" + _S + "*(" + _D + "+)%.+?"
    "MO" + _S + "*(" + _D + "+)%.+?"
    "Russie" + _S + "*(" + _D + "+)%",
    re.IGNORECASE | re.DOTALL,
)
_PAT_DOMAINS = re.compile(
    "Terrestre" + _S + "*(" + _D + "+)%.+?"
    "Maritime" + _S + "*(" + _D + "+)%.+?"
    "Transverse" + _S + "*(" + _D + "+)%.+?"
    "Contractuel" + _S + "*(" + _D + "+)%",
    re.IGNORECASE | re.DOTALL,
)

# =============================================================================
# V360-1 — Dictionnaire de synonymes pour le Trend Clustering
# =============================================================================
_SYNONYMES: dict[str, list[str]] = {
    "UGV":          ["unmanned ground vehicle", "robot terrestre", "vehicule terrestre autonome", "ugv"],
    "USV":          ["unmanned surface vehicle", "drone naval", "bateau autonome", "usv"],
    "UAV":          ["unmanned aerial vehicle", "drone aerien", "aeronef sans pilote", "uav"],
    "UCAV":         ["drone de combat", "unmanned combat aerial", "ucav", "combat drone"],
    "SHORAD":       ["short range air defense", "defense aerienne courte portee", "shorad"],
    "IA Defense":   ["intelligence artificielle defense", "ai defense", "machine learning defense",
                     "ia militaire", "autonomous weapons ai"],
    "Essaim":       ["swarm", "drone swarm", "essaim de drones", "multi-agent"],
    "C-UAS":        ["counter uas", "anti-drone", "contre-drone", "c-uas", "counter drone"],
    "Logistique":   ["logistics", "logistique autonome", "autonomous logistics", "resupply robot"],
    "Cyber":        ["cyberattaque", "cyber defense", "cybersecurite", "cyberwarfare"],
}


def _cluster_tendances(tendances: list[dict]) -> list[dict]:
    """V360-1 + CLUST-FIX — Clustering avec guard \\b pour variantes <= 3 chars."""
    variante_to_canon: dict[str, str] = {}
    for canon, variantes in _SYNONYMES.items():
        for v in variantes:
            variante_to_canon[v.lower()] = canon

    clustered: dict[str, dict] = {}

    for t in tendances:
        texte       = (t.get("texte") or "").strip()
        texte_lower = texte.lower()
        canon       = variante_to_canon.get(texte_lower)

        if not canon:
            for variante, c in variante_to_canon.items():
                # CLUST-FIX : guard \b pour les acronymes courts (<= 3 chars)
                if len(variante) <= 3:
                    if re.search(_B + re.escape(variante) + _B, texte_lower):
                        canon = c
                        break
                else:
                    if variante in texte_lower or texte_lower in variante:
                        canon = c
                        break

        key = canon if canon else texte

        if key not in clustered:
            clustered[key] = {
                "texte":        key,
                "count":        int(t.get("count", 1) or 1),
                "datepremiere": t.get("datepremiere", ""),
                "datederniere": t.get("datederniere", ""),
                "variantes":    [texte] if canon else [],
            }
        else:
            clustered[key]["count"]        += int(t.get("count", 1) or 1)
            clustered[key]["datederniere"]  = max(
                clustered[key]["datederniere"] or "",
                t.get("datederniere") or "",
            )
            if canon and texte not in clustered[key]["variantes"]:
                clustered[key]["variantes"].append(texte)

    return sorted(clustered.values(), key=lambda x: x["count"], reverse=True)


# =============================================================================
# CACHE
# =============================================================================

@st.cache_data(ttl=300)
def load_metrics(n_days: int = 30) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getmetrics(n_days)
    except Exception as e:
        st.warning(f"Metriques indisponibles : {e}")
        return []


@st.cache_data(ttl=300)
def load_reports(n_days: int = 30) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getrecentreports(n_days)
    except Exception as e:
        st.warning(f"Rapports indisponibles : {e}")
        return []


@st.cache_data(ttl=60)
def load_tendances(limit: int = 50) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.gettendancesactives(limit)
    except Exception:
        return []


@st.cache_data(ttl=60)
def load_alertes() -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getalertesactives()
    except Exception:
        return []


@st.cache_data(ttl=600)
def load_acteurs(limit: int = 50) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getacteurs(limit)
    except Exception:
        return []


@st.cache_data(ttl=300)
def _list_html_reports() -> list[tuple[str, str]]:
    files = sorted(glob.glob("output/SENTINEL_*.html"), reverse=True)
    return [(Path(f).stem.replace("SENTINEL_", ""), f) for f in files[:60]]


@st.cache_data(ttl=300)
def load_patents_ops() -> list[dict]:
    try:
        if _PATENTS_LATEST.exists():
            return json.loads(_PATENTS_LATEST.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


@st.cache_data(ttl=300)
def load_patents_history() -> list[dict]:
    try:
        if _PATENTS_HISTORY.exists():
            return json.loads(_PATENTS_HISTORY.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


@st.cache_data(ttl=300)
def load_patents_uspto() -> list[dict]:
    try:
        if _PATENTS_USPTO.exists():
            return json.loads(_PATENTS_USPTO.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


@st.cache_data(ttl=60)
def load_ops_quota() -> dict:
    try:
        if _QUOTA_FILE.exists():
            return json.loads(_QUOTA_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


# =============================================================================
# HELPERS HTML — REG-FIX : utilisation des patterns pre-compiles
# =============================================================================

def _read_html_as_text(path: str) -> str:
    try:
        raw = Path(path).read_text(encoding="utf-8", errors="ignore")
        return re.sub("<[^>]+>", " ", raw)
    except Exception:
        return ""


def _regex_alert(text: str) -> str:
    m = _PAT_ALERT.search(text)
    return m.group(1).upper() if m else "N/A"


def _regex_score(text: str) -> float | None:
    m = _PAT_SCORE.search(text)
    return float(m.group(1)) if m else None


def _regex_geo(text: str) -> dict:
    geo   = {"USA": 0, "Europe": 0, "Asie": 0, "MO": 0, "Russie": 0}
    match = _PAT_GEO.search(text)
    if match:
        for k, v in zip(geo.keys(), match.groups()):
            geo[k] = int(v)
    return geo


def _regex_domains(text: str) -> dict:
    d: dict[str, int] = {}
    match = _PAT_DOMAINS.search(text)
    if match:
        for label, val in zip(
            ["Terrestre", "Maritime", "Transverse", "Contractuel"], match.groups()
        ):
            d[label] = int(val)
    return d


# =============================================================================
# HELPERS BREVETS
# =============================================================================

_TIER1_TOKENS: frozenset[str] = frozenset({
    "anduril", "rheinmetall", "lockheed martin", "northrop grumman",
    "raytheon", "darpa", "elbit", "kongsberg", "milrem", "l3harris",
    "bae systems", "thales", "safran", "mbda", "dassault",
    "israel aerospace", "rafael", "ghost robotics", "shield ai",
})
_TIER2_TOKENS: frozenset[str] = frozenset({
    "leidos", "saic", "booz allen", "kratos", "hanwha",
    "qinetiq", "saab", "leonardo", "hensoldt", "textron",
    "aerovironment", "palantir",
})


def _patent_tier(patent: dict) -> str:
    names = [a.lower().strip() for a in patent.get("assignees", []) if a]
    for name in names:
        if any(t in name for t in _TIER1_TOKENS):
            return "TIER-1"
    for name in names:
        if any(t in name for t in _TIER2_TOKENS):
            return "TIER-2"
    return "Autre"


def _deduplicate_patents(patents: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out:  list[dict] = []
    for p in patents:
        pid = p.get("pub_num") or p.get("patent_id") or ""
        if pid and pid in seen:
            continue
        if pid:
            seen.add(pid)
        out.append(p)
    return out


# =============================================================================
# KPI
# =============================================================================

def _kpis_from_sources(metrics: list[dict], html_text: str) -> dict:
    kpis = {"alerte": "N/A", "indice": None, "nb_articles": None, "nb_pertinents": None}
    if metrics:
        latest = metrics[0]
        kpis["alerte"]        = (latest.get("alerte") or "N/A").upper()
        kpis["indice"]        = latest.get("indice")
        kpis["nb_articles"]   = latest.get("nb_articles")
        kpis["nb_pertinents"] = latest.get("nb_pertinents")
    elif html_text:
        kpis["alerte"] = _regex_alert(html_text)
        kpis["indice"] = _regex_score(html_text)
    return kpis


# =============================================================================
# GRAPHIQUES Plotly
# =============================================================================

def chart_activity(metrics_display: list[dict], metrics_sigma: list[dict]) -> go.Figure:
    data = [
        (m["date"][:10], float(m["indice"]))
        for m in reversed(metrics_display)
        if m.get("indice") is not None
    ]
    fig = go.Figure()
    if not data:
        return fig.update_layout(title="Pas de donnees disponibles")

    dates, scores = zip(*data)
    scores_list   = list(scores)

    fig.add_trace(go.Scatter(
        x=list(dates), y=scores_list,
        mode="lines+markers",
        line=dict(color="#e74c3c", width=2),
        marker=dict(size=7, color="#e74c3c"),
        name="Indice activite",
        hovertemplate="%{x}<br>Score : %{y:.1f}/10<extra></extra>",
    ))

    sigma_scores = [float(m["indice"]) for m in metrics_sigma if m.get("indice") is not None]
    if not sigma_scores:
        sigma_scores = scores_list

    avg   = sum(sigma_scores) / len(sigma_scores)
    std   = statistics.stdev(sigma_scores) if len(sigma_scores) > 1 else 0.0
    seuil = avg + _ALERT_SIGMA * std

    fig.add_hline(y=avg, line_dash="dot", line_color="gray",
                  annotation_text=f"Moy. 90j {avg:.1f}",
                  annotation_position="bottom right")
    fig.add_hline(y=seuil, line_dash="dash", line_color="orange",
                  annotation_text=f"Seuil µ+{_ALERT_SIGMA}s (90j) = {seuil:.1f}",
                  annotation_position="top right")

    alert_pts = [(d, s) for d, s in zip(dates, scores_list) if s >= seuil]
    if alert_pts:
        ad, av = zip(*alert_pts)
        fig.add_trace(go.Scatter(
            x=list(ad), y=list(av), mode="markers",
            marker=dict(size=12, color="red", symbol="star"),
            name="Depassement seuil",
        ))

    fig.update_layout(
        title=f"Indice activite sectorielle ({len(dates)} jours — seuil calcule sur 90j)",
        xaxis_title="Date", yaxis_title="Score /10",
        yaxis_range=[0, 10.5],
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(t=80, b=40),
    )
    return fig


def chart_geo_pie(geo: dict, title: str = "Repartition geographique") -> go.Figure:
    labels = [k for k, v in geo.items() if v > 0]
    values = [v for v in geo.values() if v > 0]
    if not labels:
        return go.Figure().update_layout(title="Aucune donnee geographique")
    fig = px.pie(
        values=values, names=labels, title=title,
        color_discrete_sequence=px.colors.sequential.YlOrRd[::-1],
        hole=0.38,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(margin=dict(t=60, b=20))
    return fig


def chart_domain_bar(domains: dict) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=list(domains.keys()),
        y=list(domains.values()),
        marker_color=[_DOMAIN_COLORS.get(k, "#7f8c8d") for k in domains],
        text=[f"{v:.0f}%" for v in domains.values()],
        textposition="outside",
    ))
    fig.update_layout(
        title="Repartition par domaine (%)",
        yaxis_title="%", yaxis_range=[0, 105],
        margin=dict(t=60, b=40),
    )
    return fig


def chart_domain_radar(domains: dict) -> go.Figure:
    cats = list(domains.keys())
    vals = list(domains.values())
    fig  = go.Figure(go.Scatterpolar(
        r=vals + [vals[0]], theta=cats + [cats[0]],
        fill="toself", name="Domaines",
        line_color="#e74c3c", fillcolor="rgba(231,76,60,0.2)",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title="Radar domaines",
        margin=dict(t=60),
    )
    return fig


def chart_geo_evolution(metrics: list[dict]) -> go.Figure:
    dates = [m["date"][:10] for m in reversed(metrics)]
    fig   = go.Figure()
    for zone, col in [
        ("USA", "geousa"), ("Europe", "geoeurope"),
        ("Asie", "geoasie"), ("MO", "geomo"),
    ]:
        vals = [float(m.get(col, 0) or 0) for m in reversed(metrics)]
        fig.add_trace(go.Scatter(x=dates, y=vals, mode="lines",
                                 name=zone, stackgroup="one"))
    fig.update_layout(
        title="Evolution geographique empilee (%)",
        yaxis_title="%", yaxis_range=[0, 100],
        margin=dict(t=60, b=40),
    )
    return fig


def chart_alert_timeline(metrics: list[dict]) -> go.Figure:
    dates   = [m["date"][:10] for m in reversed(metrics)]
    niveaux = [(m.get("alerte") or "VERT").upper() for m in reversed(metrics)]
    code    = {"VERT": 1, "ORANGE": 2, "ROUGE": 3}
    values  = [code.get(n, 0) for n in niveaux]
    colors  = ["#27ae60" if v == 1 else "#f39c12" if v == 2 else "#e74c3c" for v in values]
    fig = go.Figure(go.Bar(
        x=dates, y=values, marker_color=colors,
        hovertext=niveaux,
        hovertemplate="%{x}<br>Niveau : %{hovertext}<extra></extra>",
    ))
    fig.update_layout(
        title="Historique niveaux d'alerte",
        yaxis=dict(tickvals=[1, 2, 3], ticktext=["VERT", "ORANGE", "ROUGE"], range=[0, 3.5]),
        margin=dict(t=60, b=40),
    )
    return fig


def chart_patents_bar(patents: list[dict], title: str = "Top brevets") -> go.Figure:
    top = sorted(patents, key=lambda p: p.get("score", 0), reverse=True)[:15]
    if not top:
        return go.Figure().update_layout(title="Aucun brevet disponible")
    colors = [
        "#c0392b" if p.get("score", 0) >= 8.0
        else "#e67e22" if p.get("score", 0) >= 6.0
        else "#7f8c8d"
        for p in top
    ]
    labels = [f"{p.get('pub_num','?')} | {p.get('cpc','')} | {p.get('score',0)}/10" for p in top]
    fig = go.Figure(go.Bar(
        x=[p.get("score", 0) for p in top],
        y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{p.get('score', 0)}/10" for p in top],
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Titre : %{customdata[0]}<br>"
            "Deposants : %{customdata[1]}<br>"
            "Date : %{customdata[2]}<extra></extra>"
        ),
        customdata=[
            [(p.get("title") or "")[:80],
             ", ".join(p.get("assignees", [])) or "N/A",
             p.get("date", "")]
            for p in top
        ],
    ))
    fig.update_layout(
        title=title, xaxis_title="Score SENTINEL", xaxis_range=[0, 11],
        yaxis_autorange="reversed",
        height=max(400, len(top) * 42),
        margin=dict(t=60, l=360, b=40, r=60),
    )
    return fig


def chart_patents_treemap(patents: list[dict], title: str = "Cartographie innovation par Tier") -> go.Figure:
    rows = []
    for p in patents:
        tier     = _patent_tier(p)
        assignees = p.get("assignees", [])
        deposant = assignees[0][:40] if assignees else "Inconnu"
        cpc      = p.get("cpc", "N/A") or "N/A"
        score    = float(p.get("score", 0) or 0)
        pub_num  = p.get("pub_num", "?") or "?"
        rows.append({
            "Tier":     tier,
            "Deposant": deposant,
            "CPC":      f"{cpc} ({pub_num})",
            "Score":    max(score, 0.1),
            "Titre":    (p.get("title") or "")[:60],
        })
    if not rows:
        return go.Figure().update_layout(title="Aucun brevet pour le treemap")
    df = pd.DataFrame(rows)
    fig = px.treemap(
        df,
        path=["Tier", "Deposant", "CPC"],
        values="Score",
        color="Score",
        color_continuous_scale=[
            [0.0, "#7f8c8d"],
            [0.5, "#e67e22"],
            [0.8, "#e74c3c"],
            [1.0, "#c0392b"],
        ],
        color_continuous_midpoint=6.0,
        hover_data={"Titre": True, "Score": ":.1f"},
        title=title,
    )
    fig.update_traces(
        textinfo="label+value",
        texttemplate="<b>%{label}</b><br>%{value:.1f}/10",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Score : %{value:.1f}/10<br>"
            "Titre : %{customdata[0]}<extra></extra>"
        ),
    )
    fig.update_layout(
        margin=dict(t=60, l=10, b=10, r=10),
        height=520,
        coloraxis_colorbar=dict(title="Score", tickvals=[2, 4, 6, 8, 10]),
    )
    return fig


# =============================================================================
# APPLICATION PRINCIPALE
# =============================================================================

def main() -> None:

    html_reports  = _list_html_reports()
    report_dates  = [d for d, _ in html_reports]
    selected_date: str | None = None

    last_report_str = report_dates[0] if report_dates else "---"
    st.set_page_config(
        page_title=f"SENTINEL -- {last_report_str}",
        page_icon="shield",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown("""
    <style>
    .pill {
        display:inline-block; background:#2c3e50; color:#ecf0f1;
        border-radius:20px; padding:4px 12px; margin:3px 2px; font-size:.82rem;
    }
    .pill-count {
        background:#e74c3c; color:#fff; border-radius:10px;
        padding:1px 6px; font-size:.72rem; margin-left:4px;
    }
    .badge-vert   { background:#27ae60; color:#fff; padding:3px 12px;
                    border-radius:12px; font-weight:bold; font-size:.9rem; }
    .badge-orange { background:#f39c12; color:#fff; padding:3px 12px;
                    border-radius:12px; font-weight:bold; font-size:.9rem; }
    .badge-rouge  { background:#e74c3c; color:#fff; padding:3px 12px;
                    border-radius:12px; font-weight:bold; font-size:.9rem; }
    .badge-na     { background:#7f8c8d; color:#fff; padding:3px 12px;
                    border-radius:12px; font-weight:bold; font-size:.9rem; }
    </style>
    """, unsafe_allow_html=True)

    # ── SIDEBAR ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(f"## SENTINEL v{_VERSION}")
        st.caption("Veille Robotique Defense")
        st.divider()

        selected_date = (
            st.selectbox("Rapport HTML", report_dates,
                         help="Rapport a afficher dans l'onglet Rapport")
            if report_dates else None
        )

        st.divider()
        n_days = st.slider("Historique (jours)", 7, 90, 30)
        st.divider()

        if st.button("Rafraichir les donnees", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.divider()

        quota = load_ops_quota()
        if quota:
            used      = quota.get("used", 0)
            remaining = max(0, 400 - used)
            week      = quota.get("week", "---")
            last_run  = (quota.get("last_run") or "")[:16].replace("T", " ")
            st.markdown("**Quota OPS Espacenet**")
            st.progress(min(used / 400, 1.0), text=f"{remaining}/400 restantes -- {week}")
            if last_run:
                st.caption(f"Dernier run : {last_run} UTC")
        else:
            st.caption("Quota OPS : non disponible")

        st.divider()

        if _DB_AVAILABLE:
            if DBPATH.exists():
                size_kb = DBPATH.stat().st_size // 1024
                st.success(f"SQLite OK
{DBPATH.name} ({size_kb} KB)")
            else:
                st.warning("DB inexistante
Lancez sentinel_main.py d'abord")
        else:
            st.error("db_manager introuvable
Mode degrade HTML actif")

    # ── CHARGEMENT DONNEES ───────────────────────────────────────────────────
    metrics       = load_metrics(n_days)
    metrics_sigma = load_metrics(90)
    reports       = load_reports(n_days)
    tendances_raw = load_tendances()
    alertes       = load_alertes()
    acteurs       = load_acteurs()
    patents_ops   = load_patents_ops()
    patents_uspto = load_patents_uspto()
    tendances     = _cluster_tendances(tendances_raw)

    html_text = ""
    if selected_date:
        for d, p in html_reports:
            if d == selected_date:
                html_text = _read_html_as_text(p)
                break

    # ── KPIs synchronises avec selected_date ─────────────────────────────────
    if selected_date and metrics:
        metrics_for_kpi = [m for m in metrics
                           if (m.get("date") or "").startswith(selected_date)]
        if not metrics_for_kpi:
            metrics_for_kpi = [m for m in metrics_sigma
                                if (m.get("date") or "").startswith(selected_date)]
    else:
        metrics_for_kpi = metrics

    kpis      = _kpis_from_sources(metrics_for_kpi, html_text)
    alert     = kpis["alerte"]
    score     = kpis["indice"]
    badge_cls = {"VERT": "badge-vert", "ORANGE": "badge-orange",
                 "ROUGE": "badge-rouge"}.get(alert, "badge-na")

    delta_score = None
    if len(metrics_for_kpi) >= 2:
        s0 = metrics_for_kpi[0].get("indice")
        s1 = metrics_for_kpi[1].get("indice")
        if s0 is not None and s1 is not None:
            delta_score = round(float(s0) - float(s1), 1)

    kpi_label = f"(donnees du {selected_date})" if selected_date else "(30 derniers jours)"
    st.title(f"SENTINEL -- Tableau de bord veille {kpi_label}")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown("**Niveau Alerte**")
        st.markdown(f'<span class="{badge_cls}">{alert}</span>', unsafe_allow_html=True)
    c2.metric("Indice Activite",
              f"{score:.1f}/10" if score is not None else "N/A",
              delta=f"{delta_score:+.1f}" if delta_score is not None else None)
    c3.metric("Rapports en base", len(reports) or len(html_reports))
    c4.metric("Tendances",        len(tendances))
    c5.metric("Alertes ouvertes", len(alertes))

    if patents_ops or patents_uspto:
        all_patents = _deduplicate_patents(patents_ops + patents_uspto)
        nb_critical = sum(1 for p in all_patents if p.get("score", 0) >= 8.0)
        top_score   = max((p.get("score", 0) for p in all_patents), default=0)
        st.divider()
        b1, b2, b3, b4 = st.columns(4)
        b1.metric("Brevets OPS",      len(patents_ops))
        b2.metric("Brevets USPTO",     len(patents_uspto))
        b3.metric("Brevets critiques", nb_critical)
        b4.metric("Top score brevet",  f"{top_score}/10")

    st.divider()

    tabs = st.tabs([
        "Activite", "Geographie", "Domaines",
        "Alertes", "Tendances", "Acteurs", "Brevets", "Rapport HTML",
    ])

    # ── TAB 0 : Activite ─────────────────────────────────────────────────────
    with tabs[0]:
        if metrics:
            st.plotly_chart(chart_activity(metrics, metrics_sigma), use_container_width=True)
            with st.expander("Donnees brutes metriques"):
                df_m      = pd.DataFrame(metrics)
                cols_show = [c for c in ["date", "alerte", "indice",
                             "nb_articles", "nb_pertinents"] if c in df_m.columns]
                st.dataframe(df_m[cols_show].rename(columns={
                    "date": "Date", "alerte": "Alerte", "indice": "Score",
                    "nb_articles": "Analysees", "nb_pertinents": "Pertinentes",
                }), use_container_width=True, height=280)
        else:
            st.info("Aucune metrique. Lancez sentinel_main.py.")

    # ── TAB 1 : Geographie ───────────────────────────────────────────────────
    with tabs[1]:
        if metrics:
            def _avg(col: str) -> float:
                vals = [float(m.get(col, 0) or 0) for m in metrics if m.get(col) is not None]
                return sum(vals) / max(len(vals), 1)
            geo_avg = {"USA": _avg("geousa"), "Europe": _avg("geoeurope"),
                       "Asie": _avg("geoasie"), "MO": _avg("geomo"), "Russie": _avg("georussie")}
            col_pie, col_evo = st.columns(2)
            with col_pie:
                st.plotly_chart(chart_geo_pie(geo_avg, f"Repartition geo -- moy. {len(metrics)}j"),
                                use_container_width=True)
            with col_evo:
                st.plotly_chart(chart_geo_evolution(metrics), use_container_width=True)
        elif html_text:
            geo = _regex_geo(html_text)
            if any(geo.values()):
                st.plotly_chart(chart_geo_pie(geo, "Repartition geo (mode degrade HTML)"),
                                use_container_width=True)
                st.caption("Mode degrade. Lancez le pipeline pour alimenter SQLite.")
            else:
                st.info("Donnees geographiques non trouvees.")
        else:
            st.info("Selectionnez un rapport HTML ou lancez le pipeline.")

    # ── TAB 2 : Domaines ─────────────────────────────────────────────────────
    with tabs[2]:
        if metrics:
            dom_avg = {
                "Terrestre":   sum(float(m.get("terrestre",   0) or 0) for m in metrics) / len(metrics),
                "Maritime":    sum(float(m.get("maritime",    0) or 0) for m in metrics) / len(metrics),
                "Transverse":  sum(float(m.get("transverse",  0) or 0) for m in metrics) / len(metrics),
                "Contractuel": sum(float(m.get("contractuel", 0) or 0) for m in metrics) / len(metrics),
            }
            col_b, col_r = st.columns(2)
            with col_b:
                st.plotly_chart(chart_domain_bar(dom_avg), use_container_width=True)
                st.caption(f"Moyenne sur {len(metrics)} jours")
            with col_r:
                st.plotly_chart(chart_domain_radar(dom_avg), use_container_width=True)
        elif html_text:
            domains = _regex_domains(html_text)
            if domains:
                st.plotly_chart(chart_domain_bar(domains), use_container_width=True)
                st.caption("Mode degrade : source HTML")
            else:
                st.info("Donnees domaines non disponibles.")
        else:
            st.info("Aucune donnee. Lancez le pipeline.")

    # ── TAB 3 : Alertes ──────────────────────────────────────────────────────
    with tabs[3]:
        if "confirm_close" not in st.session_state:
            st.session_state["confirm_close"] = {}

        col_hist, col_crud = st.columns([1.6, 1])
        with col_hist:
            if metrics:
                st.plotly_chart(chart_alert_timeline(metrics), use_container_width=True)
            else:
                st.info("Aucun historique d'alerte disponible.")

        with col_crud:
            st.subheader(f"Alertes ouvertes ({len(alertes)})")
            if alertes:
                for i, al in enumerate(alertes):
                    texte    = al.get("texte", str(al))
                    date_ouv = al.get("dateouverture", "")
                    c_txt, c_btn = st.columns([5, 1])
                    c_txt.markdown(
                        f"**{texte}**" + (f"<br><small>{date_ouv[:10]}</small>" if date_ouv else ""),
                        unsafe_allow_html=True,
                    )
                    btn_key     = f"close_{i}"
                    confirm_key = f"confirm_{i}"
                    if st.session_state["confirm_close"].get(confirm_key):
                        if c_btn.button("?", key=btn_key, help="Confirmer la cloture"):
                            if _DB_AVAILABLE:
                                try:
                                    SentinelDB.closealerte(
                                        texte,
                                        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                                    )
                                    st.session_state["confirm_close"][confirm_key] = False
                                    st.cache_data.clear()
                                    st.toast("Alerte cloturee.")
                                    time.sleep(0.8)
                                    st.rerun()
                                except AttributeError:
                                    st.error("SentinelDB.closealerte() introuvable.")
                    else:
                        if c_btn.button("OK", key=btn_key, help="Cloturer (2x pour confirmer)"):
                            st.session_state["confirm_close"][confirm_key] = True
                            st.rerun()
                    st.markdown("---")
            else:
                st.success("Aucune alerte ouverte")

            st.divider()
            st.subheader("Ouvrir une alerte")
            with st.form("form_alerte", clear_on_submit=True):
                new_al = st.text_area("Texte", height=80,
                                      placeholder="ex: Montee en puissance USV Europe")
                if st.form_submit_button("Creer", use_container_width=True) and new_al.strip():
                    if _DB_AVAILABLE:
                        try:
                            SentinelDB.ouvrirealerte(
                                new_al.strip(),
                                datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            )
                            st.cache_data.clear()
                            st.toast("Alerte creee.")
                            time.sleep(0.8)
                            st.rerun()
                        except AttributeError:
                            st.error("SentinelDB.ouvrirealerte() introuvable.")
                    else:
                        st.error("db_manager non disponible.")

    # ── TAB 4 : Tendances ────────────────────────────────────────────────────
    with tabs[4]:
        nb_raw     = len(tendances_raw)
        nb_cluster = len(tendances)
        nb_fus     = nb_raw - nb_cluster
        st.subheader(f"Tendances ({nb_cluster} themes"
                     + (f" -- {nb_fus} variantes fusionnees" if nb_fus > 0 else "") + ")")

        if tendances:
            col_search, col_toggle = st.columns([4, 1])
            search_t    = col_search.text_input("Filtrer", placeholder="UGV, maritime, IA...",
                                                key="search_tend")
            show_detail = col_toggle.checkbox("Detail variantes", value=False)

            filt_t = [
                t for t in tendances
                if not search_t
                or search_t.lower() in t["texte"].lower()
                or any(search_t.lower() in v.lower() for v in t.get("variantes", []))
            ]

            pills_html = " ".join(
                f'<span class="pill">{t["texte"]}'
                f'<span class="pill-count">{t["count"]}</span></span>'
                for t in filt_t
            )
            st.markdown(pills_html, unsafe_allow_html=True)
            st.divider()

            with st.expander("Tableau detaille", expanded=False):
                rows_t = [{
                    "Theme canonique":      t["texte"],
                    "Occurrences cumulees": t["count"],
                    "Variantes fusionnees": (
                        ", ".join(t.get("variantes", [])) or "---"
                        if show_detail
                        else f"{len(t.get('variantes', []))} variante(s)"
                    ),
                    "Premiere detection": (t.get("datepremiere") or "")[:10],
                    "Derniere detection": (t.get("datederniere") or "")[:10],
                } for t in filt_t]
                st.dataframe(pd.DataFrame(rows_t), use_container_width=True, height=350)
        else:
            st.info("Aucune tendance. Lancez sentinel_main.py.")

    # ── TAB 5 : Acteurs ──────────────────────────────────────────────────────
    with tabs[5]:
        st.subheader(f"Acteurs surveilles ({len(acteurs)})")
        if acteurs:
            df_act = pd.DataFrame(acteurs).rename(columns={
                "nom": "Acteur", "pays": "Pays",
                "scoreactivite": "Score", "derniereactivite": "Derniere activite",
            })
            f1, f2 = st.columns(2)
            pays_all  = sorted({r.get("pays") or "" for r in acteurs if r.get("pays")})
            pays_sel  = f1.multiselect("Filtrer par pays", pays_all)
            score_min = f2.slider("Score minimum", 0.0, 10.0, 0.0, 0.5)
            df_f = df_act.copy()
            if pays_sel:
                df_f = df_f[df_f["Pays"].isin(pays_sel)]
            if score_min > 0:
                df_f = df_f[df_f["Score"] >= score_min]
            st.dataframe(df_f, use_container_width=True, height=340)

            top15 = df_f.nlargest(15, "Score") if len(df_f) >= 2 else df_f
            if not top15.empty:
                fig_a = go.Figure(go.Bar(
                    x=top15["Score"], y=top15["Acteur"], orientation="h",
                    marker_color="#e74c3c", text=top15["Score"].round(1), textposition="outside",
                ))
                fig_a.update_layout(title="Top 15 acteurs", xaxis_title="Score",
                                    yaxis_autorange="reversed",
                                    margin=dict(t=60, l=200, b=40), height=480)
                st.plotly_chart(fig_a, use_container_width=True)

            st.subheader("Evolution des scores acteurs")
            if _DB_AVAILABLE:
                try:
                    evol_data = SentinelDB.getacteursevolution(n_days=30)
                    if evol_data:
                        df_evol  = pd.DataFrame(evol_data)
                        fig_evol = go.Figure()
                        for actor in df_evol["nom"].unique():
                            sub = df_evol[df_evol["nom"] == actor]
                            fig_evol.add_trace(go.Scatter(
                                x=sub["date"], y=sub["score"],
                                mode="lines+markers", name=actor[:25],
                                line=dict(width=1.5), marker=dict(size=4),
                            ))
                        fig_evol.update_layout(xaxis_title="Date", yaxis_title="Score",
                                               height=320,
                                               legend=dict(orientation="h", y=-0.25),
                                               margin=dict(t=40, b=100))
                        st.plotly_chart(fig_evol, use_container_width=True)
                    else:
                        st.info("SentinelDB.getacteursevolution() ne retourne aucune donnee. "
                                "Implementer cette methode dans db_manager.py.")
                except AttributeError:
                    st.info("SentinelDB.getacteursevolution() absent de db_manager.py.")
                except Exception as e:
                    st.caption(f"Evolution non disponible : {e}")
            else:
                st.info("db_manager requis pour l'evolution des acteurs.")
        else:
            st.info("Aucun acteur en base. Lancez sentinel_main.py.")

        with st.expander("Ajouter / mettre a jour un acteur manuellement"):
            with st.form("form_acteur", clear_on_submit=True):
                a1, a2, a3 = st.columns(3)
                nom_a   = a1.text_input("Nom",  placeholder="Milrem Robotics")
                pays_a  = a2.text_input("Pays", placeholder="EST")
                score_a = a3.slider("Score", 0.0, 10.0, 5.0, 0.5)
                if st.form_submit_button("Enregistrer", use_container_width=True) and nom_a.strip():
                    if len(nom_a.strip()) > 100:
                        st.error("Nom trop long (max 100 caracteres).")
                    elif _DB_AVAILABLE:
                        try:
                            SentinelDB.saveacteur(
                                nom_a.strip(), pays_a.strip(), score_a,
                                datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            )
                            st.cache_data.clear()
                            st.toast(f"Acteur {nom_a} enregistre.")
                            time.sleep(0.8)
                            st.rerun()
                        except AttributeError:
                            st.error("SentinelDB.saveacteur() introuvable.")
                    else:
                        st.error("db_manager non disponible.")

    # ── TAB 6 : Brevets ──────────────────────────────────────────────────────
    with tabs[6]:
        st.subheader("Surveillance brevets -- Espacenet OPS + USPTO PatentsView")
        src_choice      = st.radio("Source", ["Espacenet OPS", "USPTO PatentsView"],
                                   horizontal=True, key="patent_src")
        patents_display = patents_ops if "OPS" in src_choice else patents_uspto

        if not patents_display:
            src_name = "patents_latest.json" if "OPS" in src_choice else "patents_uspto_latest.json"
            st.info(f"Aucun brevet ({src_name} absent). Lancez ops_patents.py.")
        else:
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            only_critical = col_f1.checkbox("Critiques seulement (>=8.0)", value=True)
            cpc_all       = sorted({p.get("cpc", "") for p in patents_display if p.get("cpc")})
            cpc_sel       = col_f2.multiselect("CPC", cpc_all, key="pat_cpc")
            score_min_p   = col_f3.slider("Score min.", 1.0, 10.0,
                                          8.0 if only_critical else 1.0, 0.5, key="pat_score")
            search_p      = col_f4.text_input("Titre / deposant", key="pat_search")

            filtered = patents_display
            if only_critical:
                filtered = [p for p in filtered if p.get("score", 0) >= 8.0]
            if cpc_sel:
                filtered = [p for p in filtered if p.get("cpc") in cpc_sel]
            if score_min_p > 1.0:
                filtered = [p for p in filtered if p.get("score", 0) >= score_min_p]
            if search_p:
                q        = search_p.lower()
                filtered = [p for p in filtered
                            if q in (p.get("title") or "").lower()
                            or any(q in a.lower() for a in p.get("assignees", []))]

            st.caption(f"{len(filtered)} brevet(s) affiche(s) sur {len(patents_display)} collectes")

            if filtered:
                viz_bar, viz_tree = st.tabs(["Bar chart top 15", "Treemap par Tier"])
                with viz_bar:
                    st.plotly_chart(chart_patents_bar(filtered, f"Top brevets -- {src_choice}"),
                                    use_container_width=True)
                with viz_tree:
                    st.plotly_chart(chart_patents_treemap(filtered,
                                                          f"Cartographie -- {src_choice}"),
                                    use_container_width=True)
                    st.caption("Taille = score SENTINEL | Couleur = intensite | "
                               "Hierarchie : Tier > Deposant > CPC")

            with st.expander("Tableau detaille", expanded=len(filtered) <= 20):
                rows = []
                for p in sorted(filtered, key=lambda x: x.get("score", 0), reverse=True):
                    tier  = _patent_tier(p)
                    goog  = p.get("url", "")
                    pdf   = p.get("pdf_url", "")
                    liens = ("" if not goog else f"[Google]({goog})") + \
                            ("" if not pdf  else f"  [PDF]({pdf})")
                    rows.append({
                        "Score":     p.get("score", 0),
                        "Tier":      tier,
                        "CPC":       p.get("cpc", ""),
                        "Titre":     (p.get("title") or "")[:90],
                        "Deposants": ", ".join(p.get("assignees", [])) or "N/A",
                        "Pays":      ", ".join(p.get("countries", [])) or "N/A",
                        "Date":      p.get("date", ""),
                        "Liens":     liens,
                    })
                if rows:
                    st.dataframe(
                        pd.DataFrame(rows),
                        use_container_width=True,
                        height=min(600, len(rows) * 38 + 40),
                        column_config={"Liens": st.column_config.LinkColumn(
                            "Liens", display_text="lien")},
                    )

            with st.expander("Historique 90 jours (OPS uniquement)"):
                history = load_patents_history()
                if history:
                    df_hist = pd.DataFrame([{
                        "Date":      p.get("date", ""),
                        "CPC":       p.get("cpc", ""),
                        "Score":     p.get("score", 0),
                        "Titre":     (p.get("title") or "")[:80],
                        "Deposants": ", ".join(p.get("assignees", [])) or "N/A",
                    } for p in sorted(history, key=lambda x: x.get("date", ""), reverse=True)])
                    st.dataframe(df_hist, use_container_width=True, height=350)
                else:
                    st.info("Aucun historique (patents_history.json absent).")

    # ── TAB 7 : Rapport HTML ─────────────────────────────────────────────────
    with tabs[7]:
        if selected_date and html_text:
            st.subheader(f"Rapport du {selected_date}")
            day_m = metrics_for_kpi[0] if metrics_for_kpi else None
            if day_m:
                dm1, dm2, dm3, dm4 = st.columns(4)
                dm1.metric("Alerte",      day_m.get("alerte", "N/A"))
                dm2.metric("Indice",      f"{float(day_m.get('indice', 0)):.1f}/10")
                dm3.metric("Analysees",   day_m.get("nb_articles",   "N/A"))
                dm4.metric("Pertinentes", day_m.get("nb_pertinents", "N/A"))
                st.divider()

            for d, p in html_reports:
                if d == selected_date:
                    try:
                        html_content = Path(p).read_text(encoding="utf-8", errors="ignore")
                        html_content = re.sub(
                            "<script[^>]*>.*?</script>", "",
                            html_content, flags=re.DOTALL | re.IGNORECASE,
                        )
                        if len(html_content) > 500_000:
                            st.warning(f"Rapport volumineux ({len(html_content)//1024} KB) "
                                       "-- tronque a 500 KB.")
                            html_content = html_content[:500_000]
                        st.components.v1.html(html_content, height=900, scrolling=True)
                    except Exception as exc:
                        st.error(f"Impossible de charger le rapport : {exc}")
                    break
        elif html_reports:
            st.info("Selectionnez un rapport dans la barre laterale.")
        else:
            st.warning("Aucun rapport HTML trouve dans output/SENTINEL_*.html.")


if __name__ == "__main__":
    main()
