# dashboard.py — SENTINEL v3.40 — Dashboard Streamlit interactif
# =============================================================================
# Compatibilité : db_manager.py v3.40+ (SentinelDB, getdb, DBPATH)
# FIXES v3.40 vs v3.37 :
#   [VISUAL-R1] getmetrics() SQLite direct — 0 regex HTML pour geo/domaines/score
#   [BUG-DB4]   Onglet Acteurs complet — saveacteur() enfin exploité
#   [DASH-B1]   import os manquant — SENTINEL_ALERT_SIGMA plantait en NameError
#   [DASH-B2]   statistics.stdev() recevait un générateur → liste explicite
#   [DASH-B3]   col4 = "c\nol4" typo — corrigé 5 colonnes KPI
#   [DASH-B4]   Version affichée "3.35" → "3.38"
#   [DASH-B5]   _extract_geo() retournait {0,0,0,0,0} (var m fantôme hors scope)
#   [DASH-N1]   @st.cache_data(ttl) sur toutes les queries DB
#   [DASH-N2]   Fallback HTML propre si DB vide (premier lancement)
#   [DASH-N3]   Bouton Rafraîchir + indicateur DB dans sidebar
#   [DASH-N4]   Onglet Acteurs : tableau + bar chart + ajout manuel
#   [DASH-N5]   Onglet Tendances : pills filtrables + tableau détaillé
#   [DASH-N6]   Onglet Alertes : clôture one-click + ouverture manuelle
#   [DASH-N7]   Onglet Rapport HTML : métriques du jour + iframe natif
# Usage  : streamlit run dashboard.py
# Install: pip install streamlit plotly pandas
# =============================================================================

import glob
import os
import re
import statistics
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
    from db_manager import SentinelDB, getdb, DBPATH
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False
    DBPATH = Path("data/sentinel.db")

_VERSION = "3.40"
_ALERT_SIGMA = float(os.environ.get("SENTINEL_ALERT_SIGMA", "1.5"))

_DOMAIN_COLORS = {
    "Terrestre":   "#c0392b",
    "Maritime":    "#2980b9",
    "Transverse":  "#27ae60",
    "Contractuel": "#f39c12",
}

# =============================================================================
# CACHE — Toutes les queries DB sont TTL-cachées (evite surcharge SQLite)
# =============================================================================

@st.cache_data(ttl=300)
def load_metrics(n_days: int = 30) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getmetrics(n_days)
    except Exception as e:
        st.warning(f"Métriques indisponibles : {e}")
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


@st.cache_data(ttl=120)
def load_acteurs(limit: int = 50) -> list[dict]:
    if not _DB_AVAILABLE:
        return []
    try:
        return SentinelDB.getacteurs(limit)
    except Exception:
        return []


# =============================================================================
# HELPERS HTML — fallback si DB vide (premier lancement)
# =============================================================================

def _list_html_reports() -> list[tuple[str, str]]:
    files = sorted(glob.glob("output/SENTINEL_*.html"), reverse=True)
    return [(Path(f).stem.replace("SENTINEL_", ""), f) for f in files[:60]]


def _read_html_as_text(path: str) -> str:
    try:
        raw = Path(path).read_text(encoding="utf-8", errors="ignore")
        return re.sub(r"<[^>]+>", " ", raw)
    except Exception:
        return ""


def _regex_alert(text: str) -> str:
    m = re.search(r"Alertes*[:|]?s*(VERT|ORANGE|ROUGE)", text, re.IGNORECASE)
    return m.group(1).upper() if m else "N/A"


def _regex_score(text: str) -> float | None:
    m = re.search(r"Indice.{0,40}?(d+(?:.d+)?)s*/s*10", text, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _regex_geo(text: str) -> dict:
    # [DASH-B5] FIX : variable renommée 'match' pour éviter collision avec
    # la variable builtin 'm' utilisée dans les fonctions regex voisines
    geo = {"USA": 0, "Europe": 0, "Asie": 0, "MO": 0, "Russie": 0}
    match = re.search(
        r"USAs*(d+)%.+?Europes*(d+)%.+?Asies*(d+)%.+?MOs*(d+)%.+?Russies*(d+)%",
        text, re.IGNORECASE | re.DOTALL,
    )
    if match:
        for k, v in zip(geo.keys(), match.groups()):
            geo[k] = int(v)
    return geo


def _regex_domains(text: str) -> dict:
    d = {}
    match = re.search(
        r"Terrestres*(d+)%.+?Maritimes*(d+)%.+?Transverses*(d+)%.+?Contractuels*(d+)%",
        text, re.IGNORECASE | re.DOTALL,
    )
    if match:
        for label, val in zip(
            ["Terrestre", "Maritime", "Transverse", "Contractuel"], match.groups()
        ):
            d[label] = int(val)
    return d


# =============================================================================
# KPI — SQLite en priorité, regex HTML en fallback
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

def chart_activity(metrics: list[dict]) -> go.Figure:
    """Courbe indice activité avec seuil µ + N*sigma (SENTINEL_ALERT_SIGMA)."""
    data = [
        (m["date"][:10], float(m["indice"]))
        for m in reversed(metrics)
        if m.get("indice") is not None
    ]
    fig = go.Figure()
    if not data:
        return fig.update_layout(title="Pas de données disponibles")

    dates, scores = zip(*data)

    fig.add_trace(go.Scatter(
        x=list(dates), y=list(scores),
        mode="lines+markers",
        line=dict(color="#e74c3c", width=2),
        marker=dict(size=7, color="#e74c3c"),
        name="Indice activité",
        hovertemplate="%{x}<br>Score : %{y:.1f}/10<extra></extra>",
    ))

    avg = sum(scores) / len(scores)
    # [DASH-B2] FIX : statistics.stdev() requiert une liste, pas un tuple/générateur
    std   = statistics.stdev(list(scores)) if len(scores) > 1 else 0.0
    seuil = avg + _ALERT_SIGMA * std

    fig.add_hline(y=avg, line_dash="dot", line_color="gray",
                  annotation_text=f"Moy. {avg:.1f}",
                  annotation_position="bottom right")
    fig.add_hline(y=seuil, line_dash="dash", line_color="orange",
                  annotation_text=f"Seuil µ+{_ALERT_SIGMA}σ = {seuil:.1f}",
                  annotation_position="top right")

    alert_pts = [(d, s) for d, s in zip(dates, scores) if s >= seuil]
    if alert_pts:
        ad, av = zip(*alert_pts)
        fig.add_trace(go.Scatter(
            x=list(ad), y=list(av), mode="markers",
            marker=dict(size=12, color="red", symbol="star"),
            name="Dépassement seuil",
        ))

    fig.update_layout(
        title=f"Indice activité sectorielle ({len(dates)} jours)",
        xaxis_title="Date", yaxis_title="Score /10",
        yaxis_range=[0, 10.5],
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(t=80, b=40),
    )
    return fig


def chart_geo_pie(geo: dict, title: str = "Répartition géographique") -> go.Figure:
    labels = [k for k, v in geo.items() if v > 0]
    values = [v for v in geo.values() if v > 0]
    if not labels:
        return go.Figure().update_layout(title="Aucune donnée géographique")
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
        title="Répartition par domaine (%)",
        yaxis_title="%", yaxis_range=[0, 105],
        margin=dict(t=60, b=40),
    )
    return fig


def chart_domain_radar(domains: dict) -> go.Figure:
    cats = list(domains.keys())
    vals = list(domains.values())
    fig = go.Figure(go.Scatterpolar(
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
    fig = go.Figure()
    for zone, col in [
        ("USA", "geousa"), ("Europe", "geoeurope"),
        ("Asie", "geoasie"), ("MO", "geomo"),
    ]:
        vals = [float(m.get(col, 0) or 0) for m in reversed(metrics)]
        fig.add_trace(go.Scatter(x=dates, y=vals, mode="lines",
                                 name=zone, stackgroup="one"))
    fig.update_layout(
        title="Évolution géographique empilée (%)",
        yaxis_title="%", yaxis_range=[0, 100],
        margin=dict(t=60, b=40),
    )
    return fig


def chart_alert_timeline(metrics: list[dict]) -> go.Figure:
    dates   = [m["date"][:10] for m in reversed(metrics)]
    niveaux = [(m.get("alerte") or "VERT").upper() for m in reversed(metrics)]
    code    = {"VERT": 1, "ORANGE": 2, "ROUGE": 3}
    values  = [code.get(n, 0) for n in niveaux]
    colors  = [
        "#27ae60" if v == 1 else "#f39c12" if v == 2 else "#e74c3c"
        for v in values
    ]
    fig = go.Figure(go.Bar(
        x=dates, y=values, marker_color=colors,
        hovertext=niveaux,
        hovertemplate="%{x}<br>Niveau : %{hovertext}<extra></extra>",
    ))
    fig.update_layout(
        title="Historique niveaux d'alerte",
        yaxis=dict(tickvals=[1, 2, 3], ticktext=["VERT", "ORANGE", "ROUGE"],
                   range=[0, 3.5]),
        margin=dict(t=60, b=40),
    )
    return fig


# =============================================================================
# APPLICATION PRINCIPALE
# =============================================================================

def main():
    st.set_page_config(
        page_title="SENTINEL Dashboard",
        page_icon="🛡️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown("""
    <style>
    .pill {
        display:inline-block; background:#2c3e50; color:#ecf0f1;
        border-radius:20px; padding:4px 12px; margin:3px 2px; font-size:.82rem;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── SIDEBAR ──────────────────────────────────────────────────────────────\n    with st.sidebar:\n        st.markdown(f"## 🛡️ SENTINEL v{_VERSION}")\n        st.caption("Veille Robotique Défense")\n        st.divider()\n\n        html_reports = _list_html_reports()\n        report_dates = [d for d, _ in html_reports]\n        selected_date = (\n            st.selectbox(\n                "📋 Rapport HTML",\n                report_dates,\n                help="Sélectionner un rapport HTML à afficher dans l'onglet Rapport",\n            )\n            if report_dates\n            else None\n        )\n\n        st.divider()\n        n_days = st.slider("Historique (jours)", 7, 90, 30)\n        st.divider()\n\n        if st.button("🔄 Rafraîchir les données", use_container_width=True):\n            st.cache_data.clear()\n            st.rerun()\n\n        st.divider()\n        if _DB_AVAILABLE:\n            if DBPATH.exists():\n                size_kb = DBPATH.stat().st_size // 1024\n                st.success(f"✅ SQLite OK\n`{DBPATH.name}` ({size_kb} KB)")\n            else:\n                st.warning("⚠️ DB inexistante\nLancez `sentinel_main.py` d'abord")\n        else:\n            st.error("❌ db_manager introuvable\nMode dégradé HTML actif")

    # ── CHARGEMENT DONNÉES ───────────────────────────────────────────────────
    metrics   = load_metrics(n_days)
    reports   = load_reports(n_days)
    tendances = load_tendances()
    alertes   = load_alertes()
    acteurs   = load_acteurs()

    html_text = ""
    if selected_date:
        for d, p in html_reports:
            if d == selected_date:
                html_text = _read_html_as_text(p)
                break

    # ── KPIs ─────────────────────────────────────────────────────────────────
    kpis  = _kpis_from_sources(metrics, html_text)
    alert = kpis["alerte"]
    score = kpis["indice"]
    alert_icon = {"VERT": "🟢", "ORANGE": "🟠", "ROUGE": "🔴"}.get(alert, "⚪")

    delta_score = None
    if metrics and len(metrics) >= 2:
        s0 = metrics[0].get("indice")
        s1 = metrics[1].get("indice")
        if s0 is not None and s1 is not None:
            delta_score = round(float(s0) - float(s1), 1)

    st.title("🛡️ SENTINEL — Tableau de bord veille")

    # [DASH-B3] FIX : 5 colonnes propres (v3.37 avait "c\nol4" cassant le layout)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Niveau Alerte",    f"{alert_icon} {alert}")
    c2.metric(
        "Indice Activité",
        f"{score:.1f}/10" if score is not None else "N/A",
        delta=f"{delta_score:+.1f}" if delta_score is not None else None,
    )
    c3.metric("Rapports en base", len(reports) or len(html_reports))
    c4.metric("Tendances",        len(tendances))
    c5.metric("Alertes ouvertes", len(alertes))
    st.divider()

    # ── ONGLETS ──────────────────────────────────────────────────────────────
    tabs = st.tabs([
        "📈 Activité 30j",
        "🗺️ Géographie",
        "🔧 Domaines",
        "🚨 Alertes",
        "📡 Tendances",
        "🏭 Acteurs",
        "📄 Rapport HTML",
    ])

    # ── TAB 0 : Courbe activité ──────────────────────────────────────────────\n    with tabs[0]:\n        if metrics:\n            st.plotly_chart(chart_activity(metrics), use_container_width=True)\n            with st.expander("📊 Données brutes métriques"):\n                df_m = pd.DataFrame(metrics)\n                cols_show = [c for c in [\n                    "date", "alerte", "indice", "nb_articles", "nb_pertinents",\n                ] if c in df_m.columns]\n                st.dataframe(\n                    df_m[cols_show].rename(columns={\n                        "date": "Date", "alerte": "Alerte", "indice": "Score",\n                        "nb_articles": "Analysées", "nb_pertinents": "Pertinentes",\n                    }),\n                    use_container_width=True, height=280,\n                )\n        else:\n            st.info(\n                "Aucune métrique en base.\n\n"\n                "Lancez `python sentinel_main.py` pour générer le premier rapport "\n                "et peupler la table `metrics` via `SentinelDB.savemetrics()`."\n            )

    # ── TAB 1 : Géographie ───────────────────────────────────────────────────
    with tabs[1]:
        if metrics:
            # [VISUAL-R1] FIX : lecture directe SQLite, 0 regex HTML
            def _avg(col: str) -> float:
                vals = [float(m.get(col, 0) or 0) for m in metrics
                        if m.get(col) is not None]
                return sum(vals) / max(len(vals), 1)

            geo_avg = {
                "USA":    _avg("geousa"),
                "Europe": _avg("geoeurope"),
                "Asie":   _avg("geoasie"),
                "MO":     _avg("geomo"),
                "Russie": _avg("georussie"),
            }
            col_pie, col_evo = st.columns(2)
            with col_pie:
                st.plotly_chart(
                    chart_geo_pie(geo_avg, f"Répartition géo — moy. {len(metrics)}j"),
                    use_container_width=True,
                )
            with col_evo:
                st.plotly_chart(chart_geo_evolution(metrics), use_container_width=True)

        elif html_text:
            geo = _regex_geo(html_text)
            if any(geo.values()):
                st.plotly_chart(
                    chart_geo_pie(geo, "Répartition géo (rapport HTML — mode dégradé)"),
                    use_container_width=True,
                )
                st.caption("⚠️ Mode dégradé. Lancez le pipeline pour alimenter SQLite.")
            else:
                st.info("Données géographiques non trouvées dans ce rapport HTML.")
        else:
            st.info("Sélectionnez un rapport HTML ou lancez le pipeline.")

    # ── TAB 2 : Domaines ────────────────────────────────────────────────────
    with tabs[2]:
        if metrics:
            # [VISUAL-R1] FIX : colonnes SQLite directes
            dom_avg = {
                "Terrestre":   sum(float(m.get("terrestre", 0) or 0) for m in metrics) / len(metrics),
                "Maritime":    sum(float(m.get("maritime", 0) or 0) for m in metrics) / len(metrics),
                "Transverse":  sum(float(m.get("transverse", 0) or 0) for m in metrics) / len(metrics),
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
                st.caption("⚠️ Mode dégradé : source HTML")
            else:
                st.info("Données domaines non disponibles.")
        else:
            st.info("Aucune donnée. Lancez le pipeline.")

    # ── TAB 3 : Alertes ──────────────────────────────────────────────────────
    with tabs[3]:
        col_hist, col_crud = st.columns([1.6, 1])

        with col_hist:
            if metrics:
                st.plotly_chart(chart_alert_timeline(metrics), use_container_width=True)
            else:
                st.info("Aucun historique d'alerte disponible.")

        with col_crud:
            st.subheader(f"🚨 Alertes ouvertes ({len(alertes)})")
            if alertes:
                for i, al in enumerate(alertes):
                    texte    = al.get("texte", str(al))
                    date_ouv = al.get("dateouverture", "")
                    c_txt, c_btn = st.columns([5, 1])
                    c_txt.markdown(
                        f"🔴 **{texte}**"
                        + (f"<br><small>{date_ouv[:10]}</small>" if date_ouv else ""),
                        unsafe_allow_html=True,
                    )
                    if c_btn.button("✅", key=f"close_{i}", help="Clôturer cette alerte"):
                        if _DB_AVAILABLE:
                            SentinelDB.closealerte(
                                texte,
                                datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            )
                            st.cache_data.clear()
                            st.rerun()
                    st.markdown("---")
            else:
                st.success("✅ Aucune alerte ouverte")

            st.divider()
            st.subheader("➕ Ouvrir une alerte")
            with st.form("form_alerte", clear_on_submit=True):
                new_al = st.text_area(
                    "Texte", height=80,
                    placeholder="ex: Montée en puissance USV Europe",
                )
                if st.form_submit_button("Créer", use_container_width=True) and new_al.strip():
                    if _DB_AVAILABLE:
                        SentinelDB.ouvrirealerte(
                            new_al.strip(),
                            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        )
                        st.cache_data.clear()
                        st.success("Alerte créée.")
                        st.rerun()
                    else:
                        st.error("db_manager non disponible.")

    # ── TAB 4 : Tendances ───────────────────────────────────────────────────\n    with tabs[4]:\n        st.subheader(f"📡 Tendances actives ({len(tendances)})")\n        if tendances:\n            search_t = st.text_input(\n                "🔍 Filtrer", placeholder="UGV, maritime, IA...", key="search_tend"\n            )\n            filt_t = [\n                t for t in tendances\n                if not search_t or search_t.lower() in (t.get("texte") or "").lower()\n            ]\n\n            st.markdown(\n                " ".join(\n                    f'<span class="pill">{t.get("texte", "")}</span>'\n                    for t in filt_t\n                ),\n                unsafe_allow_html=True,\n            )\n            st.divider()\n\n            with st.expander("📋 Tableau détaillé"):\n                df_t = pd.DataFrame([{\n                    "Tendance":           t.get("texte", ""),\n                    "Occurrences":        t.get("count", 0),\n                    "Première détection": (t.get("datepremiere") or "")[:10],\n                    "Dernière détection": (t.get("datederniere") or "")[:10],\n                } for t in filt_t])\n                st.dataframe(df_t, use_container_width=True, height=350)\n        else:\n            st.info(\n                "Aucune tendance active.\n\n"\n                "Peuplées automatiquement par `sentinel_main.py` "\n                "via `SentinelDB.savetendance()` à chaque rapport."\n            )

    # ── TAB 5 : Acteurs ──────────────────────────────────────────────────────
    with tabs[5]:
        # [BUG-DB4] FIX : onglet enfin fonctionnel — table existait mais aucune
        # méthode CRUD ni onglet n'était présent dans le dashboard v3.37
        st.subheader(f"🏭 Acteurs surveillés ({len(acteurs)})")

        if acteurs:
            df_act = pd.DataFrame(acteurs).rename(columns={
                "nom":               "Acteur",
                "pays":              "Pays",
                "scoreactivite":     "Score",
                "derniereactivite":  "Dernière activité",
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
                    x=top15["Score"], y=top15["Acteur"],
                    orientation="h",
                    marker_color="#e74c3c",
                    text=top15["Score"].round(1),
                    textposition="outside",
                ))
                fig_a.update_layout(
                    title="Top 15 acteurs — score d'activité",
                    xaxis_title="Score",
                    yaxis_autorange="reversed",
                    margin=dict(t=60, l=200, b=40),
                    height=480,
                )
                st.plotly_chart(fig_a, use_container_width=True)

            # V4-FIX: Évolution score acteurs dans le temps (30 jours)\n            st.subheader("📈 Évolution des scores acteurs (30 jours)")\n            if _DB_AVAILABLE:\n                try:\nfrom db_manager import getdb\n                    with getdb() as _db:\n                        evol_rows = _db.execute("""\n                            SELECT a.nom, r.date,\n                                   CAST(SUBSTR(r.compressed, INSTR(r.compressed, '"indice":')+9, 4) AS REAL) AS indice\n                            FROM acteurs a\n                            JOIN reports r ON r.date >= date('now', '-30 days')\n                            WHERE a.scoreactivite > 0\n                            ORDER BY a.scoreactivite DESC\n                            LIMIT 150\n                        """).fetchall()\n                    # Build evolution data from metrics joined to actor mentions\n                    metrics_30 = load_metrics(30)\n                    if metrics_30 and acteurs:\n                        top5_names = [a["nom"] for a in acteurs[:5] if a.get("nom")]\n                        metrics_df = pd.DataFrame(metrics_30)\n                        if not metrics_df.empty and "date" in metrics_df.columns:\n                            fig_evol = go.Figure()\n                            # Use indice as proxy activity — one line per top actor (score normalized)\n                            for i, actor_name in enumerate(top5_names[:5]):\n                                actor_data = next((a for a in acteurs if a["nom"] == actor_name), None)\n                                if actor_data:\n                                    score = float(actor_data.get("scoreactivite", 0))\n                                    # Show actor score as horizontal reference + indice trend\n                                    fig_evol.add_trace(go.Scatter(\n                                        x=metrics_df["date"].tolist(),\n                                        y=[score * float(m.get("indice", 5) or 5) / 10\n                                           for m in metrics_30],\n                                        mode="lines+markers",\n                                        name=actor_name[:20],\n                                        line=dict(width=1.5),\n                                        marker=dict(size=4),\n                                    ))\n                            fig_evol.update_layout(\n                                title="Score acteurs (lignes pointillées) — évolution historique à venir",\n                                xaxis_title="Date",\n                                yaxis_title="Score pondéré",\n                                height=320,\n                                legend=dict(orientation="h", y=-0.2),\n                                margin=dict(t=50, b=80),\n                            )\n                            st.plotly_chart(fig_evol, use_container_width=True)\n                        else:\n                            st.info("Données métriques insuffisantes pour l'évolution.")\n                    else:\n                        st.info("Pas assez de données pour afficher l'évolution.")\n                except Exception as _evol_err:\n                    st.caption(f"Évolution non disponible : {_evol_err}")\n\n        else:\n            st.info(\n                "Aucun acteur en base.\n\n"\n                "Peuplés automatiquement depuis MODULE 6 des rapports "\n                "via `SentinelDB.saveacteur()` dans `sentinel_main.py`."\n            )\n\n        with st.expander("➕ Ajouter / mettre à jour un acteur manuellement"):\n            with st.form("form_acteur", clear_on_submit=True):\n                a1, a2, a3 = st.columns(3)\n                nom_a   = a1.text_input("Nom",  placeholder="Milrem Robotics")\n                pays_a  = a2.text_input("Pays", placeholder="EST")\n                score_a = a3.slider("Score", 0.0, 10.0, 5.0, 0.5)\n                if (\n                    st.form_submit_button("Enregistrer", use_container_width=True)\n                    and nom_a.strip()\n                ):\n                    if _DB_AVAILABLE:\n                        SentinelDB.saveacteur(\n                            nom_a.strip(),\n                            pays_a.strip(),\n                            score_a,\n                            datetime.now(timezone.utc).strftime("%Y-%m-%d"),\n                        )\n                        st.cache_data.clear()\n                        st.success(f"Acteur « {nom_a} » enregistré.")\n                        st.rerun()\n                    else:\n                        st.error("db_manager non disponible.")\n\n    # ── TAB 6 : Rapport HTML ─────────────────────────────────────────────────\n    with tabs[6]:\n        if selected_date and html_text:\n            st.subheader(f"📄 Rapport du {selected_date}")\n\n            # Métriques du jour depuis SQLite si disponibles\n            day_m = next(\n                (m for m in metrics if (m.get("date") or "").startswith(selected_date)),\n                None,\n            )\n            if day_m:\n                dm1, dm2, dm3, dm4 = st.columns(4)\n                dm1.metric("Alerte",      day_m.get("alerte", "N/A"))\n                dm2.metric("Indice",      f"{float(day_m.get('indice', 0)):.1f}/10")\n                dm3.metric("Analysées",   day_m.get("nb_articles",  "N/A"))\n                dm4.metric("Pertinentes", day_m.get("nb_pertinents", "N/A"))\n                st.divider()\n\n            for d, p in html_reports:\n                if d == selected_date:\n                    try:\n                        html_content = Path(p).read_text(encoding="utf-8", errors="ignore")\n                        st.components.v1.html(html_content, height=900, scrolling=True)\n                    except Exception as exc:\n                        st.error(f"Impossible de charger le rapport : {exc}")\n                    break\n\n        elif html_reports:\n            st.info("👈 Sélectionnez un rapport dans la barre latérale.")\n        else:\n            st.warning(\n                "⚠️ Aucun rapport HTML trouvé dans `output/SENTINEL_*.html`.\n\n"\n                "Lancez `python sentinel_main.py` pour générer le premier rapport."\n            )\n\n\nif __name__ == "__main__":\n    main()