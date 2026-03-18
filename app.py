import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import requests
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings("ignore")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="San José Budget & Compensation Intelligence",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# DESIGN SYSTEM — Warm financial dark theme
# ============================================================
C = {
    "bg": "#09090b",
    "surface": "#18181b",
    "border": "#27272a",
    "border_light": "#3f3f46",
    "accent": "#22c55e",       # money green
    "accent_dim": "#166534",
    "warn": "#f59e0b",         # amber
    "danger": "#ef4444",
    "info": "#3b82f6",
    "purple": "#a78bfa",
    "text": "#fafafa",
    "text2": "#a1a1aa",
    "text3": "#71717a",
    "grid": "#27272a",
}

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    .stApp { background-color: #09090b; }
    header[data-testid="stHeader"] { background-color: #09090b; }
    .block-container { padding-top: 1.2rem; max-width: 1360px; }
    #MainMenu, footer, .stDeployButton { display: none !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0; background: #18181b; border-radius: 8px 8px 0 0;
        padding: 4px 4px 0; border-bottom: 1px solid #27272a;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 22px; font-family: 'DM Sans'; font-weight: 600;
        font-size: 13px; color: #71717a; border-radius: 8px 8px 0 0;
        border: none; background: transparent;
    }
    .stTabs [aria-selected="true"] {
        background: #18181b !important; color: #22c55e !important;
        border-bottom: 2px solid #22c55e !important;
    }

    /* Metric cards */
    .m-card {
        background: linear-gradient(135deg, #18181b 0%, #09090b 100%);
        border: 1px solid #27272a; border-radius: 12px;
        padding: 18px 22px; position: relative; overflow: hidden;
    }
    .m-card::before { content:''; position:absolute; top:0; left:0; width:4px; height:100%; border-radius:12px 0 0 12px; }
    .m-card.green::before { background:#22c55e; }
    .m-card.amber::before { background:#f59e0b; }
    .m-card.red::before { background:#ef4444; }
    .m-card.blue::before { background:#3b82f6; }
    .m-card.purple::before { background:#a78bfa; }
    .m-label { font-family:'IBM Plex Mono'; font-size:10.5px; color:#71717a; text-transform:uppercase; letter-spacing:1.2px; margin-bottom:6px; }
    .m-value { font-family:'DM Sans'; font-size:28px; font-weight:800; color:#fafafa; line-height:1; }
    .m-sub { font-family:'IBM Plex Mono'; font-size:11.5px; color:#52525b; margin-top:5px; }
    .m-trend-up { color:#ef4444; } .m-trend-down { color:#22c55e; }

    /* Section headers */
    .s-title { font-family:'DM Sans'; font-size:21px; font-weight:700; color:#fafafa; letter-spacing:-0.4px; margin-bottom:2px; }
    .s-sub { font-family:'IBM Plex Mono'; font-size:11.5px; color:#52525b; margin-bottom:18px; }

    /* Chart card */
    .cc { background:#18181b; border:1px solid #27272a; border-radius:12px; padding:18px; }
    .cc-title { font-family:'DM Sans'; font-size:14px; font-weight:600; color:#fafafa; margin-bottom:2px; }
    .cc-sub { font-family:'IBM Plex Mono'; font-size:10.5px; color:#52525b; margin-bottom:12px; }

    /* Insight boxes */
    .ins { border-radius:8px; padding:12px 16px; font-family:'IBM Plex Mono'; font-size:12px; line-height:1.6; color:#a1a1aa; }
    .ins-find { background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.2); }
    .ins-warn { background:rgba(245,158,11,0.08); border:1px solid rgba(245,158,11,0.2); }
    .ins-rec { background:rgba(59,130,246,0.08); border:1px solid rgba(59,130,246,0.2); }

    /* Dataset links */
    .ds-links { background:#18181b; border:1px solid #27272a; border-radius:10px; padding:18px 22px; margin-top:24px; }
    .ds-links a { color:#22c55e; text-decoration:none; font-family:'IBM Plex Mono'; font-size:12px; }
    .ds-links a:hover { text-decoration:underline; color:#4ade80; }

    /* Filter label */
    .f-label { font-family:'IBM Plex Mono'; font-size:10px; color:#71717a; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA LOADING — Employee Compensation from data.sanjoseca.gov
# ============================================================

COMP_BASE = "https://data.sanjoseca.gov/dataset/3f671aee-9aec-4015-8a5a-5f92803b2534/resource/{rid}/download/{fname}"
COMP_URLS = {
    2018: ("420cb8e2-89d6-46af-a85f-069e81b41f42", "employee-compensation-2018.csv"),
    2019: ("83751b72-9da2-421d-bb16-715957f54dd4", "employee-compensation-2019.csv"),
    2020: ("56242183-5c74-4a20-85d7-b85421443477", "employee-compensation-2020.csv"),
    2021: ("205afc93-b3d2-4199-8d44-14a435b84dd7", "employee-compensation-2021.csv"),
    2022: ("efbf228b-f436-4297-aef2-48980ae1f579", "employee-compensation-2022.csv"),
    2023: ("6f269eb5-7d88-45d3-8911-881545c6e521", "employee-compensation-2023.csv"),
    2024: ("bc7e0721-8467-43e1-a9c1-1beedcf442f1", "employee-compensation-2024.csv"),
}


def _fetch_csv(url, timeout=45):
    headers = {"User-Agent": "Mozilla/5.0 (SJ-Budget-Dashboard) Streamlit/1.0"}
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    content = resp.content.decode("utf-8-sig", errors="replace")
    return pd.read_csv(StringIO(content), low_memory=False)


def _download_one(year, url):
    try:
        df = _fetch_csv(url)
        return (year, df)
    except Exception:
        return (year, None)


@st.cache_data(ttl=3600, show_spinner=False)
def load_compensation_data():
    tasks = []
    for year, (rid, fname) in COMP_URLS.items():
        url = COMP_BASE.format(rid=rid, fname=fname)
        tasks.append((year, url))

    results = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {pool.submit(_download_one, yr, url): yr for yr, url in tasks}
        for future in as_completed(futures):
            year, df = future.result()
            if df is not None:
                df["Year"] = year
                results.append(df)

    if not results:
        return pd.DataFrame()
    combined = pd.concat(results, ignore_index=True)
    return combined


def _clean_money(series):
    """Safely convert a money column (possibly with $, commas, quotes) to float."""
    # If it's a DataFrame (duplicate cols), take the first one
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    return pd.to_numeric(
        series.astype(str)
        .apply(lambda v: v.replace(",", "").replace("$", "").replace('"', "").strip()),
        errors="coerce"
    ).fillna(0)


def process_compensation(df):
    if df.empty:
        return df

    # Remove duplicate columns BEFORE renaming (keep first occurrence)
    df = df.loc[:, ~df.columns.duplicated()]

    # Normalize columns
    col_map = {}
    seen_targets = set()
    for c in df.columns:
        cl = c.strip().lower()
        target = None
        if cl == "name": target = "name"
        elif "department" in cl: target = "department"
        elif "job" in cl and "title" in cl: target = "job_title"
        elif "total" in cl and "cash" in cl: target = "total_comp"
        elif "base" in cl and "pay" in cl: target = "base_pay"
        elif cl == "overtime" or cl.startswith("overtime"): target = "overtime"
        elif "sick" in cl or "vacation" in cl: target = "sick_vacation"
        elif "other" in cl and "cash" in cl: target = "other_cash"
        elif "defined" in cl and "contribution" in cl: target = "dc_contributions"
        elif "medical" in cl or "dental" in cl: target = "medical_dental"
        elif "retirement" in cl: target = "retirement"
        elif "long" in cl and "term" in cl: target = "ltd_life"
        elif "misc" in cl and "employ" in cl: target = "misc_costs"

        # Only map if we haven't already mapped to this target
        if target and target not in seen_targets:
            col_map[c] = target
            seen_targets.add(target)

    df = df.rename(columns=col_map)

    # Remove any remaining duplicate columns after rename
    df = df.loc[:, ~df.columns.duplicated()]

    # Parse money columns safely
    money_cols = ["total_comp", "base_pay", "overtime", "sick_vacation", "other_cash",
                  "dc_contributions", "medical_dental", "retirement", "ltd_life", "misc_costs"]
    for col in money_cols:
        if col in df.columns:
            df[col] = _clean_money(df[col])

    # Compute total benefits
    benefit_cols = [c for c in ["dc_contributions", "medical_dental", "retirement", "ltd_life", "misc_costs"] if c in df.columns]
    df["total_benefits"] = df[benefit_cols].sum(axis=1) if benefit_cols else 0

    # Compute total cost = total cash comp + total benefits
    if "total_comp" in df.columns:
        df["total_cost"] = df["total_comp"] + df["total_benefits"]
    else:
        df["total_cost"] = df["total_benefits"]

    # Overtime rate
    if "overtime" in df.columns and "base_pay" in df.columns:
        df["ot_rate"] = np.where(df["base_pay"] > 0, df["overtime"] / df["base_pay"] * 100, 0)

    return df


# ============================================================
# HELPERS
# ============================================================

def mc(icon, label, value, sub, color="green", trend=None):
    t_html = ""
    if trend is not None:
        cls = "m-trend-up" if trend > 0 else "m-trend-down"
        arrow = "▲" if trend > 0 else "▼"
        t_html = f'<span class="{cls}">{arrow} {abs(trend):.1f}%</span> '
    st.markdown(f'<div class="m-card {color}"><div class="m-label">{icon} {label}</div><div class="m-value">{value}</div><div class="m-sub">{t_html}{sub}</div></div>', unsafe_allow_html=True)


def ins(text, t="find"):
    icons = {"find": "💡", "warn": "⚠️", "rec": "📋"}
    st.markdown(f'<div class="ins ins-{t}">{icons.get(t,"")} {text}</div>', unsafe_allow_html=True)


def ch(title, sub=""):
    s = f'<div class="cc-sub">{sub}</div>' if sub else ""
    st.markdown(f'<div class="cc-title">{title}</div>{s}', unsafe_allow_html=True)


def style_fig(fig, h=320):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans, sans-serif", color=C["text3"], size=11),
        xaxis=dict(gridcolor=C["grid"], gridwidth=0.5, zeroline=False),
        yaxis=dict(gridcolor=C["grid"], gridwidth=0.5, zeroline=False),
        margin=dict(l=10, r=10, t=10, b=10), height=h,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=C["text3"])),
        hoverlabel=dict(bgcolor=C["surface"], bordercolor=C["border"], font_size=12),
    )
    return fig


def fmt_money(v):
    if abs(v) >= 1e9: return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6: return f"${v/1e6:.1f}M"
    if abs(v) >= 1e3: return f"${v/1e3:.0f}K"
    return f"${v:,.0f}"


# ============================================================
# LOAD & PROCESS
# ============================================================

raw_df = load_compensation_data()
full_df = process_compensation(raw_df)
data_ok = len(full_df) > 0


# ============================================================
# HEADER
# ============================================================

st.markdown(f"""
<div style="background:linear-gradient(135deg,#18181b 0%,#09090b 100%); border:1px solid #27272a; border-radius:12px; padding:20px 28px 16px; margin-bottom:8px;">
    <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px;">
        <div>
            <p style="font-family:'DM Sans'; font-size:25px; font-weight:800; background:linear-gradient(135deg,#fafafa 0%,#22c55e 100%); -webkit-background-clip:text; -webkit-text-fill-color:transparent; letter-spacing:-0.8px; margin:0;">
                💰 San José Budget & Compensation Intelligence
            </p>
            <p style="font-family:'IBM Plex Mono'; font-size:11.5px; color:#52525b; margin-top:2px;">
                Employee compensation analysis · Department spending · Overtime waste · data.sanjoseca.gov
            </p>
        </div>
        <div style="padding:5px 14px; border-radius:20px; background:rgba(34,197,94,0.1); border:1px solid rgba(34,197,94,0.2); font-family:'IBM Plex Mono'; font-size:11px; color:#22c55e;">
            Live Data · {len(full_df):,} records · {sorted(full_df['Year'].unique().tolist()) if data_ok else 'N/A'}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# GLOBAL FILTERS
# ============================================================

if data_ok:
    st.markdown('<div class="f-label" style="margin-top:6px;">⚙️ FILTERS</div>', unsafe_allow_html=True)
    fc1, fc2, fc3 = st.columns([2, 3, 3])

    all_years = sorted(full_df["Year"].unique().tolist())
    all_depts = sorted(full_df["department"].dropna().unique().tolist()) if "department" in full_df.columns else []

    with fc1:
        yr_range = st.select_slider("Year Range", options=all_years, value=(min(all_years), max(all_years)), key="yr")

    with fc2:
        sel_depts = st.multiselect("Departments", options=all_depts, default=all_depts, key="depts")

    with fc3:
        min_comp = st.slider("Min Total Compensation ($)", 0, 300000, 0, step=10000, key="mincomp",
                             help="Filter out employees below this total compensation threshold")

    # Apply filters
    df = full_df.copy()
    df = df[(df["Year"] >= yr_range[0]) & (df["Year"] <= yr_range[1])]
    if sel_depts and "department" in df.columns:
        df = df[df["department"].isin(sel_depts)]
    if "total_comp" in df.columns:
        df = df[df["total_comp"] >= min_comp]

    st.caption(f"🔍 Showing **{len(df):,}** of {len(full_df):,} records")
else:
    df = pd.DataFrame()


# ============================================================
# TABS
# ============================================================

tab_summary, tab_dept, tab_overtime, tab_trends, tab_top = st.tabs(
    ["Budget Overview", "Department Breakdown", "Overtime & Waste", "Year-over-Year Trends", "Top Earners"]
)


# ======================== BUDGET OVERVIEW ========================
with tab_summary:
    if not data_ok:
        st.warning("⚠️ Could not load compensation data.")
    else:
        st.markdown('<div class="s-title">City Compensation Spending Overview</div>', unsafe_allow_html=True)
        st.markdown('<div class="s-sub">Employee compensation is typically 75–80% of the City operating budget</div>', unsafe_allow_html=True)

        # Key metrics
        latest_yr = df["Year"].max()
        latest = df[df["Year"] == latest_yr]
        prev_yr = latest_yr - 1
        prev = df[df["Year"] == prev_yr] if prev_yr in df["Year"].values else pd.DataFrame()

        total_spend = latest["total_cost"].sum()
        total_cash = latest["total_comp"].sum()
        total_ot = latest["overtime"].sum() if "overtime" in latest.columns else 0
        total_benefits = latest["total_benefits"].sum()
        headcount = len(latest)
        avg_comp = latest["total_comp"].mean() if headcount > 0 else 0

        # YoY change
        prev_spend = prev["total_cost"].sum() if len(prev) > 0 else 0
        spend_change = ((total_spend - prev_spend) / prev_spend * 100) if prev_spend > 0 else 0

        prev_ot = prev["overtime"].sum() if len(prev) > 0 and "overtime" in prev.columns else 0
        ot_change = ((total_ot - prev_ot) / prev_ot * 100) if prev_ot > 0 else 0

        # Total OT as deficit/waste indicator
        ot_pct_of_base = (total_ot / latest["base_pay"].sum() * 100) if latest["base_pay"].sum() > 0 else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: mc("💵", f"Total Spending ({latest_yr})", fmt_money(total_spend), f"cash + benefits", "green", spend_change)
        with c2: mc("🏦", "Total Cash Comp", fmt_money(total_cash), f"{headcount:,} employees", "blue")
        with c3: mc("⏰", "Overtime Spending", fmt_money(total_ot), f"{ot_pct_of_base:.1f}% of base pay", "red", ot_change)
        with c4: mc("🏥", "Total Benefits", fmt_money(total_benefits), "medical + retirement + misc", "purple")
        with c5: mc("👤", "Avg Compensation", fmt_money(avg_comp), f"per employee", "amber")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        # Composition breakdown
        col_a, col_b = st.columns([3, 2])
        with col_a:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Spending Composition Breakdown", f"Where every dollar goes — {latest_yr}")
            comp_data = {
                "Category": ["Base Pay", "Overtime", "Sick/Vacation", "Other Cash", "Medical/Dental", "Retirement", "Other Benefits"],
                "Amount": [
                    latest["base_pay"].sum(),
                    latest.get("overtime", pd.Series([0])).sum(),
                    latest.get("sick_vacation", pd.Series([0])).sum(),
                    latest.get("other_cash", pd.Series([0])).sum(),
                    latest.get("medical_dental", pd.Series([0])).sum(),
                    latest.get("retirement", pd.Series([0])).sum(),
                    latest.get("ltd_life", pd.Series([0])).sum() + latest.get("misc_costs", pd.Series([0])).sum() + latest.get("dc_contributions", pd.Series([0])).sum(),
                ]
            }
            comp_df = pd.DataFrame(comp_data)
            comp_df = comp_df[comp_df["Amount"] > 0].sort_values("Amount", ascending=True)
            colors = [C["accent"], C["danger"], C["warn"], C["info"], C["purple"], "#06b6d4", "#ec4899"]
            fig = go.Figure(go.Bar(
                y=comp_df["Category"], x=comp_df["Amount"], orientation="h",
                marker_color=colors[:len(comp_df)],
                text=comp_df["Amount"].apply(fmt_money), textposition="outside",
                textfont=dict(size=11, color=C["text2"]),
            ))
            style_fig(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Cash vs Benefits Split", "Proportion of total cost")
            pie_data = pd.DataFrame({
                "Type": ["Cash Compensation", "Benefits"],
                "Amount": [total_cash, total_benefits],
            })
            fig = go.Figure(go.Pie(
                labels=pie_data["Type"], values=pie_data["Amount"], hole=0.5,
                marker=dict(colors=[C["accent"], C["purple"]]),
                textinfo="percent+label", textfont=dict(size=11, color=C["text2"]),
            ))
            style_fig(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        # Top departments by spend
        st.markdown('<div class="cc">', unsafe_allow_html=True)
        ch("Top 10 Departments by Total Spending", f"{latest_yr} — including cash compensation + benefits")
        dept_spend = latest.groupby("department").agg(
            total=("total_cost", "sum"), headcount=("name", "count"), avg=("total_cost", "mean")
        ).reset_index().sort_values("total", ascending=False).head(10)
        fig = go.Figure(go.Bar(
            x=dept_spend["department"], y=dept_spend["total"],
            marker_color=C["accent"], text=dept_spend["total"].apply(fmt_money),
            textposition="outside", textfont=dict(size=10, color=C["text2"]),
        ))
        style_fig(fig, 320)
        fig.update_layout(xaxis=dict(tickangle=-30))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

        i1, i2 = st.columns(2)
        with i1: ins(f"Total city compensation spending in {latest_yr}: <strong style='color:#fafafa'>{fmt_money(total_spend)}</strong>. Year-over-year change: <strong style='color:{'#ef4444' if spend_change > 0 else '#22c55e'}'>{spend_change:+.1f}%</strong>.", "find")
        with i2: ins(f"Overtime accounts for <strong style='color:#fafafa'>{fmt_money(total_ot)}</strong> ({ot_pct_of_base:.1f}% of base pay). Departments with OT rates above 30% signal potential staffing deficits or systematic overwork.", "warn")


# ======================== DEPARTMENT BREAKDOWN ========================
with tab_dept:
    if not data_ok:
        st.warning("⚠️ No data available.")
    else:
        st.markdown('<div class="s-title">Department-Level Spending Analysis</div>', unsafe_allow_html=True)
        st.markdown('<div class="s-sub">Compare spending, headcount, and cost-per-employee across departments</div>', unsafe_allow_html=True)

        latest = df[df["Year"] == df["Year"].max()]
        dept_agg = latest.groupby("department").agg(
            total_spend=("total_cost", "sum"),
            total_cash=("total_comp", "sum"),
            total_ot=("overtime", "sum"),
            total_base=("base_pay", "sum"),
            total_benefits=("total_benefits", "sum"),
            headcount=("name", "count"),
            avg_comp=("total_comp", "mean"),
        ).reset_index()
        dept_agg["ot_rate"] = np.where(dept_agg["total_base"] > 0, dept_agg["total_ot"] / dept_agg["total_base"] * 100, 0)
        dept_agg["cost_per_emp"] = np.where(dept_agg["headcount"] > 0, dept_agg["total_spend"] / dept_agg["headcount"], 0)
        dept_agg = dept_agg.sort_values("total_spend", ascending=False)

        # Metrics
        top_dept = dept_agg.iloc[0] if len(dept_agg) > 0 else None
        worst_ot = dept_agg.loc[dept_agg["ot_rate"].idxmax()] if len(dept_agg) > 0 else None

        c1, c2, c3 = st.columns(3)
        if top_dept is not None:
            with c1: mc("🏛", "Largest Department", fmt_money(top_dept["total_spend"]), top_dept["department"], "green")
        if worst_ot is not None:
            with c2: mc("⏰", "Highest OT Rate", f"{worst_ot['ot_rate']:.1f}%", worst_ot["department"], "red")
        with c3: mc("📊", "Departments Tracked", str(len(dept_agg)), f"{dept_agg['headcount'].sum():,} total employees", "blue")

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # Chart: Department spend comparison
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Spending by Department", "Total cost = cash + benefits")
            d = dept_agg.head(15).sort_values("total_spend", ascending=True)
            fig = go.Figure(go.Bar(
                y=d["department"], x=d["total_spend"], orientation="h",
                marker_color=C["accent"], text=d["total_spend"].apply(fmt_money),
                textposition="outside", textfont=dict(size=10, color=C["text2"]),
            ))
            style_fig(fig, 400)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Cost per Employee by Department", "Average total cost per headcount")
            d = dept_agg.head(15).sort_values("cost_per_emp", ascending=True)
            fig = go.Figure(go.Bar(
                y=d["department"], x=d["cost_per_emp"], orientation="h",
                marker_color=C["info"], text=d["cost_per_emp"].apply(fmt_money),
                textposition="outside", textfont=dict(size=10, color=C["text2"]),
            ))
            style_fig(fig, 400)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        # OT rate by department
        st.markdown('<div class="cc">', unsafe_allow_html=True)
        ch("Overtime as % of Base Pay — by Department", "Departments above 30% (red line) signal chronic understaffing")
        d = dept_agg[dept_agg["total_base"] > 100000].sort_values("ot_rate", ascending=False).head(15)
        colors_ot = [C["danger"] if v > 30 else C["warn"] if v > 15 else C["accent"] for v in d["ot_rate"]]
        fig = go.Figure(go.Bar(
            x=d["department"], y=d["ot_rate"], marker_color=colors_ot,
            text=d["ot_rate"].round(1).astype(str) + "%", textposition="outside",
            textfont=dict(size=10, color=C["text2"]),
        ))
        fig.add_hline(y=30, line_dash="dash", line_color=C["danger"], opacity=0.5,
                      annotation_text="30% threshold", annotation_font_color=C["danger"])
        style_fig(fig, 320)
        fig.update_layout(xaxis=dict(tickangle=-35))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

        # Full department table
        with st.expander("📋 Full Department Data Table"):
            display_df = dept_agg[["department", "headcount", "total_spend", "total_cash", "total_ot", "total_benefits", "ot_rate", "cost_per_emp"]].copy()
            display_df.columns = ["Department", "Headcount", "Total Spend", "Cash Comp", "Overtime", "Benefits", "OT Rate %", "Cost/Employee"]
            for c in ["Total Spend", "Cash Comp", "Overtime", "Benefits", "Cost/Employee"]:
                display_df[c] = display_df[c].apply(lambda v: f"${v:,.0f}")
            display_df["OT Rate %"] = display_df["OT Rate %"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(display_df, use_container_width=True, height=400)


# ======================== OVERTIME & WASTE ========================
with tab_overtime:
    if not data_ok:
        st.warning("⚠️ No data available.")
    else:
        st.markdown('<div class="s-title">Overtime Spending & Budget Waste Analysis</div>', unsafe_allow_html=True)
        st.markdown('<div class="s-sub">Overtime is the #1 controllable budget cost — where is it concentrated?</div>', unsafe_allow_html=True)

        latest = df[df["Year"] == df["Year"].max()]
        total_ot = latest["overtime"].sum() if "overtime" in latest.columns else 0
        total_base = latest["base_pay"].sum() if "base_pay" in latest.columns else 0
        ot_pct = (total_ot / total_base * 100) if total_base > 0 else 0

        # Employees with OT > base pay (extreme cases)
        extreme_ot = latest[latest["overtime"] > latest["base_pay"]] if "overtime" in latest.columns else pd.DataFrame()
        extreme_cost = extreme_ot["overtime"].sum() if len(extreme_ot) > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1: mc("💸", "Total Overtime", fmt_money(total_ot), f"across all departments", "red")
        with c2: mc("📊", "OT as % of Base", f"{ot_pct:.1f}%", "city-wide average", "amber")
        with c3: mc("🚨", "Extreme OT Cases", f"{len(extreme_ot):,}", "OT exceeds base pay", "red")
        with c4: mc("🔥", "Extreme OT Cost", fmt_money(extreme_cost), "potential waste", "red")

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # OT trend by year
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Overtime Spending by Year", "Is overtime growing faster than base pay?")
            ot_trend = df.groupby("Year").agg(
                total_ot=("overtime", "sum"), total_base=("base_pay", "sum")
            ).reset_index()
            ot_trend["ot_pct"] = ot_trend["total_ot"] / ot_trend["total_base"] * 100
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=ot_trend["Year"], y=ot_trend["total_ot"], name="Overtime $", marker_color=C["danger"], opacity=0.7), secondary_y=False)
            fig.add_trace(go.Scatter(x=ot_trend["Year"], y=ot_trend["ot_pct"], name="OT % of Base", mode="lines+markers", line=dict(color=C["warn"], width=3), marker=dict(size=8)), secondary_y=True)
            fig.update_yaxes(title_text="Overtime $", secondary_y=False, gridcolor=C["grid"])
            fig.update_yaxes(title_text="OT %", secondary_y=True, gridcolor=C["grid"])
            style_fig(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Top 10 Departments by Overtime $", "Where is the overtime budget concentrated?")
            dept_ot = latest.groupby("department")["overtime"].sum().reset_index().sort_values("overtime", ascending=True).tail(10)
            fig = go.Figure(go.Bar(
                y=dept_ot["department"], x=dept_ot["overtime"], orientation="h",
                marker_color=C["danger"], text=dept_ot["overtime"].apply(fmt_money),
                textposition="outside", textfont=dict(size=10, color=C["text2"]),
            ))
            style_fig(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        # Extreme OT employees
        if len(extreme_ot) > 0:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch(f"🚨 Employees Where Overtime Exceeds Base Pay ({len(extreme_ot)} cases)", "These represent the most extreme overtime situations — potential indicators of chronic understaffing")
            show_cols = ["name", "department", "job_title", "base_pay", "overtime", "total_comp"]
            show_cols = [c for c in show_cols if c in extreme_ot.columns]
            display = extreme_ot[show_cols].sort_values("overtime", ascending=False).head(20).copy()
            for c in ["base_pay", "overtime", "total_comp"]:
                if c in display.columns:
                    display[c] = display[c].apply(lambda v: f"${v:,.0f}")
            display.columns = [c.replace("_", " ").title() for c in display.columns]
            st.dataframe(display, use_container_width=True, height=min(400, 40 + len(display) * 35))
            st.markdown('</div>', unsafe_allow_html=True)

        i1, i2 = st.columns(2)
        with i1: ins(f"<strong style='color:#fafafa'>{len(extreme_ot)}</strong> employees earned more in overtime than their base salary, costing <strong style='color:#fafafa'>{fmt_money(extreme_cost)}</strong>. This signals chronic understaffing — hiring additional staff at base pay rates would likely cost less.", "warn")
        with i2: ins("Police and Fire departments typically account for 80%+ of all overtime. Targeted hiring in these departments could reduce overtime dependency and improve service quality simultaneously.", "rec")


# ======================== TRENDS ========================
with tab_trends:
    if not data_ok:
        st.warning("⚠️ No data available.")
    else:
        st.markdown('<div class="s-title">Year-over-Year Spending Trends</div>', unsafe_allow_html=True)
        st.markdown('<div class="s-sub">Tracking compensation growth, headcount changes, and cost escalation</div>', unsafe_allow_html=True)

        yearly = df.groupby("Year").agg(
            total_spend=("total_cost", "sum"), total_cash=("total_comp", "sum"),
            total_ot=("overtime", "sum"), total_base=("base_pay", "sum"),
            total_benefits=("total_benefits", "sum"), headcount=("name", "count"),
            avg_comp=("total_comp", "mean"),
        ).reset_index()
        yearly["yoy_change"] = yearly["total_spend"].pct_change() * 100
        yearly["cost_per_emp"] = yearly["total_spend"] / yearly["headcount"]

        first_spend = yearly["total_spend"].iloc[0] if len(yearly) > 0 else 0
        last_spend = yearly["total_spend"].iloc[-1] if len(yearly) > 0 else 0
        total_growth = ((last_spend - first_spend) / first_spend * 100) if first_spend > 0 else 0

        c1, c2, c3 = st.columns(3)
        with c1: mc("📈", f"Total Growth ({yearly['Year'].min()}–{yearly['Year'].max()})", f"{total_growth:+.1f}%", f"{fmt_money(first_spend)} → {fmt_money(last_spend)}", "green" if total_growth < 20 else "red")
        with c2: mc("👥", "Headcount Change", f"{yearly['headcount'].iloc[-1]:,}", f"from {yearly['headcount'].iloc[0]:,}", "blue")
        with c3:
            cpe_change = ((yearly['cost_per_emp'].iloc[-1] - yearly['cost_per_emp'].iloc[0]) / yearly['cost_per_emp'].iloc[0] * 100) if yearly['cost_per_emp'].iloc[0] > 0 else 0
            mc("💰", "Cost/Employee Change", f"{cpe_change:+.1f}%", f"{fmt_money(yearly['cost_per_emp'].iloc[0])} → {fmt_money(yearly['cost_per_emp'].iloc[-1])}", "amber")

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # Total spend trend
        st.markdown('<div class="cc">', unsafe_allow_html=True)
        ch("Total Compensation Spending Over Time", "Stacked: Base Pay + Overtime + Benefits")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=yearly["Year"], y=yearly["total_base"], name="Base Pay", stackgroup="one", fillcolor="rgba(34,197,94,0.4)", line=dict(color=C["accent"])))
        fig.add_trace(go.Scatter(x=yearly["Year"], y=yearly["total_ot"], name="Overtime", stackgroup="one", fillcolor="rgba(239,68,68,0.4)", line=dict(color=C["danger"])))
        fig.add_trace(go.Scatter(x=yearly["Year"], y=yearly["total_benefits"], name="Benefits", stackgroup="one", fillcolor="rgba(167,139,250,0.4)", line=dict(color=C["purple"])))
        style_fig(fig, 320)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("YoY Spending Change %", "Annual growth rate of total compensation")
            colors_yoy = [C["danger"] if v > 5 else C["warn"] if v > 0 else C["accent"] for v in yearly["yoy_change"].fillna(0)]
            fig = go.Figure(go.Bar(
                x=yearly["Year"], y=yearly["yoy_change"].fillna(0), marker_color=colors_yoy,
                text=yearly["yoy_change"].fillna(0).round(1).astype(str) + "%", textposition="outside",
                textfont=dict(size=11, color=C["text2"]),
            ))
            fig.add_hline(y=0, line_color=C["text3"], line_width=1)
            style_fig(fig, 280)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="cc">', unsafe_allow_html=True)
            ch("Headcount vs Average Comp", "Are costs rising from more employees or higher pay?")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=yearly["Year"], y=yearly["headcount"], name="Headcount", marker_color=C["info"], opacity=0.6), secondary_y=False)
            fig.add_trace(go.Scatter(x=yearly["Year"], y=yearly["avg_comp"], name="Avg Comp", mode="lines+markers", line=dict(color=C["warn"], width=3), marker=dict(size=8)), secondary_y=True)
            fig.update_yaxes(title_text="Headcount", secondary_y=False, gridcolor=C["grid"])
            fig.update_yaxes(title_text="Avg Comp $", secondary_y=True, gridcolor=C["grid"])
            style_fig(fig, 280)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)


# ======================== TOP EARNERS ========================
with tab_top:
    if not data_ok:
        st.warning("⚠️ No data available.")
    else:
        latest_yr = df["Year"].max()
        st.markdown(f'<div class="s-title">Top Earners — {latest_yr}</div>', unsafe_allow_html=True)
        st.markdown('<div class="s-sub">Highest total compensation packages in the city workforce</div>', unsafe_allow_html=True)

        latest = df[df["Year"] == latest_yr].sort_values("total_comp", ascending=False)

        c1, c2, c3 = st.columns(3)
        top1 = latest.iloc[0] if len(latest) > 0 else None
        if top1 is not None:
            with c1: mc("👑", "Highest Paid", fmt_money(top1["total_comp"]), f"{top1.get('name', 'N/A')} — {top1.get('department', '')}", "green")
        above_200k = latest[latest["total_comp"] > 200000]
        with c2: mc("💎", "Above $200K", f"{len(above_200k):,}", f"{len(above_200k)/len(latest)*100:.1f}% of workforce" if len(latest) > 0 else "", "amber")
        above_300k = latest[latest["total_comp"] > 300000]
        with c3: mc("🔥", "Above $300K", f"{len(above_300k):,}", f"total: {fmt_money(above_300k['total_comp'].sum())}", "red")

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # Top 25 table
        st.markdown('<div class="cc">', unsafe_allow_html=True)
        ch("Top 25 by Total Cash Compensation", "Includes base pay, overtime, and other cash")
        show_cols = ["name", "department", "job_title", "total_comp", "base_pay", "overtime", "total_benefits"]
        show_cols = [c for c in show_cols if c in latest.columns]
        top25 = latest[show_cols].head(25).copy()
        for c in ["total_comp", "base_pay", "overtime", "total_benefits"]:
            if c in top25.columns:
                top25[c] = top25[c].apply(lambda v: f"${v:,.0f}")
        top25.columns = [c.replace("_", " ").title() for c in top25.columns]
        st.dataframe(top25, use_container_width=True, height=600)
        st.markdown('</div>', unsafe_allow_html=True)

        # Distribution chart
        st.markdown('<div class="cc">', unsafe_allow_html=True)
        ch("Compensation Distribution", "Histogram of total cash compensation")
        fig = go.Figure(go.Histogram(
            x=latest["total_comp"], nbinsx=50,
            marker_color=C["accent"], marker_line_color=C["border"], marker_line_width=0.5,
        ))
        fig.add_vline(x=latest["total_comp"].median(), line_dash="dash", line_color=C["warn"],
                      annotation_text=f"Median: {fmt_money(latest['total_comp'].median())}", annotation_font_color=C["warn"])
        style_fig(fig, 280)
        fig.update_xaxes(title_text="Total Cash Compensation ($)")
        fig.update_yaxes(title_text="# Employees")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)


# ============================================================
# DATASET LINKS
# ============================================================

st.markdown("""
<div class="ds-links">
    <div style="font-family:'DM Sans'; font-size:15px; font-weight:600; color:#fafafa; margin-bottom:12px;">
        📂 Source Dataset — San José Open Data Portal
    </div>
    <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px;">
        <div>
            <div style="font-family:'IBM Plex Mono'; font-size:10px; color:#52525b; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">💰 Employee Compensation</div>
            <a href="https://data.sanjoseca.gov/dataset/employee-compensation-plan" target="_blank">data.sanjoseca.gov/dataset/employee-compensation-plan</a>
            <div style="font-size:11px; color:#3f3f46; margin-top:4px;">2013–2024 · CSV · Finance Dept · CC0 Licensed</div>
        </div>
        <div>
            <div style="font-family:'IBM Plex Mono'; font-size:10px; color:#52525b; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">📊 OpenGov Transparency</div>
            <a href="https://sanjoseca.opengov.com/transparency" target="_blank">sanjoseca.opengov.com/transparency</a>
            <div style="font-size:11px; color:#3f3f46; margin-top:4px;">Interactive budget & financial visualization</div>
        </div>
        <div>
            <div style="font-family:'IBM Plex Mono'; font-size:10px; color:#52525b; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">📄 Budget Documents</div>
            <a href="https://www.sanjoseca.gov/your-government/departments-offices/office-of-the-city-manager/budget/budget-documents" target="_blank">sanjoseca.gov/.../budget-documents</a>
            <div style="font-size:11px; color:#3f3f46; margin-top:4px;">Adopted operating & capital budgets · PDF</div>
        </div>
    </div>
    <div style="margin-top:14px; padding-top:12px; border-top:1px solid #27272a; display:flex; justify-content:space-between; font-family:'IBM Plex Mono'; font-size:11px; color:#52525b;">
        <span>All data: CC0 Licensed · <a href="https://data.sanjoseca.gov/dataset" target="_blank">Browse all 170 datasets →</a></span>
        <span>Dashboard built with Streamlit + Plotly · Live data from data.sanjoseca.gov</span>
    </div>
</div>
""", unsafe_allow_html=True)