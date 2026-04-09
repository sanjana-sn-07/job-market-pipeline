import os
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

st.set_page_config(
    page_title="Job Market Skills Dashboard",
    page_icon="📊",
    layout="wide"
)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5433)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "pipeline_pass"),
}


@st.cache_data(ttl=300)
def run_query(sql):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Job Market Skills Intelligence")
st.caption("Live data from USAJobs & Adzuna · Processed with GPT-4o-mini · Powered by Apache Airflow + dbt")
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_jobs      = run_query("SELECT COUNT(*) AS n FROM processed_jobs")
total_skills    = run_query("SELECT COUNT(DISTINCT skill) AS n FROM job_skills WHERE skill != '__processed__'")
llm_skills      = run_query("SELECT COUNT(DISTINCT skill) AS n FROM llm_extracted_skills WHERE skill != '__processed__'")
sources         = run_query("SELECT COUNT(DISTINCT source) AS n FROM processed_jobs")

col1.metric("Total Jobs Ingested",   int(total_jobs["n"].iloc[0]))
col2.metric("Keyword Skills Found",  int(total_skills["n"].iloc[0]))
col3.metric("LLM Skills Extracted",  int(llm_skills["n"].iloc[0]))
col4.metric("Data Sources",          int(sources["n"].iloc[0]))

st.divider()

# ── Top skills bar chart ───────────────────────────────────────────────────────
st.subheader("🔥 Top In-Demand Skills (Keyword Extraction)")

top_n = st.slider("Show top N skills", min_value=5, max_value=20, value=15)

top_skills_df = run_query(f"""
    SELECT skill, COUNT(*) AS job_count
    FROM job_skills
    WHERE skill != '__processed__'
    GROUP BY skill
    ORDER BY job_count DESC
    LIMIT {top_n}
""")

fig1 = px.bar(
    top_skills_df,
    x="job_count", y="skill",
    orientation="h",
    color="job_count",
    color_continuous_scale="Greens",
    labels={"job_count": "Job Count", "skill": "Skill"},
    title=f"Top {top_n} Most In-Demand Skills"
)
fig1.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
st.plotly_chart(fig1, use_container_width=True)

# ── LLM skills bar chart ───────────────────────────────────────────────────────
st.subheader("🤖 Top Skills (LLM Extraction via GPT-4o-mini)")

llm_top_df = run_query(f"""
    SELECT skill, COUNT(*) AS job_count
    FROM llm_extracted_skills
    WHERE skill != '__processed__'
    GROUP BY skill
    ORDER BY job_count DESC
    LIMIT {top_n}
""")

if not llm_top_df.empty:
    fig2 = px.bar(
        llm_top_df,
        x="job_count", y="skill",
        orientation="h",
        color="job_count",
        color_continuous_scale="Purples",
        labels={"job_count": "Job Count", "skill": "Skill"},
        title=f"Top {top_n} Skills from LLM Extraction"
    )
    fig2.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No LLM-extracted skills yet. Run extract_skills_llm.py to populate.")

st.divider()

# ── Skill trends over time ─────────────────────────────────────────────────────
st.subheader("📈 Skill Trends Over Time")

trend_df = run_query("""
    SELECT week_start, skill, job_count, skill_rank
    FROM mart_skill_trends
    ORDER BY week_start, skill_rank
""")

if not trend_df.empty:
    top_skills_list = (
        trend_df.groupby("skill")["job_count"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    selected_skills = st.multiselect(
        "Select skills to compare",
        options=trend_df["skill"].unique().tolist(),
        default=top_skills_list[:5]
    )

    if selected_skills:
        filtered = trend_df[trend_df["skill"].isin(selected_skills)]
        fig3 = px.line(
            filtered,
            x="week_start", y="job_count",
            color="skill",
            markers=True,
            labels={"week_start": "Week", "job_count": "Job Count", "skill": "Skill"},
            title="Weekly Skill Demand Over Time"
        )
        st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No trend data yet. Run dbt models to populate mart_skill_trends.")

st.divider()

# ── Jobs by source & seniority ─────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📡 Jobs by Source")
    source_df = run_query("""
        SELECT source, COUNT(*) AS job_count
        FROM processed_jobs
        GROUP BY source
    """)
    fig4 = px.pie(source_df, names="source", values="job_count",
                  color_discrete_sequence=px.colors.qualitative.Set2)
    st.plotly_chart(fig4, use_container_width=True)

with col_b:
    st.subheader("🎯 Jobs by Seniority Level")
    seniority_df = run_query("""
        SELECT seniority_level, COUNT(*) AS job_count
        FROM int_jobs_cleaned
        GROUP BY seniority_level
        ORDER BY job_count DESC
    """)
    fig5 = px.bar(seniority_df, x="seniority_level", y="job_count",
                  color="seniority_level",
                  color_discrete_sequence=px.colors.qualitative.Pastel)
    fig5.update_layout(showlegend=False)
    st.plotly_chart(fig5, use_container_width=True)

st.divider()

# ── Prophet Forecast ───────────────────────────────────────────────────────────
st.subheader("🔮 6-Month Skill Demand Forecast (Prophet ML Model)")
st.caption("Historical data shown as solid lines · Forecast shown as dashed lines with confidence band")

forecast_df = run_query("""
    SELECT skill, ds, yhat, yhat_lower, yhat_upper, is_forecast
    FROM skill_forecasts
    ORDER BY ds
""")

if not forecast_df.empty:
    forecast_skills = forecast_df["skill"].unique().tolist()
    selected_forecast = st.multiselect(
        "Select skills to forecast",
        options=forecast_skills,
        default=forecast_skills[:5]
    )

    if selected_forecast:
        plot_df = forecast_df[forecast_df["skill"].isin(selected_forecast)].copy()
        plot_df["type"] = plot_df["is_forecast"].map({True: "Forecast", False: "Actual"})
        plot_df["label"] = plot_df["skill"] + " (" + plot_df["type"] + ")"

        fig6 = px.line(
            plot_df,
            x="ds", y="yhat",
            color="skill",
            line_dash="type",
            line_dash_map={"Actual": "solid", "Forecast": "dash"},
            labels={"ds": "Week", "yhat": "Predicted Job Count", "skill": "Skill", "type": ""},
            title="Skill Demand Forecast — Next 6 Months"
        )
        st.plotly_chart(fig6, use_container_width=True)
        st.caption("⚠️ Forecast accuracy improves as more weekly data is collected. Currently based on ~4 weeks of data.")
else:
    st.info("No forecast data yet. Run `python forecast/forecast.py` to generate forecasts.")

st.divider()

# ── Raw data explorer ──────────────────────────────────────────────────────────
st.subheader("🔍 Explore Recent Jobs")

search_skill = st.text_input("Filter by skill (e.g. python, dbt, spark)", "")

if search_skill:
    jobs_df = run_query(f"""
        SELECT p.job_id, p.title_normalized AS title, p.company, p.location,
               p.source, p.ingested_at::date AS ingested_date
        FROM processed_jobs p
        JOIN job_skills js ON p.job_id = js.job_id
        WHERE LOWER(js.skill) LIKE LOWER('%%{search_skill}%%')
        ORDER BY p.ingested_at DESC
        LIMIT 50
    """)
else:
    jobs_df = run_query("""
        SELECT job_id, title_normalized AS title, company, location, source,
               ingested_at::date AS ingested_date
        FROM processed_jobs
        ORDER BY ingested_at DESC
        LIMIT 50
    """)

st.dataframe(jobs_df, use_container_width=True)
st.caption(f"Showing {len(jobs_df)} jobs")
