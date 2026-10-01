import os
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import plotly.graph_objects as go

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

DATABASE_TIMEZONE = os.environ.get("DATABASE_TIMEZONE", "UTC")
DISPLAY_TIMEZONE = os.environ.get("DISPLAY_TIMEZONE", "America/Los_Angeles")

@st.cache_data(ttl=300)
def run_query(sql, params=None):
    # params are bound by psycopg2, never string-formatted into the SQL
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

def color_with_alpha(hex_color, alpha=0.14):
    value = hex_color.lstrip("#")
    r, g, b = (int(value[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r}, {g}, {b}, {alpha})"


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

top_skills_df = run_query("""
    SELECT skill, COUNT(DISTINCT job_id) AS job_count
    FROM job_skills
    WHERE skill != '__processed__'
    GROUP BY skill
    ORDER BY job_count DESC, skill ASC
    LIMIT %s
""", (top_n,))

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
st.caption(
    "⚠️ These two charts are not directly comparable — keyword extraction covers the full processed "
    "population, while LLM extraction intentionally processes a limited pending batch per database "
    "per run across both USAJobs and Adzuna. For a like-for-like comparison on the same job population, "
    "see the `mart_llm_vs_keyword_skills` model."
)

llm_top_df = run_query("""
    SELECT skill, COUNT(DISTINCT job_id) AS job_count
    FROM llm_extracted_skills
    WHERE skill != '__processed__'
    GROUP BY skill
    ORDER BY job_count DESC, skill ASC
    LIMIT %s
""", (top_n,))

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
    SELECT
        week_start,
        skill,
        SUM(job_count) AS job_count
    FROM mart_skill_trends
    GROUP BY week_start, skill
    ORDER BY week_start, skill
""")

observed_weeks_df = run_query("""
    SELECT DISTINCT date_trunc('week', ingested_at)::date AS week_start
    FROM processed_jobs
    ORDER BY week_start
""")

if not trend_df.empty:
    trend_df["week_start"] = pd.to_datetime(trend_df["week_start"])
    observed_weeks_df["week_start"] = pd.to_datetime(
        observed_weeks_df["week_start"]
    )

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
        filtered = trend_df[
            trend_df["skill"].isin(selected_skills)
        ].copy()

        full_weeks = pd.date_range(
            start=trend_df["week_start"].min(),
            end=trend_df["week_start"].max(),
            freq="W-MON",
        )

        observed_weeks = set(
            observed_weeks_df["week_start"].dt.normalize()
        )

        weekly_matrix = (
            filtered.pivot_table(
                index="week_start",
                columns="skill",
                values="job_count",
                aggfunc="sum",
            )
            .reindex(full_weeks)
        )

        observed_mask = weekly_matrix.index.normalize().isin(
            observed_weeks
        )

        for skill in selected_skills:
            weekly_matrix.loc[observed_mask, skill] = (
                weekly_matrix.loc[observed_mask, skill]
                .fillna(0)
            )

        plot_ready = (
            weekly_matrix[selected_skills]
            .rename_axis("week_start")
            .reset_index()
            .melt(
                id_vars="week_start",
                var_name="skill",
                value_name="job_count",
            )
        )

        fig3 = px.line(
            plot_ready,
            x="week_start",
            y="job_count",
            color="skill",
            markers=True,
            labels={
                "week_start": "Week",
                "job_count": "Job Count",
                "skill": "Skill",
            },
            title="Weekly Skill Demand Over Time",
        )

        fig3.update_traces(connectgaps=False)

        st.plotly_chart(fig3, use_container_width=True)

        st.caption(
            "Weeks with no pipeline ingestion are shown as gaps. "
            "A zero is used only when jobs were collected that week "
            "but the selected skill was not found."
        )
else:
    st.info(
        "No trend data yet. Run dbt models to populate mart_skill_trends."
    )

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
    fig5 = px.bar(
    seniority_df,
    x="seniority_level",
    y="job_count",
    color="seniority_level",
    color_discrete_sequence=px.colors.qualitative.Pastel,
    labels={
        "seniority_level": "Seniority Level",
        "job_count": "Job Count",
    },
    )
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
    forecast_df["ds"] = pd.to_datetime(forecast_df["ds"])

forecast_skills = forecast_df["skill"].unique().tolist()

selected_forecast = st.multiselect(
    "Select skills to forecast",
    options=forecast_skills,
    default=forecast_skills[:5]
)

if selected_forecast:
    plot_df = (
        forecast_df[
            forecast_df["skill"].isin(selected_forecast)
        ]
        .copy()
        .sort_values(["skill", "ds"])
    )

    fig6 = go.Figure()

    palette = px.colors.qualitative.Plotly

    for i, skill in enumerate(selected_forecast):
        skill_data = plot_df[
            plot_df["skill"] == skill
        ]

        actual = skill_data[
            ~skill_data["is_forecast"]
        ].copy()

        future = skill_data[
            skill_data["is_forecast"]
        ].copy()

        color = palette[i % len(palette)]

        # Historical observations
        if not actual.empty:
            actual = actual.sort_values("ds")

            # Build a complete weekly calendar so periods with no pipeline
            # ingestion appear as gaps instead of misleading straight lines.
            full_actual_weeks = pd.date_range(
                start=actual["ds"].min(),
                end=actual["ds"].max(),
                freq="W-MON",
            )

            actual_plot = (
                actual.set_index("ds")[["yhat"]]
                .reindex(full_actual_weeks)
            )

            fig6.add_trace(
                go.Scatter(
                    x=actual_plot.index,
                    y=actual_plot["yhat"],
                    mode="lines+markers",
                    name=f"{skill}, Actual",
                    line=dict(
                        color=color,
                        width=2,
                    ),
                    marker=dict(size=6),
                    connectgaps=False,
                )
            )

        if not future.empty:

            # Upper confidence bound
            fig6.add_trace(
                go.Scatter(
                    x=future["ds"],
                    y=future["yhat_upper"],
                    mode="lines",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

            # Lower bound + shaded area
            fig6.add_trace(
                go.Scatter(
                    x=future["ds"],
                    y=future["yhat_lower"],
                    mode="lines",
                    line=dict(width=0),
                    fill="tonexty",
                    fillcolor=color_with_alpha(color),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

            forecast_line = future[
                ["ds", "yhat"]
            ].copy()

            # Connect historical series to forecast
            if not actual.empty:
                last_actual = (
                    actual.sort_values("ds")
                    .iloc[-1]
                )

                connector = pd.DataFrame({
                    "ds": [last_actual["ds"]],
                    "yhat": [last_actual["yhat"]],
                })

                forecast_line = pd.concat(
                    [connector, forecast_line],
                    ignore_index=True,
                )

            fig6.add_trace(
                go.Scatter(
                    x=forecast_line["ds"],
                    y=forecast_line["yhat"],
                    mode="lines",
                    name=f"{skill}, Forecast",
                    line=dict(
                        color=color,
                        width=2,
                        dash="dash",
                    ),
                )
            )

    fig6.update_layout(
        title="Skill Demand Forecast — Next 6 Months",
        xaxis_title="Week",
        yaxis_title="Job Count",
        legend_title_text="Skill / Series",
        hovermode="x unified",
    )

    st.plotly_chart(
        fig6,
        use_container_width=True,
    )

    actual_rows = plot_df[
        ~plot_df["is_forecast"]
    ]

    history_counts = (
        actual_rows.groupby("skill")
        .size()
    )

    history_start = actual_rows["ds"].min()
    history_end = actual_rows["ds"].max()

    if not history_counts.empty:

        if history_counts.min() == history_counts.max():
            history_text = (
                f"{int(history_counts.iloc[0])} "
                "observed ingestion weeks"
            )
        else:
            history_text = (
                f"{int(history_counts.min())}–"
                f"{int(history_counts.max())} "
                "observed ingestion weeks"
                "across the selected skills"
            )

        st.caption(
            f"⚠️ Forecasts are based on {history_text}, "
            f"from {history_start:%b %Y} to {history_end:%b %Y}. "
            "Shaded areas show Prophet uncertainty intervals. "
            "Forecast reliability should improve as more "
            "historical data is collected."
        )
else:
    st.info("No forecast data yet. Run `python forecast/forecast.py` to generate forecasts.")

st.divider()

# ── Raw data explorer ──────────────────────────────────────────────────────────
st.subheader("🔍 Explore Recent Jobs")

search_skill = st.text_input("Filter by skill (e.g. python, dbt, spark)", "")

if search_skill:
    jobs_df = run_query("""
        SELECT
            p.job_id,
            p.title_normalized AS title,
            p.company,
            p.location,
            p.source,
            (
                p.ingested_at
                AT TIME ZONE %s
                AT TIME ZONE %s
            ) AS ingested_at
        FROM processed_jobs p
        WHERE EXISTS (
            SELECT 1
            FROM job_skills js
            WHERE js.job_id = p.job_id
              AND LOWER(js.skill) LIKE LOWER(%s)
        )
        ORDER BY p.ingested_at DESC
        LIMIT 50
    """, (
        DATABASE_TIMEZONE,
        DISPLAY_TIMEZONE,
        f"%{search_skill}%"
    ))

else:
    jobs_df = run_query("""
        SELECT
            job_id,
            title_normalized AS title,
            company,
            location,
            source,
            (
                ingested_at
                AT TIME ZONE %s
                AT TIME ZONE %s
            ) AS ingested_at
        FROM processed_jobs
        ORDER BY ingested_at DESC
        LIMIT 50
    """, (
        DATABASE_TIMEZONE,
        DISPLAY_TIMEZONE,
    ))

if "ingested_at" in jobs_df.columns:
    jobs_df["ingested_at"] = (
        pd.to_datetime(jobs_df["ingested_at"])
        .dt.strftime("%Y-%m-%d %H:%M")
    )

st.dataframe(
    jobs_df,
    use_container_width=True,
)

st.caption(
    f"Showing {len(jobs_df)} jobs · "
    f"timestamps displayed in {DISPLAY_TIMEZONE}"
)
