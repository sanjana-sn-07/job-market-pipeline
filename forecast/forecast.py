import os
import logging
import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from prophet import Prophet

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5433)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "pipeline_pass"),
}


def create_forecast_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS skill_forecasts (
            id            SERIAL PRIMARY KEY,
            skill         VARCHAR(255),
            ds            DATE,
            yhat          FLOAT,
            yhat_lower    FLOAT,
            yhat_upper    FLOAT,
            is_forecast   BOOLEAN,
            forecasted_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cursor.close()
    logger.info("skill_forecasts table ready")


def run_forecast(top_n=10, periods=26):
    conn = psycopg2.connect(**DB_CONFIG)
    create_forecast_table(conn)

    # Load all historical weekly data from dbt gold table
    df = pd.read_sql("""
        SELECT week_start AS ds, skill, job_count AS y
        FROM mart_skill_trends
        ORDER BY week_start
    """, conn)

    if df.empty:
        logger.error("No data in mart_skill_trends — run dbt first")
        conn.close()
        return

    # Pick the top N skills by total historical job count
    top_skills = (
        df.groupby("skill")["y"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index.tolist()
    )
    logger.info(f"Forecasting top {top_n} skills: {top_skills}")

    # Wipe old forecasts so we always have fresh results
    cursor = conn.cursor()
    cursor.execute("DELETE FROM skill_forecasts")
    conn.commit()
    cursor.close()

    for skill in top_skills:
        skill_df = df[df["skill"] == skill][["ds", "y"]].copy()
        skill_df["ds"] = pd.to_datetime(skill_df["ds"])
        skill_df = skill_df.sort_values("ds")

        if len(skill_df) < 2:
            logger.warning(f"Skipping {skill} — not enough data points")
            continue

        try:
            model = Prophet(
                weekly_seasonality=False,
                daily_seasonality=False,
                yearly_seasonality=False,
                changepoint_prior_scale=0.3,
            )
            model.fit(skill_df)

            future = model.make_future_dataframe(periods=periods, freq="W")
            forecast = model.predict(future)

            cursor = conn.cursor()

            # Save actual historical values
            for _, row in skill_df.iterrows():
                cursor.execute("""
                    INSERT INTO skill_forecasts
                        (skill, ds, yhat, yhat_lower, yhat_upper, is_forecast)
                    VALUES (%s, %s, %s, %s, %s, FALSE)
                """, (skill, row["ds"].date(), row["y"], row["y"], row["y"]))

            # Save future forecast values only
            future_only = forecast[forecast["ds"] > skill_df["ds"].max()]
            for _, row in future_only.iterrows():
                cursor.execute("""
                    INSERT INTO skill_forecasts
                        (skill, ds, yhat, yhat_lower, yhat_upper, is_forecast)
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                """, (
                    skill,
                    row["ds"].date(),
                    max(0, row["yhat"]),
                    max(0, row["yhat_lower"]),
                    max(0, row["yhat_upper"]),
                ))

            conn.commit()
            cursor.close()
            logger.info(f"Forecast saved for: {skill}")

        except Exception as e:
            logger.error(f"Error forecasting {skill}: {e}")

    conn.close()
    logger.info(f"All forecasts complete for {len(top_skills)} skills")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_forecast(top_n=10, periods=26)
