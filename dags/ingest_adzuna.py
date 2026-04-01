import requests
import psycopg2
import psycopg2.extras
import logging
import os
from rds_config import RDS_CONFIG

logger = logging.getLogger(__name__)

ADZUNA_APP_ID  = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}

CONFIGS = [DB_CONFIG, RDS_CONFIG]


def pull_from_adzuna(keyword="data engineer", results_per_page=25):
    url = f"https://api.adzuna.com/v1/api/jobs/us/search/1"

    params = {
        "app_id":           ADZUNA_APP_ID,
        "app_key":          ADZUNA_APP_KEY,
        "what":             keyword,
        "results_per_page": results_per_page,
        "content-type":     "application/json",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Adzuna API timed out after 30 seconds")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"Adzuna API returned error: {e.response.status_code}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to Adzuna API — network issue")
        raise

    try:
        data = response.json()
        jobs = data.get("results", [])
    except (KeyError, ValueError) as e:
        logger.error(f"Unexpected Adzuna API response format: {e}")
        raise

    logger.info(f"Pulled {len(jobs)} jobs from Adzuna for keyword: {keyword}")
    return jobs


def insert_adzuna_jobs(jobs):
    if not jobs:
        logger.warning("No Adzuna jobs to insert")
        return

    for config in CONFIGS:
        _insert_to_db(jobs, config)


def _insert_to_db(jobs, config):
    try:
        conn = psycopg2.connect(**config)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database {config['host']}: {e}")
        raise

    cursor  = conn.cursor()
    inserted = 0
    skipped  = 0

    for job in jobs:
        job_id      = str(job.get("id", ""))
        title       = job.get("title", "")
        company     = job.get("company", {}).get("display_name", "")
        location    = job.get("location", {}).get("display_name", "")
        description = job.get("description", "")

        if not job_id:
            logger.warning(f"Skipping Adzuna job with no ID: {title}")
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO raw_jobs (job_id, title, company, location, description, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
            """, (job_id, title, company, location, description, "adzuna",
                  psycopg2.extras.Json(job)))
            inserted += 1
        except Exception as e:
            logger.warning(f"Skipping Adzuna job {job_id}: {e}")
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Adzuna — Inserted: {inserted} | Skipped (duplicates): {skipped}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    keywords = ["data engineer", "data analyst"]
    for keyword in keywords:
        jobs = pull_from_adzuna(keyword=keyword, results_per_page=25)
        insert_adzuna_jobs(jobs)
