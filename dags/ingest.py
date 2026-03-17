import requests
import psycopg2
import psycopg2.extras
import logging
import os

logger = logging.getLogger(__name__)

USAJOBS_API_KEY = os.environ.get("USAJOBS_API_KEY", "")
USAJOBS_EMAIL   = os.environ.get("USAJOBS_EMAIL", "")

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}


def pull_from_usajobs(keyword="data engineer", results_per_page=100):
    url = "https://data.usajobs.gov/api/search"

    headers = {
        "Host":              "data.usajobs.gov",
        "User-Agent":        USAJOBS_EMAIL,
        "Authorization-Key": USAJOBS_API_KEY
    }

    params = {
        "Keyword":        keyword,
        "ResultsPerPage": results_per_page,
        "Fields":         "min"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("USAJobs API timed out after 30 seconds")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"USAJobs API returned error: {e.response.status_code}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to USAJobs API — network issue")
        raise

    try:
        data = response.json()
        jobs = data["SearchResult"]["SearchResultItems"]
    except (KeyError, ValueError) as e:
        logger.error(f"Unexpected API response format: {e}")
        raise

    logger.info(f"Pulled {len(jobs)} jobs from USAJobs for keyword: {keyword}")
    return jobs


def insert_jobs(jobs):
    if not jobs:
        logger.warning("No jobs to insert — skipping database write")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        raise

    cursor  = conn.cursor()
    inserted = 0
    skipped  = 0

    for job in jobs:
        position = job["MatchedObjectDescriptor"]

        job_id      = position.get("PositionID", "")
        title       = position.get("PositionTitle", "")
        company     = position.get("OrganizationName", "")
        location    = position.get("PositionLocationDisplay", "")
        description = position.get("UserArea", {}).get("Details", {}).get("JobSummary", "")

        if not job_id:
            logger.warning(f"Skipping job with no ID: {title}")
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO raw_jobs (job_id, title, company, location, description, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
            """, (job_id, title, company, location, description, "usajobs",
                  psycopg2.extras.Json(position)))
            inserted += 1
        except Exception as e:
            logger.warning(f"Skipping job {job_id}: {e}")
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Done — Inserted: {inserted} | Skipped (duplicates): {skipped}")


if __name__ == "__main__":
    jobs = pull_from_usajobs()
    insert_jobs(jobs)
