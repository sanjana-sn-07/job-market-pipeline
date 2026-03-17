import re
import psycopg2
import psycopg2.extras
import logging
import os

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}

TITLE_REPLACEMENTS = [
    (r"\bSr\.?\b",     "Senior"),
    (r"\bJr\.?\b",     "Junior"),
    (r"\bEngr\.?\b",   "Engineer"),
    (r"\bMgr\.?\b",    "Manager"),
    (r"\bDev\.?\b",    "Developer"),
    (r"\bAssoc\.?\b",  "Associate"),
    (r"\bSpec\.?\b",   "Specialist"),
    (r"\bPrinc\.?\b",  "Principal"),
    (r"\bArch\.?\b",   "Architect"),
]


def strip_html(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_title(title):
    if not title:
        return ""
    for pattern, replacement in TITLE_REPLACEMENTS:
        title = re.sub(pattern, replacement, title, flags=re.IGNORECASE)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def clean_and_store():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        raise

    cursor = conn.cursor()

    cursor.execute("""
        SELECT job_id, title, company, location, description, source
        FROM raw_jobs
        WHERE job_id NOT IN (SELECT job_id FROM processed_jobs)
    """)
    raw_jobs = cursor.fetchall()

    if not raw_jobs:
        logger.info("No new jobs to clean — processed_jobs is already up to date")
        cursor.close()
        conn.close()
        return

    inserted = 0
    for job_id, title, company, location, description, source in raw_jobs:
        title_normalized = normalize_title(title)
        description_clean = strip_html(description)

        try:
            cursor.execute("""
                INSERT INTO processed_jobs
                    (job_id, title, title_normalized, company, location, description_clean, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
            """, (job_id, title, title_normalized, company, location, description_clean, source))
            inserted += 1
        except Exception as e:
            logger.warning(f"Skipping job {job_id} during clean: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Cleaned and stored {inserted} new jobs into processed_jobs")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_and_store()
