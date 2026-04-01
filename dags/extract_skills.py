import psycopg2
import logging
import os
import re
from rds_config import RDS_CONFIG

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}

CONFIGS = [DB_CONFIG, RDS_CONFIG]

SKILLS = [
    # Languages
    "python", "sql", "java", "scala", "r", "bash", "go",
    # Data Engineering
    "airflow", "dbt", "spark", "kafka", "hadoop", "flink",
    "etl", "elt", "data pipeline", "data warehouse", "data lake",
    # Databases
    "postgresql", "mysql", "mongodb", "redis", "snowflake",
    "bigquery", "redshift", "databricks",
    # Cloud
    "aws", "gcp", "azure", "s3", "ec2", "lambda",
    # Tools
    "docker", "kubernetes", "git", "terraform", "jenkins",
    # ML / AI
    "machine learning", "deep learning", "pytorch", "tensorflow",
    "scikit-learn", "llm", "openai",
    # Viz
    "tableau", "power bi", "streamlit", "looker",
]


def extract_skills_from_text(text):
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for skill in SKILLS:
        pattern = r"\b" + re.escape(skill) + r"s?\b"
        if re.search(pattern, text_lower):
            found.append(skill)
    return found


def extract_and_store():
    for config in CONFIGS:
        _extract_for_db(config)


def _extract_for_db(config):
    try:
        conn = psycopg2.connect(**config)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database {config['host']}: {e}")
        raise

    cursor = conn.cursor()

    cursor.execute("""
        SELECT job_id, description_clean, source
        FROM processed_jobs
        WHERE job_id NOT IN (
            SELECT DISTINCT job_id FROM job_skills
        )
    """)
    jobs = cursor.fetchall()

    if not jobs:
        logger.info(f"No new jobs to extract skills from on {config['host']}")
        cursor.close()
        conn.close()
        return

    total_skills = 0
    for job_id, description, source in jobs:
        skills = extract_skills_from_text(description)
        for skill in skills:
            try:
                cursor.execute("""
                    INSERT INTO job_skills (job_id, skill, source)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (job_id, skill) DO NOTHING
                """, (job_id, skill, source))
                total_skills += 1
            except Exception as e:
                logger.warning(f"Skipping skill {skill} for job {job_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Extracted {total_skills} skill tags from {len(jobs)} jobs on {config['host']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_and_store()
