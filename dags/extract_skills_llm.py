"""LLM skill extraction for every pending USAJobs and Adzuna posting.

A batch is a database fetch size, NOT a per-run processing cap. Each database
has its own pending queue, so an RDS write failure can be repaired on a retry
without relying on a job remaining pending in the local database.
"""
import json
import logging
import os
from functools import lru_cache

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import boto3
import psycopg2
from openai import OpenAI
from rds_config import RDS_CONFIG

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "project-db"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "job_market"),
    "user": os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", ""),
}
CONFIGS = [DB_CONFIG, RDS_CONFIG]

SYSTEM_PROMPT = """You are a technical recruiter expert. Extract all technical skills, tools,
technologies, and programming languages mentioned in the job description.

Return ONLY a JSON array of skill strings, lowercase, no duplicates.
Example: ["python", "sql", "aws", "apache spark", "dbt", "machine learning"]

If no technical skills are found, return an empty array: []"""


def get_openai_key():
    """Try Secrets Manager first, then the environment; fail for a missing key."""
    secret_key = ""
    try:
        secrets = boto3.client("secretsmanager", region_name="us-east-1")
        response = secrets.get_secret_value(SecretId="job-market/openai")
        secret_key = json.loads(response["SecretString"]).get("OPENAI_API_KEY", "")
    except Exception as exc:
        logger.warning("Secrets Manager unavailable; trying OPENAI_API_KEY env var: %s", exc)

    key = secret_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    return key


@lru_cache(maxsize=1)
def get_llm_client():
    """Load credentials/client once per worker process, not once per job."""
    return OpenAI(api_key=get_openai_key())


def extract_skills_with_llm(description, job_id, llm_client=None):
    """Return a validated list; raise on API, truncation, or response failures.

    An actually empty description is a legitimate empty result and gets the
    sentinel. Unlike the former implementation, nonempty short text is sent.
    """
    if not description or not description.strip():
        return []

    llm_client = llm_client or get_llm_client()
    response = llm_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content":
                "Extract technical skills from this job description:\n\n"
                + description},
        ],
        temperature=0,
        max_tokens=300,
    )

    if not response.choices:
        raise ValueError("LLM returned no choices for job %s" % job_id)
    choice = response.choices[0]
    if choice.finish_reason == "length":
        raise ValueError("LLM response was truncated for job %s" % job_id)

    content = choice.message.content
    if not isinstance(content, str) or not content.strip():
        raise ValueError("LLM returned no text for job %s" % job_id)
    content = content.strip()
    if content.startswith("```"):
        parts = content.split("```")
        if len(parts) < 3:
            raise ValueError("Unclosed JSON code fence for job %s" % job_id)
        content = parts[1].strip()
        if content.lower().startswith("json"):
            content = content[4:].strip()

    try:
        skills = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON from LLM for job %s" % job_id) from exc
    if not isinstance(skills, list) or any(not isinstance(s, str) for s in skills):
        raise ValueError("LLM result must be an array of strings for job %s" % job_id)

    normalized = []
    seen = set()
    for skill in skills:
        skill = skill.strip().lower()
        if not skill or skill == "__processed__":
            continue
        if len(skill) > 100:  # current schema's VARCHAR(100) limit
            raise ValueError("LLM skill longer than 100 characters for job %s" % job_id)
        if skill not in seen:
            seen.add(skill)
            normalized.append(skill)
    return normalized


def fetch_pending_jobs(config, batch_size):
    """Read a page of unprocessed jobs from THIS database, across both APIs."""
    conn = psycopg2.connect(**config)
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT p.job_id, p.description_clean, p.source
                FROM processed_jobs AS p
                WHERE p.job_id IS NOT NULL
                  AND p.source IN ('usajobs', 'adzuna')
                  AND NOT EXISTS (
                    SELECT 1
                    FROM llm_extracted_skills AS l
                    WHERE l.job_id = p.job_id
                  )
                ORDER BY p.ingested_at NULLS FIRST, p.job_id
                LIMIT %s
            """, (batch_size,))
            return cursor.fetchall()
    finally:
        conn.close()


def insert_llm_skills(job_id, skills, source, config):
    """Atomically commit one job's tags, including an empty-result sentinel."""
    values = skills if skills else ["__processed__"]
    conn = psycopg2.connect(**config)
    try:
        with conn:  # commit on success, roll back on any SQL error
            with conn.cursor() as cursor:
                inserted = 0
                for skill in values:
                    cursor.execute("""
                        INSERT INTO llm_extracted_skills
                            (job_id, skill, source, extraction_method)
                        VALUES (%s, %s, %s, 'llm')
                        ON CONFLICT (job_id, skill) DO NOTHING
                    """, (job_id, skill, source))
                    inserted += cursor.rowcount  # zero when DO NOTHING fired
        return inserted
    finally:
        conn.close()


def extract_llm_skills_and_store(batch_size=100, max_jobs_per_db=5):
    """Process at most max_jobs_per_db pending records in each database."""
    if not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")
    if not isinstance(max_jobs_per_db, int) or max_jobs_per_db < 1:
        raise ValueError("max_jobs_per_db must be a positive integer")

    cache = {}
    stats = {"jobs_processed": 0, "skills_inserted": 0}

    for config in CONFIGS:
        host = config.get("host", "unknown")
        processed_this_db = 0

        while processed_this_db < max_jobs_per_db:
            remaining = max_jobs_per_db - processed_this_db
            jobs = fetch_pending_jobs(config, min(batch_size, remaining))
            if not jobs:
                break

            logger.info("Processing %s pending jobs on %s", len(jobs), host)
            for job_id, description, source in jobs:
                cache_key = (source, job_id, description or "")
                if cache_key not in cache:
                    cache[cache_key] = extract_skills_with_llm(
                        description, job_id
                    )

                skills = cache[cache_key]
                inserted = insert_llm_skills(job_id, skills, source, config)

                processed_this_db += 1
                stats["jobs_processed"] += 1
                stats["skills_inserted"] += inserted - (
                    1 if not skills and inserted else 0
                )

        logger.info("Processed %s jobs on %s", processed_this_db, host)

    logger.info("LLM extraction complete: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_llm_skills_and_store()
