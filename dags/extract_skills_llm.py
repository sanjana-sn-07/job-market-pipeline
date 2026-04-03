import psycopg2
import logging
import os
import json
from openai import OpenAI
from rds_config import RDS_CONFIG

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}

CONFIGS = [DB_CONFIG, RDS_CONFIG]

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

SYSTEM_PROMPT = """You are a technical recruiter expert. Extract all technical skills, tools, 
technologies, and programming languages mentioned in the job description.

Return ONLY a JSON array of skill strings, lowercase, no duplicates.
Example: ["python", "sql", "aws", "apache spark", "dbt", "machine learning"]

If no technical skills are found, return an empty array: []"""


def extract_skills_with_llm(description, job_id):
    if not description or len(description.strip()) < 50:
        return []

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Extract technical skills from this job description:\n\n{description[:2000]}"}
            ],
            temperature=0,
            max_tokens=300
        )

        content = response.choices[0].message.content.strip()

        # strip markdown code blocks if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        skills = json.loads(content)

        if isinstance(skills, list):
            return [s.lower().strip() for s in skills if isinstance(s, str)]
        return []

    except json.JSONDecodeError as e:
        logger.warning(f"LLM returned non-JSON for job {job_id}: {content[:100]}")
        return []
    except Exception as e:
        logger.error(f"OpenAI API error for job {job_id}: {e}")
        return []


def insert_llm_skills(job_id, skills, source, config):
    if not skills:
        return 0

    try:
        conn = psycopg2.connect(**config)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        raise

    cursor = conn.cursor()
    inserted = 0

    for skill in skills:
        try:
            cursor.execute("""
                INSERT INTO llm_extracted_skills (job_id, skill, source, extraction_method)
                VALUES (%s, %s, %s, 'llm')
                ON CONFLICT (job_id, skill) DO NOTHING
            """, (job_id, skill, source))
            inserted += 1
        except Exception as e:
            logger.warning(f"Skipping skill {skill} for job {job_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    return inserted


def extract_llm_skills_and_store(batch_size=20):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        raise

    cursor = conn.cursor()
    cursor.execute("""
        SELECT job_id, description_clean, source
        FROM processed_jobs
        WHERE job_id NOT IN (
            SELECT DISTINCT job_id FROM llm_extracted_skills
        )
        AND source = 'adzuna'
        LIMIT %s
    """, (batch_size,))
    jobs = cursor.fetchall()
    cursor.close()
    conn.close()

    if not jobs:
        logger.info("No new jobs to process with LLM")
        return

    logger.info(f"Processing {len(jobs)} jobs with LLM (batch size: {batch_size})")

    total_skills = 0
    for job_id, description, source in jobs:
        skills = extract_skills_with_llm(description, job_id)
        logger.info(f"Job {job_id}: extracted {len(skills)} skills — {skills}")

        for config in CONFIGS:
            try:
                inserted = insert_llm_skills(job_id, skills, source, config)
                total_skills += inserted
            except Exception as e:
                logger.warning(f"Skipping database {config.get('host', 'unknown')}: {e}")

    logger.info(f"LLM extraction complete — {total_skills} skill tags inserted across {len(jobs)} jobs")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_llm_skills_and_store(batch_size=20)
