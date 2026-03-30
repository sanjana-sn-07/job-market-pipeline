import boto3
import psycopg2
import psycopg2.extras
import logging
import os
import json
from datetime import date

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "project-db"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "job_market"),
    "user":     os.environ.get("DB_USER", "pipeline_user"),
    "password": os.environ.get("DB_PASSWORD", "")
}

AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME", "")
AWS_REGION      = os.environ.get("AWS_REGION", "us-west-1")


def upload_processed_jobs_to_s3():
    today = date.today().isoformat()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        raise

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT job_id, title_normalized, company, location,
               description_clean, source, ingested_at::text
        FROM processed_jobs
        WHERE date(ingested_at) = CURRENT_DATE
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        logger.info("No new processed jobs today — skipping S3 upload")
        return

    data = [dict(row) for row in rows]
    json_data = json.dumps(data, indent=2)

    s3_key = f"processed_jobs/date={today}/jobs.json"

    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json_data.encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"Uploaded {len(data)} jobs to s3://{AWS_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    upload_processed_jobs_to_s3()
