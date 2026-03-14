import requests
import psycopg2
import psycopg2.extras
from datetime import datetime

USAJOBS_API_KEY = "AQJG5zfmh58XnkO2cE34okB/Y05rTQIf2pN6BcDs4x0="
USAJOBS_EMAIL   = "sanjana.sn.07@gmail.com"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "job_market",
    "user":     "pipeline_user",
    "password": "pipeline_pass"
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

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: {response.status_code} — {response.text}")
        return []

    data = response.json()
    jobs = data["SearchResult"]["SearchResultItems"]
    print(f"Pulled {len(jobs)} jobs from USAJobs")
    return jobs


def insert_jobs(jobs):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0
    skipped  = 0

    for job in jobs:
        position = job["MatchedObjectDescriptor"]

        job_id      = position.get("PositionID", "")
        title       = position.get("PositionTitle", "")
        company     = position.get("OrganizationName", "")
        location    = position.get("PositionLocationDisplay", "")
        description = position.get("UserArea", {}).get("Details", {}).get("JobSummary", "")

        try:
            cursor.execute("""
                INSERT INTO raw_jobs (job_id, title, company, location, description, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
            """, (job_id, title, company, location, description, "usajobs",
                  psycopg2.extras.Json(position)))
            inserted += 1
        except Exception as e:
            print(f"Skipping job {job_id}: {e}")
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted: {inserted} | Skipped (duplicates): {skipped}")


if __name__ == "__main__":
    jobs = pull_from_usajobs()
    insert_jobs(jobs)