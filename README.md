# Job Market Analytics Pipeline

![CI](https://github.com/sanjana-sn-07/job-market-pipeline/actions/workflows/ci.yml/badge.svg)

An end-to-end data engineering pipeline that ingests thousands of tech job postings daily from multiple APIs, transforms them through a Medallion Architecture using dbt, extracts required skills using keyword matching and LLM enrichment, and serves insights via an interactive Streamlit dashboard with 6-month skill demand forecasting.

---

## Architecture

```
USAJobs API + Adzuna API
         │
         ▼
   Apache Airflow (orchestration)
         │
    ┌────┴────────────────────┐
    ▼                         ▼
raw_jobs (Bronze)         AWS S3
    │                   (daily backup)
    ▼
processed_jobs (Silver)
    │
    ▼
job_skills (Silver)
    │
    ▼
mart_skill_trends (Gold) ──→ AWS RDS (production)
    │
    ▼
Streamlit Dashboard + Tableau Public
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Ingestion | Python, Apache Airflow, requests |
| Transformation | dbt (4 models, 18 data quality tests) |
| Storage | PostgreSQL, AWS S3, AWS RDS |
| AI Layer | OpenAI API — LLM skill extraction from job descriptions |
| ML Layer | Facebook Prophet — 6-month skill demand forecasting |
| Dashboard | Streamlit, Tableau Public |
| Testing | pytest (25 unit tests), dbt tests (18 data quality checks) |
| CI/CD | GitHub Actions — runs pytest on every push |
| Infrastructure | Docker, Docker Compose, AWS (S3, RDS, EC2) |

---

## Features

- **Dual-source ingestion** — pulls job postings daily from USAJobs (government) and Adzuna (private sector) APIs
- **Medallion Architecture** — Bronze (raw) → Silver (cleaned) → Gold (aggregated) data layers
- **dbt transformation pipeline** — 4 models with full lineage tracking and 18 automated data quality tests
- **Keyword skill extraction** — regex-based extraction of 40+ skills from job descriptions
- **LLM skill extraction** — OpenAI GPT-4o enrichment to catch skills missed by keyword matching
- **Government vs private sector comparison** — side-by-side skill demand analysis across data sources
- **6-month forecasting** — Facebook Prophet ML model predicts which skills will be most in demand
- **Interactive dashboard** — Streamlit app with skill trend charts, filters, and forecast visualization
- **Tableau Public version** — stakeholder-facing dashboard for DA role applications
- **Cloud infrastructure** — AWS RDS for production database, AWS S3 for data lake storage
- **Automated testing** — 43 total checks (25 pytest + 18 dbt) run on every code change via GitHub Actions

---

## Data Pipeline DAG

```
ingest_usajobs ──┐
                 ├──→ clean_jobs → extract_skills → upload_to_s3
ingest_adzuna  ──┘
```

Both ingestion tasks run in parallel. Clean and extract only run after both sources complete.

---

## dbt Models

| Model | Layer | Type | Description |
|---|---|---|---|
| `stg_jobs` | Staging | View | Filters nulls, renames columns |
| `int_jobs_cleaned` | Intermediate | View | Adds seniority level, work type, salary flags |
| `int_skills_extracted` | Intermediate | View | Joins skills with job context |
| `mart_skill_trends` | Mart | Table | Weekly skill counts ranked by frequency |

---

## Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.12+
- AWS account (for S3 and RDS)

### Environment Variables
Create a `.env` file in the project root:
```
USAJOBS_API_KEY=your_key
USAJOBS_EMAIL=your_email
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_BUCKET_NAME=your_bucket
AWS_REGION=us-west-1
RDS_HOST=your_rds_endpoint
RDS_PORT=5432
RDS_NAME=postgres
RDS_USER=pipeline_user
RDS_PASSWORD=your_password
DB_PASSWORD=pipeline_pass
```

### Run Locally
```bash
docker-compose up -d
# Access Airflow UI at http://localhost:8080
# Username: airflow | Password: airflow
```

### Run dbt
```bash
cd job_market_dbt
dbt run          # local development
dbt run --target prod   # AWS RDS
dbt test         # run 18 data quality checks
```

### Run Tests
```bash
pip install pytest psycopg2-binary requests
pytest tests/ -v
```

---

## Status

Active development — April 2026

**Completed:**
- Dual-source ingestion pipeline (USAJobs + Adzuna)
- Full Medallion Architecture with dbt
- AWS S3 + RDS cloud integration
- Keyword-based skill extraction
- 43 automated tests + GitHub Actions CI

**In progress:**
- OpenAI LLM skill extraction
- Streamlit dashboard
- Prophet forecasting model
- Tableau Public version
