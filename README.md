# Job Market Analytics Pipeline

![CI](https://github.com/sanjana-sn-07/job-market-pipeline/actions/workflows/ci.yml/badge.svg)

An end-to-end data engineering pipeline that ingests up to 100 tech job postings daily from two APIs (USAJobs, Adzuna), transforms them through a Medallion Architecture using dbt, extracts required skills using keyword matching and LLM enrichment, and serves insights via an interactive Streamlit dashboard with 6-month skill demand forecasting.

📹 **Demo:** [Watch Loom walkthrough](https://loom.com/share/6e23d87debfd44d99df5e17e2c725b44)
📝 **Blog post:** [How I Built a Job Market Analytics Pipeline with Airflow, dbt, GPT-4o-mini, and Prophet](https://medium.com/@sanjana.sn.07/how-i-built-a-job-market-analytics-pipeline-with-airflow-dbt-gpt-4o-mini-and-prophet-d4c7e8c12253)

---

## Architecture

![Pipeline Architecture](docs/architecture.png)

---

## Tech Stack

| Layer | Tools |
|---|---|
| Ingestion | Python, Apache Airflow, requests |
| Transformation | dbt (5 models, 1 seed, 23 data quality tests) |
| Storage | PostgreSQL, AWS S3, AWS RDS |
| AI Layer | OpenAI API — LLM skill extraction from job descriptions |
| ML Layer | Facebook Prophet — 6-month skill demand forecasting |
| Dashboard | Streamlit, Tableau Public |
| Testing | pytest (25 unit tests), dbt tests (23 data quality checks) |
| CI/CD | GitHub Actions — runs pytest on every push |
| Infrastructure | Docker, Docker Compose, AWS (S3, RDS, EC2) |

---

## Features

- **Dual-source ingestion** — pulls job postings daily from USAJobs (government) and Adzuna (private sector) APIs
- **Medallion Architecture** — Bronze (raw) → Silver (cleaned) → Gold (aggregated) data layers
- **dbt transformation pipeline** — 5 models with full lineage tracking and 23 automated data quality tests, run as a `dbt build` task inside the Airflow DAG
- **Keyword skill extraction** — regex-based extraction of 48 skills from job descriptions
- **LLM skill extraction** — OpenAI GPT-4o-mini enrichment to catch skills missed by keyword matching
- **Government vs private sector comparison** — side-by-side skill demand analysis across data sources
- **6-month forecasting** — Facebook Prophet ML model predicts which skills will be most in demand
- **Interactive dashboard** — Streamlit app with skill trend charts, filters, and forecast visualization
- **Tableau Public version** — stakeholder-facing dashboard for DA role applications
- **Cloud infrastructure** — AWS RDS for production database, AWS S3 for data lake storage
- **Automated testing** — 48 total checks: 25 pytest unit tests run in GitHub Actions on every push, and 23 dbt data quality tests run daily inside the DAG as part of `dbt build`

---

## Data Pipeline DAG

8 tasks, `@daily`, `catchup=False`, 2 retries with a 5-minute delay:

```
ingest_usajobs ──┐
                 ├──→ clean_jobs → extract_skills → extract_skills_llm → upload_to_s3 → run_dbt → run_forecast
ingest_adzuna  ──┘
```

Both ingestion tasks run in parallel and fan in — cleaning only starts once both sources succeed.
`run_dbt` executes `dbt build`, which runs the seed, the models and the 23 data quality tests together in
dependency order — so a failing test blocks the models downstream of it, the task fails, and
`run_forecast` never runs on bad data.

---

## dbt Models

| Model | Layer | Type | Description |
|---|---|---|---|
| `stg_jobs` | Staging | View | Filters nulls, renames columns |
| `int_jobs_cleaned` | Intermediate | View | Adds seniority level, work type, salary flags |
| `int_skills_extracted` | Intermediate | View | Joins skills with job context |
| `mart_skill_trends` | Mart | Table | Weekly skill counts ranked by frequency |
| `mart_llm_vs_keyword_skills` | Mart | Table | Compares LLM vs keyword extraction on a shared job population, with alias normalization |
| `skill_aliases` | Seed | Table | Maps LLM skill variants onto canonical names (`apache spark` → `spark`) |

---

## Live Dashboards

| Dashboard | Link |
|---|---|
| 📊 Tableau Public | [View Dashboard](https://public.tableau.com/app/profile/sanjana.sringari.nataraju/viz/JobMarketAnalyticsDashboardPoweredbyAirflowdbtProphetML/Dashboard1) |
| 🖥️ Streamlit | Run locally (see setup below) |

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
OPENAI_API_KEY=your_openai_key   # only needed if not using AWS Secrets Manager
```

`job_market_dbt/profiles.yml` is committed and reads these same environment variables via
dbt's `env_var()`, so no credentials live in the repo. Its defaults point at the local Postgres
published on host port 5433; inside the Airflow containers docker-compose supplies
`DB_HOST=project-db` and `DB_PORT=5432`.

### Run Locally
```bash
docker compose build    # first run only, or after changing requirements-*.txt
docker compose up -d
# Access Airflow UI at http://localhost:8080
# Username: airflow | Password: airflow
```

The stack runs a **custom Airflow image** (`Dockerfile`) rather than the stock one, because
dependencies pip-installed at container start were slow and non-deterministic — and could not
install the pinned dbt version at all, since `apache/airflow:2.8.0` defaults to Python 3.8 while
`dbt-core` 1.11 requires Python ≥ 3.9. The image pins the same Airflow version on the
`python3.11` variant, so no metadata database migration is needed.

dbt is installed into an **isolated virtualenv** at `/opt/dbt-venv`, and the DAG calls
`/opt/dbt-venv/bin/dbt` by absolute path. dbt and Airflow share transitive dependencies
(Jinja2, click, protobuf) with incompatible pins, so separate environments mean neither can
break the other. Dependencies are split across `requirements-airflow.txt` (imported by the
PythonOperator tasks) and `requirements-dbt.txt` (pinned to match local development).

### Run dbt
The DAG runs dbt automatically as the `run_dbt` task. To run it by hand:
```bash
cd job_market_dbt
dbt build --profiles-dir .                 # seed + 5 models + 23 tests, in dependency order
dbt build --profiles-dir . --target prod   # same, against AWS RDS
dbt test  --profiles-dir .                 # tests only
```

### Run Tests
```bash
pip install pytest psycopg2-binary requests
pytest tests/ -v
```

---

## Status

✅ Complete — April 2026

- Dual-source ingestion pipeline (USAJobs + Adzuna)
- 8-task Airflow DAG — ingestion through dbt transformation to forecasting, fully automated
- Full Medallion Architecture with dbt (5 models, 1 seed, 23 tests)
- AWS S3 + RDS cloud integration + AWS Secrets Manager
- Keyword-based + LLM (GPT-4o-mini) skill extraction
- Facebook Prophet 6-month forecasting model
- Streamlit interactive dashboard
- Tableau Public dashboard (live URL above)
- 48 automated tests + GitHub Actions CI/CD
