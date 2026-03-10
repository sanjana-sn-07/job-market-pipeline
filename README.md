# job-market-pipeline
End-to-end data pipeline for job market analytics with Airflow, dbt, and AI-powered skill extraction


# Job Market Analytics Pipeline
An end-to-end data pipeline that ingests thousands of tech job postings daily,
transforms them using dbt, extracts required skills using an LLM, and serves
insights via an interactive Streamlit dashboard.
## Architecture
> diagram coming soon
## Tech Stack
- **Ingestion:** Python, Apache Airflow
- **Transformation:** dbt
- **Storage:** PostgreSQL, AWS S3, AWS RDS
- **AI Layer:** OpenAI API (skill extraction)
- **Dashboard:** Streamlit
- **Infrastructure:** Docker, AWS EC2
## Features (in progress)
- [ ] Daily ingestion from USAJobs and Adzuna APIs
- [ ] dbt models for cleaning and aggregating job data
- [ ] LLM-powered skill extraction from job descriptions
- [ ] Interactive dashboard showing skill demand trends over time
- [ ] Tableau Public version for stakeholder-facing reporting
## Setup
> instructions coming as project is built
## Status
🚧 In active development — March 2026
