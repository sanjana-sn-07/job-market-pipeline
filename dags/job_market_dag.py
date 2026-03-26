from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingest import pull_from_usajobs, insert_jobs
from ingest_adzuna import pull_from_adzuna, insert_adzuna_jobs
from clean import clean_and_store
from extract_skills import extract_and_store

default_args = {
    'owner':            'sanjana',
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='job_market_pipeline',
    default_args=default_args,
    description='Daily ingestion and cleaning of job postings from USAJobs API',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=['job-market', 'ingestion'],
) as dag:

    def ingest_and_store():
        keywords = ["data engineer", "data analyst"]
        total = 0
        for keyword in keywords:
            jobs = pull_from_usajobs(keyword=keyword, results_per_page=25)
            insert_jobs(jobs)
            total += len(jobs)
        print(f"Ingestion complete — processed {total} jobs across {len(keywords)} keywords")

    def ingest_adzuna():
        keywords = ["data engineer", "data analyst"]
        total = 0
        for keyword in keywords:
            jobs = pull_from_adzuna(keyword=keyword, results_per_page=25)
            insert_adzuna_jobs(jobs)
            total += len(jobs)
        print(f"Adzuna ingestion complete — processed {total} jobs")

    ingest_usajobs_task = PythonOperator(
        task_id='ingest_usajobs',
        python_callable=ingest_and_store,
    )

    ingest_adzuna_task = PythonOperator(
        task_id='ingest_adzuna',
        python_callable=ingest_adzuna,
    )

    clean_task = PythonOperator(
        task_id='clean_jobs',
        python_callable=clean_and_store,
    )

    skills_task = PythonOperator(
        task_id='extract_skills',
        python_callable=extract_and_store,
    )

    [ingest_usajobs_task, ingest_adzuna_task] >> clean_task >> skills_task