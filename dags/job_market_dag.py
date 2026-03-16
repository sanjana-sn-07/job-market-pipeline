from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingest import pull_from_usajobs, insert_jobs

default_args = {
    'owner':            'sanjana',
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='job_market_pipeline',
    default_args=default_args,
    description='Daily ingestion of job postings from USAJobs API',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=['job-market', 'ingestion'],
) as dag:

    def ingest_and_store():
        jobs = pull_from_usajobs(keyword="data engineer", results_per_page=100)
        insert_jobs(jobs)
        print(f"Pipeline complete — processed {len(jobs)} jobs")

    run_pipeline = PythonOperator(
        task_id='ingest_usajobs',
        python_callable=ingest_and_store,
    )