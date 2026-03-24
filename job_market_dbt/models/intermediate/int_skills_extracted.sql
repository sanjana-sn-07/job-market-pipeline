with jobs as (
    select
        job_id,
        job_title,
        company_name,
        job_location,
        job_description,
        data_source,
        ingested_date
    from {{ ref('int_jobs_cleaned') }}
),

skills as (
    select
        job_id,
        skill,
        source,
        ingested_at::date as ingested_date
    from job_skills
),

joined as (
    select
        s.job_id,
        j.job_title,
        j.company_name,
        j.job_location,
        j.data_source,
        s.skill,
        s.ingested_date
    from skills s
    left join jobs j on s.job_id = j.job_id
)

select * from joined
