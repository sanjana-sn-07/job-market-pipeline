with base as (
    select
        job_id,
        job_title,
        company_name,
        job_location,
        job_description,
        data_source,
        ingested_date,
        ingested_at
    from {{ ref('stg_jobs') }}
),

enriched as (
    select
        job_id,
        job_title,
        company_name,
        job_location,
        job_description,
        data_source,
        ingested_date,
        ingested_at,

        -- salary range extraction
        case
            when lower(job_description) like '%$%'
              or lower(job_description) like '%salary%'
              or lower(job_description) like '%compensation%'
            then true
            else false
        end as has_salary_info,

        -- seniority level classification
        case
            when lower(job_title) like '%senior%'
              or lower(job_title) like '%sr%'
              or lower(job_title) like '%lead%'
              or lower(job_title) like '%principal%'
              or lower(job_title) like '%staff%'
            then 'Senior'
            when lower(job_title) like '%junior%'
              or lower(job_title) like '%jr%'
              or lower(job_title) like '%entry%'
            then 'Junior'
            when lower(job_title) like '%manager%'
              or lower(job_title) like '%director%'
              or lower(job_title) like '%head%'
              or lower(job_title) like '%vp%'
            then 'Manager'
            else 'Mid-Level'
        end as seniority_level,

        -- remote / onsite flag
        case
            when lower(job_location) like '%remote%'
              or lower(job_description) like '%fully remote%'
              or lower(job_description) like '%work from home%'
              or lower(job_description) like '%100% remote%'
            then 'Remote'
            when lower(job_description) like '%hybrid%'
            then 'Hybrid'
            else 'Onsite'
        end as work_type

    from base
)

select * from enriched
