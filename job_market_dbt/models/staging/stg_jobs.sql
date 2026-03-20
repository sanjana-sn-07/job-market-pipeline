with source as (
    select
        job_id,
        title,
        title_normalized,
        company,
        location,
        description_clean,
        source,
        ingested_at
    from processed_jobs
    where job_id is not null
      and title is not null
      and description_clean is not null
      and description_clean != ''
),

renamed as (
    select
        job_id,
        title_normalized                              as job_title,
        company                                       as company_name,
        location                                      as job_location,
        description_clean                             as job_description,
        source                                        as data_source,
        date(ingested_at)                             as ingested_date,
        ingested_at
    from source
)

select * from renamed
