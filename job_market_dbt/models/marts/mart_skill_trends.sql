with skills as (
    select
        skill,
        data_source,
        ingested_date,
        date_trunc('week', ingested_date::timestamp) as week_start
    from {{ ref('int_skills_extracted') }}
    where skill is not null
),

weekly_counts as (
    select
        skill,
        data_source,
        week_start,
        count(*) as job_count
    from skills
    group by skill, data_source, week_start
),

ranked as (
    select
        skill,
        data_source,
        week_start,
        job_count,
        rank() over (
            partition by week_start
            order by job_count desc
        ) as skill_rank
    from weekly_counts
)

select
    skill,
    data_source,
    week_start,
    job_count,
    skill_rank
from ranked
order by week_start desc, skill_rank asc
