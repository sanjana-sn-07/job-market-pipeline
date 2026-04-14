with keyword_skills as (
    select
        skill,
        count(distinct job_id) as keyword_job_count
    from job_skills
    where skill != '__processed__'
    group by skill
),

llm_skills as (
    select
        skill,
        count(distinct job_id) as llm_job_count
    from llm_extracted_skills
    where skill != '__processed__'
    group by skill
),

combined as (
    select
        coalesce(k.skill, l.skill)   as skill,
        coalesce(k.keyword_job_count, 0) as keyword_job_count,
        coalesce(l.llm_job_count, 0)     as llm_job_count
    from keyword_skills k
    full outer join llm_skills l on lower(k.skill) = lower(l.skill)
)

select
    skill,
    keyword_job_count,
    llm_job_count,
    keyword_job_count + llm_job_count as total_job_count,
    case
        when keyword_job_count > 0 and llm_job_count > 0 then 'both'
        when keyword_job_count > 0 then 'keyword only'
        else 'llm only'
    end as extraction_source
from combined
order by total_job_count desc
