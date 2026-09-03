-- Compares the keyword scanner against the LLM on an equal footing.
--
-- The keyword scanner runs over every ingested posting, but the LLM step is
-- capped at a batch of Adzuna postings per run. Counting each method over its
-- own population made 'keyword only' look dominant by construction, so both
-- sides are restricted to the jobs the LLM actually attempted. That set is a
-- subset of what the keyword scanner has seen, because extract_skills always
-- runs before extract_skills_llm in the DAG.
--
-- A '__processed__' sentinel row marks a job the LLM attempted but found no
-- skills in, so such a job counts toward the population but never toward a
-- skill count.
--
-- Both sides are also mapped through the skill_aliases seed so the LLM's
-- 'apache spark' matches the keyword list's 'spark' instead of looking like a
-- skill only the LLM found.

with comparable_jobs as (
    select distinct job_id
    from llm_extracted_skills
),

population as (
    select count(*) as comparable_job_count
    from comparable_jobs
),

keyword_skills as (
    select
        coalesce(a.canonical_skill, lower(js.skill)) as skill,
        js.job_id
    from job_skills js
    join comparable_jobs c
      on js.job_id = c.job_id
    left join {{ ref('skill_aliases') }} a
      on lower(js.skill) = a.alias
    where js.skill != '__processed__'
),

llm_skills as (
    select
        coalesce(a.canonical_skill, lower(l.skill)) as skill,
        l.job_id
    from llm_extracted_skills l
    left join {{ ref('skill_aliases') }} a
      on lower(l.skill) = a.alias
    where l.skill != '__processed__'
),

keyword_counts as (
    select skill, count(distinct job_id) as keyword_job_count
    from keyword_skills
    group by skill
),

llm_counts as (
    select skill, count(distinct job_id) as llm_job_count
    from llm_skills
    group by skill
),

agreement as (
    -- jobs where both methods independently found the same skill
    select k.skill, count(distinct k.job_id) as agreement_job_count
    from keyword_skills k
    join llm_skills l
      on k.skill = l.skill
     and k.job_id = l.job_id
    group by k.skill
),

combined as (
    select
        coalesce(k.skill, l.skill)       as skill,
        coalesce(k.keyword_job_count, 0) as keyword_job_count,
        coalesce(l.llm_job_count, 0)     as llm_job_count
    from keyword_counts k
    full outer join llm_counts l
      on k.skill = l.skill
)

select
    c.skill,
    c.keyword_job_count,
    c.llm_job_count,
    coalesce(ag.agreement_job_count, 0) as agreement_job_count,
    p.comparable_job_count,
    case
        when c.keyword_job_count > 0 and c.llm_job_count > 0 then 'both'
        when c.keyword_job_count > 0                        then 'keyword only'
        else 'llm only'
    end as extraction_source
from combined c
cross join population p
left join agreement ag
  on c.skill = ag.skill
order by greatest(c.keyword_job_count, c.llm_job_count) desc, c.skill asc
