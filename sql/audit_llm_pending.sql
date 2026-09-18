-- Read-only. Run separately against local PostgreSQL AND RDS before enabling the
-- uncapped LLM task. It reports how many existing processed jobs are pending.
SELECT p.source, COUNT(*) AS pending_llm_jobs
FROM processed_jobs AS p
WHERE p.job_id IS NOT NULL
  AND p.source IN ('usajobs', 'adzuna')
  AND NOT EXISTS (
      SELECT 1
      FROM llm_extracted_skills AS l
      WHERE l.job_id = p.job_id
  )
GROUP BY p.source
ORDER BY p.source;
