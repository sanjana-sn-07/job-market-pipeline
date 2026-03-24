-- raw ingestion layer
CREATE TABLE IF NOT EXISTS raw_jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE,
    title VARCHAR(500),
    company VARCHAR(255),
    location VARCHAR(255),
    description TEXT,
    salary_min INTEGER,
    salary_max INTEGER,
    posted_date DATE,
    source VARCHAR(50),
    raw_json JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- cleaned / processed layer
CREATE TABLE IF NOT EXISTS processed_jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE,
    title VARCHAR(500),
    title_normalized VARCHAR(500),
    company VARCHAR(255),
    location VARCHAR(255),
    description_clean TEXT,
    source VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- skill tags — one row per skill per job
CREATE TABLE IF NOT EXISTS job_skills (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255),
    skill VARCHAR(100),
    source VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(job_id, skill)
);
