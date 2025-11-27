-- DATABASE jobqueue;
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'running' | 'done' | 'failed'
    payload JSONB NOT NULL,
    idempotency_key TEXT,
    attempt_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    leased_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure (worker_id, idempotency_key) is unique when idempotency_key is set
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_worker_id_idempotency
ON jobs(worker_id, idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Speed up lookup of jobs for workers / leasing
CREATE INDEX IF NOT EXISTS idx_jobs_status_leased
ON jobs(status, leased_until);
