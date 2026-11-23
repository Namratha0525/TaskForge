CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    request_key varchar(128) COLLATE "C" UNIQUE NOT NULL,
    kind varchar(20) NOT NULL,
    payload text NOT NULL,
    priority integer NOT NULL CHECK (priority BETWEEN 0 AND 9),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 5),
    attempts integer NOT NULL DEFAULT 0,
    state varchar(12) NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    lease_token uuid,
    lease_until timestamptz,
    last_error text
);
CREATE INDEX IF NOT EXISTS job_priority_idx ON jobs (priority DESC, created_at, id)
    WHERE state = 'PENDING';
CREATE INDEX IF NOT EXISTS job_lease_idx ON jobs (lease_until) WHERE state = 'RUNNING';
CREATE INDEX IF NOT EXISTS job_state_idx ON jobs (state, created_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS job_effects (
    job_id uuid PRIMARY KEY REFERENCES jobs(id),
    result text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
