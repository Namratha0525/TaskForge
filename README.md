# TaskForge

TaskForge is a Spring Boot background job platform. It accepts jobs over HTTP and runs simulated invoice, report and email handlers on a configurable worker pool. PostgreSQL is the source of truth. Redis Pub/Sub can wake workers across instances.

## Run with PostgreSQL

```sh
docker compose up --build -d
curl -fsS http://localhost:8081/health
python3 smoke.py
```

Wait for the health endpoint before running the smoke test. To enable Redis wakeups and scale worker processes:

```sh
docker compose --profile redis up -d --wait redis
REDIS_ENABLED=true docker compose --profile redis up --build -d --scale worker=2
```

Each worker process runs three threads by default. Set `WORKERS` to change the number, or use zero for a submission-only process.

## Submit and monitor

```sh
curl -sS http://localhost:8081/jobs \
  -H 'Authorization: Bearer local-taskforge-demo-token' \
  -H 'Idempotency-Key: invoice-order-42' \
  -H 'Content-Type: application/json' \
  -d '{"kind":"INVOICE","payload":"order-42","priority":8,"maxAttempts":3}'
```

Use `GET /jobs/{id}` to inspect a job and its result. Use `GET /jobs?state=FAILED&limit=25` for recent failures and `GET /stats` for counts by state. Send the same bearer token on all job and stats routes. The token represents a single local operator. Send `RETRY_DEMO` to force the first attempt to fail. With `maxAttempts=1`, the job ends in `FAILED`.

## Architecture

```mermaid
flowchart TD
    API[Spring Boot API] --> Store[(PostgreSQL jobs and results)]
    API --> Redis[Optional Redis wakeups]
    Redis --> Workers[Worker processes and threads]
    Workers --> Store
```

1. The API validates requests and commits the job in PostgreSQL. The unique key maps repeated submissions to one job. A changed request with the same key returns HTTP 409.
2. An idle worker claims the highest-priority eligible unlocked job. SQL uses an ordered index and `FOR UPDATE SKIP LOCKED`. Created time and ID provide tie-breakers. Jobs already running continue when a higher-priority job arrives.
3. The claim increments attempts and assigns a lease token and expiry. Completion uses that token as a fencing check. An expired attempt cannot commit an effect after another worker claims the job.
4. A failed attempt waits 1, 2, 4 or 8 seconds according to the attempt number. Expired claims use the same retry budget. Workers recover expired jobs in batches.
5. A worker marks a job completed and writes its simulated result in one transaction. The result table has one row per job. Running an external operation would require downstream idempotency or a transactional outbox, since a local transaction cannot undo a real email or external request.
6. The API sends a Redis wakeup after PostgreSQL commits. A missed message or a Redis outage during operation leaves the job discoverable through polling.

## Milestones to reproduce

| Milestone | Demonstration |
| --- | --- |
| PostgreSQL API | Submit a job, repeat the same key, and observe one ID. |
| Concurrency and priority | Stop the worker, submit two report jobs with different priorities, then start a single worker. Check that the high-priority report completes first. |
| Retry and recovery | Submit `RETRY_DEMO` and inspect the attempt count. For crash recovery, kill the worker while a report is `RUNNING`, then restart it. |
| Redis and CI | Enable Redis, scale workers, and run the integration smoke suite. GitHub Actions runs the smoke suite in both Redis configurations. |

## Interview practice

- **Where is the priority queue?** SQL orders ready jobs by priority and creation time. An in-memory heap would lose its state on restart and would need coordination across workers.
- **What if two workers pick the same job?** The database locks one candidate row during claim and other workers skip it.
- **Why keep PostgreSQL if you use Redis?** PostgreSQL stores durable queue state, leases and effects. Redis only signals that new work may be ready.
- **Is processing exactly once?** An attempt can run more than once after a crash. The simulated database effect is written once per job through fencing, a transaction and a unique key.
- **What would you change for long-running or dependent jobs?** Use lease renewal for long-running jobs. For dependencies, validate cycles when adding edges and enqueue a job when all prerequisites complete.

## Render demo

The Blueprint in `render.yaml` defines a free web service and a free PostgreSQL database in Singapore. The API and a single worker thread share the web service. Redis stays disabled for this profile.

When setting up the Blueprint, set `DATABASE_URL` to the JDBC form of the internal Postgres address, `jdbc:postgresql://INTERNAL_HOST:5432/taskforge`. If the database host is still being assigned, enter `jdbc:postgresql://localhost:5432/taskforge` during initial setup and replace it in the web service environment once the database has been created. Render supplies the database user, database password, and a randomly generated API token. Retrieve the token privately from the service's environment settings to call authenticated endpoints. Check `/health` and then run `smoke.py` against the deployed service using `TASKFORGE_URL` and `TASKFORGE_TOKEN`.

Free web services can sleep, which pauses job processing until the site wakes. Free Render PostgreSQL databases expire after 30 days. See [Render's free plan limits](https://render.com/docs/free).
