package taskforge;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class JobStore {
    public enum State { PENDING, RUNNING, COMPLETED, FAILED }
    public enum Kind { INVOICE, REPORT, SIMULATED_EMAIL, RETRY_DEMO }

    public record Job(UUID id, Kind kind, String payload, int priority, int maxAttempts,
                      int attempts, State state, OffsetDateTime createdAt,
                      OffsetDateTime availableAt, OffsetDateTime finishedAt, String lastError) {}

    record Submission(Job job, boolean replayed) {}
    record Claim(Job job, UUID token) {}

    private final JdbcTemplate jdbc;

    JobStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public Submission submit(String key, Kind kind, String payload, int priority, int maxAttempts) {
        UUID id = UUID.randomUUID();
        int inserted = jdbc.update("""
            INSERT INTO jobs (id, request_key, kind, payload, priority, max_attempts)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (request_key) DO NOTHING
            """, id, key, kind.name(), payload, priority, maxAttempts);
        Job job = jdbc.queryForObject("SELECT * FROM jobs WHERE request_key = ?", this::map, key);
        if (job.kind() != kind || !job.payload().equals(payload) || job.priority() != priority
                || job.maxAttempts() != maxAttempts) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Key already used for another job");
        }
        return new Submission(job, inserted == 0);
    }

    Optional<Claim> claim(int leaseSeconds) {
        UUID token = UUID.randomUUID();
        return jdbc.query("""
            WITH candidate AS (
                SELECT id FROM jobs WHERE state = 'PENDING' AND available_at <= now()
                ORDER BY priority DESC, created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs j SET state = 'RUNNING', attempts = attempts + 1,
                lease_token = ?, lease_until = now() + make_interval(secs => ?)
            FROM candidate c WHERE j.id = c.id RETURNING j.*
            """, this::map, token, leaseSeconds)
                .stream().findFirst().map(job -> new Claim(job, token));
    }

    @Transactional
    public boolean complete(Claim claim, String result) {
        int updated = jdbc.update("""
            UPDATE jobs SET state = 'COMPLETED', finished_at = now(), last_error = NULL,
                lease_token = NULL, lease_until = NULL
            WHERE id = ? AND state = 'RUNNING' AND lease_token = ? AND lease_until > now()
            """, claim.job().id(), claim.token());
        if (updated == 0) {
            return false;
        }
        jdbc.update("INSERT INTO job_effects (job_id, result) VALUES (?, ?)", claim.job().id(), result);
        return true;
    }

    void fail(Claim claim, String error) {
        jdbc.update("""
            UPDATE jobs SET state = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
                available_at = now() + make_interval(secs => (1 << (attempts - 1))),
                finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                last_error = ?, lease_token = NULL, lease_until = NULL
            WHERE id = ? AND state = 'RUNNING' AND lease_token = ? AND lease_until > now()
            """, error, claim.job().id(), claim.token());
    }

    void recoverExpired() {
        jdbc.update("""
            WITH expired AS (
                SELECT id FROM jobs WHERE state = 'RUNNING' AND lease_until <= now()
                ORDER BY lease_until LIMIT 100 FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs j SET state = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
                available_at = now() + make_interval(secs => (1 << (attempts - 1))),
                finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                last_error = 'Worker lease expired', lease_token = NULL, lease_until = NULL
            FROM expired e WHERE j.id = e.id
            """);
    }

    Optional<Job> find(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = ?", this::map, id).stream().findFirst();
    }

    List<Job> recent(State state, int limit) {
        if (state == null) {
            return jdbc.query("SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT ?",
                    this::map, limit);
        }
        return jdbc.query("SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                this::map, state.name(), limit);
    }

    List<Map<String, Object>> counts() {
        return jdbc.queryForList("SELECT state, count(*) AS count FROM jobs GROUP BY state ORDER BY state");
    }

    Optional<String> result(UUID id) {
        return jdbc.query("SELECT result FROM job_effects WHERE job_id = ?",
                (row, index) -> row.getString(1), id).stream().findFirst();
    }

    private Job map(ResultSet row, int index) throws SQLException {
        return new Job(row.getObject("id", UUID.class), Kind.valueOf(row.getString("kind")),
                row.getString("payload"), row.getInt("priority"), row.getInt("max_attempts"),
                row.getInt("attempts"), State.valueOf(row.getString("state")),
                row.getObject("created_at", OffsetDateTime.class),
                row.getObject("available_at", OffsetDateTime.class),
                row.getObject("finished_at", OffsetDateTime.class), row.getString("last_error"));
    }
}
