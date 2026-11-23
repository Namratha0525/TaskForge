package taskforge;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class JobApi {
    record JobRequest(@NotNull JobStore.Kind kind, @NotBlank @Size(max = 2000) String payload,
                      @NotNull @Min(0) @Max(9) Integer priority,
                      @NotNull @Min(1) @Max(5) Integer maxAttempts) {}
    record JobDetails(JobStore.Job job, String result) {}

    private final JobStore store;
    private final Dispatch dispatch;
    private final byte[] expected;

    JobApi(JobStore store, Dispatch dispatch, @Value("${taskforge.token}") String token) {
        if (token.length() < 16) {
            throw new IllegalArgumentException("TASKFORGE_TOKEN needs at least 16 characters");
        }
        this.store = store;
        this.dispatch = dispatch;
        this.expected = ("Bearer " + token).getBytes(StandardCharsets.UTF_8);
    }

    @PostMapping("/jobs")
    ResponseEntity<JobStore.Job> submit(
            @RequestHeader(value = "Authorization", required = false) String auth,
            @RequestHeader(value = "Idempotency-Key", required = false) String key,
            @Valid @RequestBody JobRequest request) {
        authorize(auth);
        if (key == null || !key.matches("[!-~]{1,128}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid Idempotency-Key");
        }
        var submission = store.submit(key, request.kind(), request.payload(),
                request.priority(), request.maxAttempts());
        if (!submission.replayed()) {
            dispatch.publish();
        }
        return ResponseEntity.status(submission.replayed() ? 200 : 202).body(submission.job());
    }

    @GetMapping("/jobs/{id}")
    JobDetails get(@RequestHeader(value = "Authorization", required = false) String auth,
                   @PathVariable UUID id) {
        authorize(auth);
        // Fetch the result only if the job snapshot already shows the committed transition.
        var job = store.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        String result = job.state() == JobStore.State.COMPLETED ? store.result(id).orElseThrow() : null;
        return new JobDetails(job, result);
    }

    @GetMapping("/jobs")
    List<JobStore.Job> list(@RequestHeader(value = "Authorization", required = false) String auth,
                            @RequestParam(required = false) JobStore.State state,
                            @RequestParam(defaultValue = "25") int limit) {
        authorize(auth);
        if (limit < 1 || limit > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Limit must be 1–100");
        }
        return store.recent(state, limit);
    }

    @GetMapping("/stats")
    List<Map<String, Object>> stats(
            @RequestHeader(value = "Authorization", required = false) String auth) {
        authorize(auth);
        return store.counts();
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "up");
    }

    private void authorize(String auth) {
        if (auth == null || !MessageDigest.isEqual(expected, auth.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Bearer token required");
        }
    }
}
