package taskforge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

@Component
class Workers implements SmartLifecycle {
    private final JobStore store;
    private final Dispatch dispatch;
    private final int size;
    private final int leaseSeconds;
    private volatile boolean running;
    private ExecutorService pool;

    Workers(JobStore store, Dispatch dispatch, @Value("${taskforge.workers}") int size,
            @Value("${taskforge.lease-seconds}") int leaseSeconds) {
        if (size < 0 || size > 32 || leaseSeconds < 3 || leaseSeconds > 120) {
            throw new IllegalArgumentException("Use 0–32 workers and a 3–120 second lease");
        }
        this.store = store;
        this.dispatch = dispatch;
        this.size = size;
        this.leaseSeconds = leaseSeconds;
    }

    @Override
    public void start() {
        if (size == 0) {
            return;
        }
        running = true;
        pool = Executors.newFixedThreadPool(size);
        for (int index = 0; index < size; index++) {
            pool.submit(this::work);
        }
    }

    @Override
    public void stop() {
        running = false;
        if (pool == null) {
            return;
        }
        pool.shutdownNow();
        try {
            pool.awaitTermination(4, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() { return running; }

    private void work() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                store.recoverExpired();
                var claim = store.claim(leaseSeconds);
                if (claim.isPresent()) {
                    execute(claim.get());
                } else {
                    dispatch.awaitWork();
                }
            } catch (DataAccessException error) {
                System.err.println("Job database operation unavailable");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException stopped) {
                    Thread.currentThread().interrupt();
                }
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void execute(JobStore.Claim claim) throws InterruptedException {
        String result;
        try {
            result = perform(claim.job());
        } catch (RuntimeException error) {
            store.fail(claim, error instanceof DemoFailure
                    ? "Simulated transient failure" : "Handler execution failed");
            return;
        }
        store.complete(claim, result);
    }

    private String perform(JobStore.Job job) throws InterruptedException {
        // The handler computes a result. The store commits the durable effect.
        Thread.sleep(job.kind() == JobStore.Kind.REPORT ? 1500 : 100);
        if (job.kind() == JobStore.Kind.RETRY_DEMO && job.attempts() == 1) {
            throw new DemoFailure();
        }
        return switch (job.kind()) {
            case INVOICE -> "Generated simulated invoice for " + job.payload();
            case REPORT -> "Generated simulated report for " + job.payload();
            case SIMULATED_EMAIL -> "Recorded simulated email for " + job.payload();
            case RETRY_DEMO -> "Succeeded after transient failure";
        };
    }

    private static final class DemoFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
