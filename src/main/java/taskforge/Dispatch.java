package taskforge;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
class Dispatch {
    private static final String CHANNEL = "taskforge:job-ready";
    private final Semaphore wake = new Semaphore(0);
    private final StringRedisTemplate redis;
    private final boolean enabled;
    private final int workerCount;

    Dispatch(StringRedisTemplate redis, @Value("${taskforge.redis.enabled}") boolean enabled,
             @Value("${taskforge.workers}") int workerCount) {
        this.redis = redis;
        this.enabled = enabled;
        this.workerCount = workerCount;
    }

    void publish() {
        signal();
        if (enabled) {
            try {
                redis.convertAndSend(CHANNEL, "ready");
            } catch (RuntimeException error) {
                // Other instances will discover the durable job on their next poll.
                System.err.println("Redis notification unavailable");
            }
        }
    }

    synchronized void signal() {
        int needed = workerCount - wake.availablePermits();
        if (needed > 0) {
            wake.release(needed);
        }
    }

    void awaitWork() throws InterruptedException {
        wake.tryAcquire(500, TimeUnit.MILLISECONDS);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "taskforge.redis.enabled", havingValue = "true")
    static class RedisWakeup {
        @Bean
        RedisMessageListenerContainer wakeupListener(RedisConnectionFactory connections,
                                                     Dispatch dispatch) {
            var listener = new RedisMessageListenerContainer();
            listener.setConnectionFactory(connections);
            listener.addMessageListener((message, pattern) -> dispatch.signal(), new ChannelTopic(CHANNEL));
            return listener;
        }
    }
}
