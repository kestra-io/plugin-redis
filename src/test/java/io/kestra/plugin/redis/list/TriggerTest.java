package io.kestra.plugin.redis.list;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

class TriggerTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_trigger";
    }

    @BeforeEach
    void setUp() throws Exception {
        push();
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        Integer count = (Integer) execution.getTrigger().getVariables().get("count");
        assertThat(count, greaterThanOrEqualTo(2));
    }

    @Test
    void shouldUnblockInFlightEvaluateOnKill() throws Exception {
        // Key with no data: evaluate() must not hold the calling thread past maxDuration once
        // kill() closes the Redis connection the underlying ListPop is running on.
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mytriggerkey_kill_" + IdUtils.create()))
            .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        var result = new AtomicReference<Optional<Execution>>();
        Thread runner = new Thread(() -> {
            try {
                result.set(trigger.evaluate(conditionContext, null));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        // Give evaluate() time to build its ListPop task and open the Redis connection.
        Thread.sleep(Duration.ofMillis(200).toMillis());

        long killStart = System.currentTimeMillis();
        trigger.kill();
        long killElapsedMs = System.currentTimeMillis() - killStart;

        assertThat("Trigger.kill() must not block for the full maxDuration", killElapsedMs, lessThan(15000L));
        assertThat("evaluate() must return promptly after kill()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed evaluate() must not be reported as a trigger error", thrown.get(), nullValue());
        assertThat("A killed evaluate() must not fire an execution", result.get().isPresent(), is(false));
    }
}
