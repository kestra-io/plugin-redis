package io.kestra.plugin.redis.stream;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

// Runs sanity-checks/stream-recovery.yaml: a crashed worker's entries are taken over, processed and acknowledged.
@KestraTest(startRunner = true)
class StreamRecoveryRunnerTest {
    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String KEY = "stream_recovery_runner_key";

    @BeforeEach
    void setUp() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.delete(KEY);
            fixture.createGroup(KEY, "sanity", "0");
            var first = fixture.add(KEY, Map.of("order", "1"));
            var second = fixture.add(KEY, Map.of("order", "2"));

            // a worker read both entries and crashed 30 minutes ago
            fixture.deliver(KEY, "sanity", "crashed_worker", 10);
            fixture.makeIdle(KEY, "sanity", Duration.ofMinutes(30), first, second);
        }
    }

    @AfterEach
    void tearDown() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.delete(KEY);
        }
    }

    @Test
    @ExecuteFlow("sanity-checks/stream-recovery.yaml")
    void recoverProcessAcknowledge(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(6));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        // verified on a separate connection: the recovery finished the work
        try (var fixture = new StreamFixture(REDIS_URI)) {
            assertThat(fixture.pendingIds(KEY, "sanity"), is(empty()));
            assertThat(fixture.streamLength(KEY), is(2L));
        }
    }
}
