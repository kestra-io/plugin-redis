package io.kestra.plugin.redis.stream;

import java.util.LinkedHashMap;
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

// Runs sanity-checks/stream.yaml; the stream is seeded here because this slice of the sub-group only consumes.
@KestraTest(startRunner = true)
class StreamRunnerTest {
    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String KEY = "stream_group_runner_key";

    @BeforeEach
    void setUp() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            // a previous run of this flow leaves the group and its entry behind
            fixture.delete(KEY);

            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("order", "1");
            fields.put("payload", "{\"amount\": 10.50}");

            fixture.add(KEY, fields);
        }
    }

    @AfterEach
    void tearDown() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.delete(KEY);
        }
    }

    @Test
    @ExecuteFlow("sanity-checks/stream.yaml")
    void readProcessAcknowledge(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(8));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
