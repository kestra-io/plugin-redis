package io.kestra.plugin.redis.string;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IncrementTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void noValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(1L));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2L));
    }

    @Test
    void longValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .amount(Property.ofValue(2L))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2L));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(4L));
    }


    @Test
    void doubleValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .amount(Property.ofValue(2.5D))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2.5D));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(5D));
    }
}
