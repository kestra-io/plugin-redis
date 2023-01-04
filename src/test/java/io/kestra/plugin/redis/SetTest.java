package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
class SetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testSetGet() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Set task = Set.builder()
                .uri(REDIS_URI)
                .key("key")
                .value("value")
                .get(true)
                .build();

        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is("value"));
    }

    @Test
    void testSet() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Set task = Set.builder()
                .uri(REDIS_URI)
                .key("key2")
                .value("{\"value\":\"1\"}")
                .build();

        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is(nullValue()));
    }

}
