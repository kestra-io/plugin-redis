package io.kestra.plugin.redis;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.redis.string.Set;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class SetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testSetGet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set taskInit = Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("keySetGet"))
            .value(Property.of("value"))
            .build();

        Set task = Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("keySetGet"))
            .value(Property.of("value"))
            .get(Property.of(true))
            .build();

        taskInit.run(runContext);
        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is("value"));
    }

    @Test
    void testSet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set task = Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("key2"))
            .value(Property.of("{\"value\":\"1\"}"))
            .build();

        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is(nullValue()));
    }
}
