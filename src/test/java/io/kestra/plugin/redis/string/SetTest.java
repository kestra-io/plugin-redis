package io.kestra.plugin.redis.string;

import com.fasterxml.jackson.core.JsonParseException;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class SetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testSetGet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set taskInit = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("keySetGet"))
            .value(Property.ofValue("value"))
            .build();

        Set task = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("keySetGet"))
            .value(Property.ofValue("value"))
            .get(Property.ofValue(true))
            .build();

        taskInit.run(runContext);
        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is("value"));
    }

    @Test
    void testSet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set task = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("key2"))
            .value(Property.ofValue("{\"value\":\"1\"}"))
            .build();

        Set.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOldValue(), is(nullValue()));
    }

    @Test
    void testSetInvalidJson() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set task = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("key2"))
            .value(Property.ofValue("value"))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();

        JsonParseException exception = assertThrows(JsonParseException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("Unrecognized token"));
    }

    @Test
    void testSetKeepTtlWithExpirationDuration() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set task = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("invalidKey1"))
            .value(Property.ofValue("value"))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .options(Set.Options.builder()
                .keepTtl(Property.ofValue(true))
                .expirationDuration(Property.ofValue(java.time.Duration.ofSeconds(10)))
                .build()
            )
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("you can't use 'keepTtl' with 'expirationDuration' or 'expirationDate'"));
    }
}
