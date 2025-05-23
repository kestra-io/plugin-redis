package io.kestra.plugin.redis.string;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testDeleteList() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(Arrays.asList("keyDelete1", "keyDelete2")))
            .build();

        Delete.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @Test
    void testDeleteListFailed() {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(Arrays.asList("keyDeleted", "keyDeleted2")))
            .failedOnMissing(Property.ofValue(true))
            .build();

        Exception e = Assertions.assertThrows(NullPointerException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("Missing keys, only 1 key deleted"));
    }

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        createSetTask("keyDeleteOne", "value1").run(runContext);
        createSetTask("keyDelete1", "value2").run(runContext);
        createSetTask("keyDelete2", "value3").run(runContext);
        createSetTask("keyDeleted", "value4").run(runContext);
    }

    static Set createSetTask(String key, String value) {
        return Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .value(Property.ofValue(value))
            .build();
    }
}
