package io.kestra.plugin.redis.json;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        createSetTask("keyDelete1", Map.of("field", "value1")).run(runContext);
        createSetTask("keyDelete2", Map.of("field", "value2")).run(runContext);
        createSetTask("keyDeleted", Map.of("field", "value3")).run(runContext);
    }

    @Test
    void testDeleteKeysAndPaths() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(
                Property.ofValue(
                    Map.of(
                        "keyDelete1", List.of("$"),
                        "keyDelete2", List.of("$.field")
                    )
                )
            )
            .build();

        Delete.Output output = task.run(runContext);

        assertThat(output.getCount(), is(2));
    }

    @Test
    void testDeleteWithMissingKeyAndFail() {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(
                Property.ofValue(
                    Map.of(
                        "keyDeleted", List.of("$.field"),
                        "nonExistingKey", List.of("$")
                    )
                )
            )
            .failedOnMissing(Property.ofValue(true))
            .build();

        Exception e = Assertions.assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is(
            "1 of 2 requested paths deleted nothing: nonExistingKey $. Paths that exist were still deleted. Set `failedOnMissing` to false to accept that."
        ));
    }

    @Test
    void testDeleteWithMissingPathOnExistingKeyAndFail() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        createSetTask("keyDeletePaths", Map.of("field", "value4", "keep", "value5")).run(runContext);

        var task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(
                Property.ofValue(
                    Map.of(
                        "keyDeletePaths", List.of("$.field", "$.missingField")
                    )
                )
            )
            .failedOnMissing(Property.ofValue(true))
            .build();

        var e = Assertions.assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is(
            "1 of 2 requested paths deleted nothing: keyDeletePaths $.missingField. Paths that exist were still deleted. Set `failedOnMissing` to false to accept that."
        ));

        var remaining = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("keyDeletePaths"))
            .build()
            .run(runContext);
        assertThat(remaining.getData(), is(Map.of("keep", "value5")));
    }

    static Set createSetTask(String key, Object value) {
        return Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .value(Property.ofValue(value))
            .build();
    }
}
