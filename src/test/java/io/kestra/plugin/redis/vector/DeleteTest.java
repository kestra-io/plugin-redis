package io.kestra.plugin.redis.vector;

import java.util.Arrays;
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
    private static final String VECTOR_SET = "deleteTestVectorSet";

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .element(Property.ofValue("elemDelete1"))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .build()
            .run(runContext);

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .element(Property.ofValue("elemDelete2"))
            .vector(Property.ofValue(Arrays.asList(0.0, 1.0, 0.0)))
            .build()
            .run(runContext);
    }

    @Test
    void testDeleteElements() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .elements(Property.ofValue(Arrays.asList("elemDelete1", "elemDelete2")))
            .build();

        Delete.Output output = task.run(runContext);

        assertThat(output.getCount(), is(2));
    }

    @Test
    void testDeleteDuplicateElementDoesNotFailOnMissing() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "deleteTestDuplicateVectorSet";

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .element(Property.ofValue("elemDeleteDuplicate"))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .build()
            .run(runContext);

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .elements(Property.ofValue(Arrays.asList("elemDeleteDuplicate", "elemDeleteDuplicate")))
            .failedOnMissing(Property.ofValue(true))
            .build();

        Delete.Output output = task.run(runContext);

        assertThat(output.getCount(), is(1));
    }

    @Test
    void testDeleteMissingElementFails() {
        RunContext runContext = runContextFactory.of(Map.of());

        Delete task = Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .elements(Property.ofValue(Arrays.asList("elemDeleteMissing1", "elemDeleteMissing2")))
            .failedOnMissing(Property.ofValue(true))
            .build();

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(
            e.getMessage(), is(
                "Only 0 of 2 element(s) removed — verify the element ids exist in the vector set "
                    + "before calling Delete, or set `failedOnMissing` to false to ignore missing ids."
            )
        );
    }
}
