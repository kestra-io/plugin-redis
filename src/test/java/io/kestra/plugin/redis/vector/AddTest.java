package io.kestra.plugin.redis.vector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import io.lettuce.core.vector.QuantizationType;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AddTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    // VADD only replies `true` when the element didn't exist yet, so each test needs a fresh
    // key/element pair, otherwise a re-run against the same persistent Redis container would see
    // the element already present and get a `false` (updated) reply instead.
    @Test
    void testAddVector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Add.Output output = Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("addTestVectorSet-" + UUID.randomUUID()))
            .element(Property.ofValue("elem1"))
            .vector(Property.ofValue(Arrays.asList(1.0, 2.0, 3.0)))
            .build()
            .run(runContext);

        assertThat(output.getAdded(), is(true));
    }

    @Test
    void testAddVectorWithOptions() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Add.Output output = Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("addTestVectorSetOptions-" + UUID.randomUUID()))
            .element(Property.ofValue("elem1"))
            .vector(Property.ofValue(Arrays.asList(1.0, 2.0, 3.0, 4.0)))
            .quantization(Property.ofValue(QuantizationType.Q8))
            .explorationFactor(Property.ofValue(100))
            .maxNodes(Property.ofValue(16))
            .attributes(Property.ofValue(Map.of("category", "electronics")))
            .build()
            .run(runContext);

        assertThat(output.getAdded(), is(true));
    }

    @Test
    void testAddEmptyVectorFails() {
        RunContext runContext = runContextFactory.of(Map.of());

        Add task = Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("addTestEmptyVectorSet-" + UUID.randomUUID()))
            .element(Property.ofValue("elem1"))
            .vector(Property.ofValue(List.of()))
            .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("`vector` must contain at least one dimension"));
    }
}
