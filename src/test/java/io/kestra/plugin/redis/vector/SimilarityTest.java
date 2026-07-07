package io.kestra.plugin.redis.vector;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimilarityTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";
    private static final String VECTOR_SET = "similarityTestVectorSet";

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        addVector("elem1", Arrays.asList(1.0, 0.0, 0.0)).run(runContext);
        addVector("elem2", Arrays.asList(0.9, 0.1, 0.0)).run(runContext);
        addVector("elem3", Arrays.asList(0.0, 1.0, 0.0)).run(runContext);
    }

    static Add addVector(String element, java.util.List<Double> vector) {
        return Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .element(Property.ofValue(element))
            .vector(Property.ofValue(vector))
            .build();
    }

    @Test
    void testSimilarityByVector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Similarity.Output output = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(2))
            .build()
            .run(runContext);

        assertThat(output.getMatches(), hasSize(2));
        assertThat(output.getMatches().getFirst(), is("elem1"));
        assertThat(output.getScores(), aMapWithSize(2));
        assertThat(output.getScores().get("elem1"), is(1.0));
    }

    @Test
    void testSimilarityByElement() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Similarity.Output output = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .element(Property.ofValue("elem1"))
            .count(Property.ofValue(3))
            .build()
            .run(runContext);

        assertThat(output.getMatches(), hasItem("elem1"));
        assertThat(output.getScores().get("elem1"), is(1.0));
    }

    @Test
    void testSimilarityWithAttributesFilter() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "similarityTestFilterVectorSet-" + UUID.randomUUID();

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .element(Property.ofValue("elemElectronics"))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .attributes(Property.ofValue(Map.of("category", "electronics")))
            .build()
            .run(runContext);

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .element(Property.ofValue("elemBooks"))
            .vector(Property.ofValue(Arrays.asList(0.9, 0.1, 0.0)))
            .attributes(Property.ofValue(Map.of("category", "books")))
            .build()
            .run(runContext);

        Similarity.Output output = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(5))
            .filter(Property.ofValue(".category == \"electronics\""))
            .build()
            .run(runContext);

        assertThat(output.getMatches(), contains("elemElectronics"));
    }

    @Test
    void testSimilarityWithFilterEfficiencyAndEpsilon() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Similarity.Output output = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(5))
            .filterEfficiency(Property.ofValue(50))
            .epsilon(Property.ofValue(0.5))
            .build()
            .run(runContext);

        assertThat(output.getMatches(), is(not(empty())));
    }

    @Test
    void testBothVectorAndElementSetFails() {
        RunContext runContext = runContextFactory.of(Map.of());

        Similarity task = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .element(Property.ofValue("elem1"))
            .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("Exactly one of `vector` or `element` must be set to run a similarity search"));
    }

    @Test
    void testNeitherVectorNorElementSetFails() {
        RunContext runContext = runContextFactory.of(Map.of());

        Similarity task = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("Exactly one of `vector` or `element` must be set to run a similarity search"));
    }
}
