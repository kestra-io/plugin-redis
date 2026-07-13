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
import jakarta.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

        assertThat(output.getMatches(), contains("elem1", "elem2"));
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
        String key = "similarityTestFilterEfficiencyEpsilonVectorSet-" + UUID.randomUUID();

        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .element(Property.ofValue("elemNear"))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .attributes(Property.ofValue(Map.of("category", "electronics")))
            .build()
            .run(runContext);

        // orthogonal to the query vector: VSIM reports a similarity score of 0.5, i.e. a distance of 0.5
        Add.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .element(Property.ofValue("elemFar"))
            .vector(Property.ofValue(Arrays.asList(0.0, 1.0, 0.0)))
            .attributes(Property.ofValue(Map.of("category", "electronics")))
            .build()
            .run(runContext);

        // an epsilon tighter than elemFar's distance (0.5) excludes it, while the same query without
        // epsilon returns both: this fails if `epsilon` were silently dropped before reaching VSimArgs.
        Similarity.Output narrowed = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(5))
            .filter(Property.ofValue(".category == \"electronics\""))
            .filterEfficiency(Property.ofValue(50))
            .epsilon(Property.ofValue(0.1))
            .build()
            .run(runContext);

        assertThat(narrowed.getMatches(), contains("elemNear"));
        assertThat(narrowed.getScores().get("elemNear"), is(1.0));

        Similarity.Output unbounded = Similarity.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(5))
            .filter(Property.ofValue(".category == \"electronics\""))
            .build()
            .run(runContext);

        assertThat(unbounded.getMatches(), containsInAnyOrder("elemNear", "elemFar"));
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
    void testCountBoundsValidation() {
        RunContext runContext = runContextFactory.of(Map.of());

        // proves the @Min/@Max constraints on `count` are declared on the Property<T> type parameter
        // (not the field itself), which is the only position Kestra's PropertyValueExtractor unwraps for
        // validation; a field-level annotation would throw UnexpectedTypeException instead of validating.
        Similarity validTask = Similarity.builder()
            .id("similarity")
            .type(Similarity.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(1))
            .build();

        assertDoesNotThrow(() -> runContext.validate(validTask));

        Similarity belowMin = Similarity.builder()
            .id("similarity")
            .type(Similarity.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(0))
            .build();

        ConstraintViolationException belowMinViolation = assertThrows(ConstraintViolationException.class, () -> runContext.validate(belowMin));
        assertThat(belowMinViolation.getConstraintViolations(), is(not(empty())));

        Similarity aboveMax = Similarity.builder()
            .id("similarity")
            .type(Similarity.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(VECTOR_SET))
            .vector(Property.ofValue(Arrays.asList(1.0, 0.0, 0.0)))
            .count(Property.ofValue(10001))
            .build();

        ConstraintViolationException aboveMaxViolation = assertThrows(ConstraintViolationException.class, () -> runContext.validate(aboveMax));
        assertThat(aboveMaxViolation.getConstraintViolations(), is(not(empty())));
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
