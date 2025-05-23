package io.kestra.plugin.redis.json;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .value(Property.ofValue(Map.of("key1", "value1", "key2", 2)))
            .build()
            .run(runContext);
    }

    @Test
    void testGetWholeJson() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get.Output output = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .build()
            .run(runContext);

        assertThat(output.getData(), instanceOf(Map.class));
        Map<String, Object> data = (Map<String, Object>) output.getData();
        assertThat(data.get("key1"), is("value1"));
        assertThat(data.get("key2"), is(2));
    }

    @Test
    void testGetJsonPath() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get.Output output = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .path(Property.ofValue("$.key1"))
            .build()
            .run(runContext);

        assertThat(output.getData(), is("value1"));
    }

    @Test
    void testMissingKey() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get.Output output = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("missingKey"))
            .build()
            .run(runContext);

        assertThat(output.getData(), is(nullValue()));
    }

    @Test
    void testMissingKeyWithFail() {
        RunContext runContext = runContextFactory.of(Map.of());

        Get task = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("missingKey"))
            .failedOnMissing(Property.ofValue(true))
            .build();

        NullPointerException e = Assertions.assertThrows(NullPointerException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("Missing keys 'missingKey'"));
    }
}
