package io.kestra.plugin.redis.json;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testSetJson() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set.Output output = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .value(Property.ofValue(Map.of("key1", "value1", "value2", 2)))
            .build()
            .run(runContext);

        Get.Output get = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .build()
            .run(runContext);

        Map<String, Object> data = (Map<String, Object>) get.getData();
        assertThat(data.get("key1"), is("value1"));
        assertThat(data.get("value2"), is(2));
    }

    @Test
    void testSetWithPath() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .value(Property.ofValue(Map.of("key1", Map.of())))
            .build()
            .run(runContext);

        Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .path(Property.ofValue("$.key1"))
            .value(Property.ofValue("{\"key1\":\"value1\"}"))
            .build()
            .run(runContext);

        Get.Output get = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("jsonKey"))
            .path(Property.ofValue("$.key1"))
            .build()
            .run(runContext);

        Map<String, Object> expected = Map.of("key1", "value1");
        assertThat(get.getData(), is(expected));
    }

    @Test
    void testSetMustNotExist() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set.Output first = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mustNotExistJsonKey"))
            .value(Property.ofValue(Map.of("mustNotExistKey1", "value1")))
            .options(Set.Options.builder().mustNotExist(Property.ofValue(true)).build())
            .build()
            .run(runContext);

        assertThat(first.getOldValue(), nullValue());

        Set.Output second = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mustNotExistJsonKey"))
            .value(Property.ofValue(Map.of("key2", "value2")))
            .options(Set.Options.builder().mustNotExist(Property.ofValue(true)).build())
            .build()
            .run(runContext);

        assertThat(second.getOldValue(), nullValue());

        Get.Output get = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mustNotExistJsonKey"))
            .build()
            .run(runContext);

        System.out.println(get.getData());

        Map<String, Object> data = (Map<String, Object>) get.getData();
        assertThat(data.get("mustNotExistKey1"), is("value1"));
        assertThat(data.get("key2"), nullValue());

    }
}
