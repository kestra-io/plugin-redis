package io.kestra.plugin.redis;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.plugin.redis.string.Get;
import io.kestra.plugin.redis.string.Set;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testGet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get task = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("key"))
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getData(), is("value"));
    }

    @Test
    void testGetJson() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get task = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("keyJson"))
            .serdeType(Property.of(SerdeType.JSON))
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getData(), is(JacksonMapper.ofJson(false).readValue("{\"data\":5}", Object.class)));
    }

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        createSetTask("key", "value").run(runContext);
        createSetTask("keyJson", "{\"data\":5}").run(runContext);
    }

    static Set createSetTask(String key, String value) {
        return Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of(key))
            .value(Property.of(value))
            .build();
    }
}
