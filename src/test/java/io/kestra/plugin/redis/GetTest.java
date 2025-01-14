package io.kestra.plugin.redis;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.plugin.redis.string.Get;
import io.kestra.plugin.redis.string.Set;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
    void testMissingGet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get task = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("missing"))
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getData(), is(nullValue()));
    }

    @Test
    void testMissingGetFailed() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Get task = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("missing"))
            .failedOnMissing(Property.of(true))
            .build();

        NullPointerException e = Assertions.assertThrows(NullPointerException.class, () -> task.run(runContext));

        assertThat(e.getMessage(), is("Missing keys 'missing'"));
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

        assertThat(((Map<String, Object>)runOutput.getData()).get("key"), is("value"));
        assertThat(((Map<String, Object>)runOutput.getData()).get("int"), is(5));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSetGetJson() throws Exception {
        String random = IdUtils.create();

        RunContext runContext = runContextFactory.of(Map.of());

        Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("fromMap"))
            .serdeType(Property.of(SerdeType.JSON))
            .value(Property.of(Map.of("key", random)))
            .build()
            .run(runContext);

        Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("fromString"))
            .serdeType(Property.of(SerdeType.JSON))
            .value(Property.of(JacksonMapper.ofJson().writeValueAsString(Map.of("key", random))))
            .build()
            .run(runContext);

        Get.Output runOutput = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("fromMap"))
            .serdeType(Property.of(SerdeType.JSON))
            .build()
            .run(runContext);
        assertThat(((Map<String, Object>)runOutput.getData()).get("key"), is(random));

        runOutput = Get.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("fromString"))
            .serdeType(Property.of(SerdeType.JSON))
            .build()
            .run(runContext);
        assertThat(((Map<String, Object>)runOutput.getData()).get("key"), is(random));
    }

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        createSetTask("key", "value").run(runContext);
        createSetTask("keyJson", "{\"int\":5, \"key\": \"value\"}").run(runContext);
    }

    static Set createSetTask(String key, String value) {
        return Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of(key))
            .value(Property.of(value))
            .build();
    }
}
