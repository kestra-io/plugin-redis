package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.redis.services.SerdeType;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testGet() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Get task = Get.builder()
            .uri(REDIS_URI)
            .key("key")
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getData(), is("value"));
    }

    @Test
    void testGetJson() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Get task = Get.builder()
            .uri(REDIS_URI)
            .key("keyJson")
            .serdeType(SerdeType.JSON)
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getData(),
            is(JacksonMapper.ofJson(false).readValue("{\"data\":5}", Object.class)));
    }

    @BeforeAll
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        createSetTask("key", "value").run(runContext);
        createSetTask("keyJson", "{\"data\":5}").run(runContext);
    }

    static Set createSetTask(String key, String value) {
        return Set.builder()
            .uri(REDIS_URI)
            .key(key)
            .value(value)
            .build();
    }
}
