package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
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

}
