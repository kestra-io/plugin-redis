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

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IncrementTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testIncrementJsonValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Set set = Set.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("incrementKey"))
            .value(Property.ofValue(Map.of("counter", 5)))
            .build();
        set.run(runContext);

        Increment increment = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("incrementKey"))
            .path(Property.ofValue("$.counter"))
            .amount(Property.ofValue(3))
            .build();
        Increment.Output output = increment.run(runContext);

        assertThat(output.getValue().longValue(), is(8L));
        assertThat(output.getKey(), is("incrementKey"));

        Get get = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("incrementKey"))
            .build();
        Get.Output getOutput = get.run(runContext);

        assertThat(output.getValue().longValue(), is(8L));
    }
}
