package io.kestra.plugin.redis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.string.Set;
import io.kestra.plugin.redis.string.Ttl;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TtlTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void noValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Set setTask = Set.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of(s))
            .value(Property.of("test"))
            .options(Set.Options
                .builder()
                .expirationDuration(Property.of(Duration.ofDays(1)))
                .build()
            )
            .build();
        setTask.run(runContext);

        Ttl ttlTask = Ttl.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of(s))
            .build();

        Ttl.Output ttlOutput = ttlTask.run(runContext);

        assertThat(ttlOutput.getTtl(), greaterThan(86000L));
    }
}
