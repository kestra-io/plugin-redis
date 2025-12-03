package io.kestra.plugin.redis.string;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IncrementTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void noValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(1L));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2L));
    }

    @Test
    void longValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .amount(Property.ofValue(2L))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2L));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(4L));
    }


    @Test
    void doubleValue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String s = IdUtils.create();

        Increment task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(s))
            .amount(Property.ofValue(2.5D))
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(2.5D));

        runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(5D));
    }

    @Test
    void withExpirationDuration() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var key = IdUtils.create();

        var task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .amount(Property.ofValue(1L))
            .options(Increment.Options.builder()
                .expirationDuration(Property.ofValue(Duration.ofSeconds(10)))
                .build())
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(1L));


        var ttlTask = Ttl.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .build();

        Ttl.Output ttlOutput = ttlTask.run(runContext);
        assertThat(ttlOutput.getTtl(), is(lessThanOrEqualTo(10L)));

        // we wait for expiration
        Thread.sleep(11000);

        var getTask = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .build();

        Get.Output getOutput = getTask.run(runContext);
        assertThat(getOutput.getData(), is(nullValue()));
    }


    @Test
    void withExpirationDate() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var key = IdUtils.create();

        ZonedDateTime expirationDate = ZonedDateTime.now().plusSeconds(10);

        var task = Increment.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .amount(Property.ofValue(5L))
            .options(Increment.Options.builder()
                .expirationDate(Property.ofValue(expirationDate))
                .build())
            .build();

        Increment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getValue(), is(5L));

        var ttlTask = Ttl.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .build();

        Ttl.Output ttlOutput = ttlTask.run(runContext);
        assertThat(ttlOutput.getTtl(), is(lessThanOrEqualTo(10L)));

        Thread.sleep(11100);

        var getTask = Get.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .build();

        Get.Output getOutput = getTask.run(runContext);
        assertThat(getOutput.getData(), is(nullValue()));
    }
}
