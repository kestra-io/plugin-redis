package io.kestra.plugin.templates;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import io.kestra.plugin.templates.client.RedisApiService;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
class RedisApiTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_PROTOCOL = "redis";
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_DB = 0;
    private static final String REDIS_PORT = "6379";
    private static final String REDIS_PASSWORD = "redis";

    private static RedisApiService redisApiService;
    private static StatefulRedisConnection<String, String> connection;

    @BeforeAll
    static void setUp() {
        redisApiService = new RedisApiService(REDIS_PROTOCOL, REDIS_PASSWORD, REDIS_HOST, REDIS_PORT, REDIS_DB, "STRING");
        connection = redisApiService.connect();
        assert connection != null;
    }

    @AfterAll
    static void tearDown() {
        redisApiService.disconnect();
    }

    @Test
    void testGetString() throws Exception {
        // store something in redis
        redisApiService.set("key", "secret");

        RunContext runContext = runContextFactory.of(ImmutableMap.of("key", "secret"));

        RedisTask task = RedisTask.builder()
                .redisApiService(redisApiService)
                .operation("GET")
                .key("key")
                .build();

        RedisTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getChild().getValue(), is("secret"));
    }

    @Test
    void testGetJson() throws Exception {
        // store something in redis
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("somekey", "somevalue");
        redisApiService.set("key", jsonObject.getAsString());

        RunContext runContext = runContextFactory.of(ImmutableMap.of("key", "secret"));

        RedisTask task = RedisTask.builder()
                .redisApiService(redisApiService)
                .operation("GET")
                .key("key")
                .build();

        RedisTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getChild().getValue(), is("secret"));
    }
}
