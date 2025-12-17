package io.kestra.plugin.redis.json;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class RedisStackTestSupport {
    private static final DockerImageName REDIS_STACK_IMAGE = DockerImageName.parse("redis/redis-stack-server:latest");
    private static final int REDIS_STACK_HOST_PORT = 6380;
    private static GenericContainer<?> redisStack;
    private static String cachedUri;

    public static synchronized String redisUriOrNull() {
        if (cachedUri != null) {
            return cachedUri;
        }

        // Use a non-default port to avoid colliding with developer machines already running Redis on 6379.
        String localhostUri = "redis://:redis@localhost:" + REDIS_STACK_HOST_PORT + "/0";

        // Prefer Testcontainers when Docker is available
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                if (redisStack == null) {
                    redisStack = new FixedHostPortGenericContainer<>(REDIS_STACK_IMAGE.asCanonicalNameString())
                        .withEnv("REDIS_ARGS", "--requirepass redis")
                        .withFixedExposedPort(REDIS_STACK_HOST_PORT, 6379);
                }

                if (!redisStack.isRunning()) {
                    redisStack.start();
                }

                String host = redisStack.getHost();
                cachedUri = "redis://:redis@" + host + ":" + REDIS_STACK_HOST_PORT + "/0";
                return cachedUri;
            }
        } catch (Throwable ignored) {
            // ignore and try localhost fallback probe below
        }

        // If Docker isn't available, allow running against a locally-provided Redis Stack (e.g., docker-compose)
        if (supportsRedisJson(localhostUri)) {
            cachedUri = localhostUri;
            return cachedUri;
        }

        return null;
    }

    public static synchronized String redisUri() {
        String uri = redisUriOrNull();
        if (uri == null) {
            throw new IllegalStateException("Redis Stack (RedisJSON) is not available");
        }
        return uri;
    }

    private static boolean supportsRedisJson(String uri) {
        RedisClient client = null;
        try {
            client = RedisClient.create(uri);
            try (StatefulRedisConnection<String, String> connection = client.connect()) {
                ProtocolKeyword jsonSet = () -> "JSON.SET".getBytes(StandardCharsets.UTF_8);
                String key = "probe_json_" + UUID.randomUUID();
                CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key)
                    .add("$")
                    .add("{\"ok\":true}");

                connection.sync().dispatch(jsonSet, new StatusOutput<>(StringCodec.UTF8), args);
                return true;
            }
        } catch (Throwable ignored) {
            return false;
        } finally {
            if (client != null) {
                try {
                    client.shutdown();
                } catch (Throwable ignored) {
                    // ignore
                }
            }
        }
    }
}
