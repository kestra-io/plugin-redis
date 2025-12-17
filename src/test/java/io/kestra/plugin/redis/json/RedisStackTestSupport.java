package io.kestra.plugin.redis.json;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class RedisStackTestSupport {
    private static final DockerImageName REDIS_STACK_IMAGE = DockerImageName.parse("redis/redis-stack-server:latest");
    private static GenericContainer<?> redisStack;

    public static synchronized String redisUri() {
        String defaultUri = "redis://:redis@localhost:6379/0";
        try {
            if (redisStack == null) {
                redisStack = new GenericContainer<>(REDIS_STACK_IMAGE)
                    .withEnv("REDIS_ARGS", "--requirepass redis")
                    .withExposedPorts(6379);
            }

            if (!redisStack.isRunning()) {
                redisStack.start();
            }

            String host = redisStack.getHost();
            Integer port = redisStack.getMappedPort(6379);
            return "redis://:redis@" + host + ":" + port + "/0";
        } catch (Throwable t) {
            // Fallback to CI/local compose Redis Stack on localhost
            return defaultUri;
        }
    }
}
