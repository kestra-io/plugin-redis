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
        if (redisStack == null) {
            redisStack = new GenericContainer<>(REDIS_STACK_IMAGE)
                .withEnv("REDIS_ARGS", "--requirepass redis")
                .withExposedPorts(6379)
                .withCreateContainerCmdModifier(cmd -> {
                    ExposedPort container = ExposedPort.tcp(6379);
                    Ports.Binding host = Ports.Binding.bindPort(6380);
                    cmd.getHostConfig().withPortBindings(new PortBinding(host, container));
                });
        }

        if (!redisStack.isRunning()) {
            redisStack.start();
        }

        String host = redisStack.getHost();
        Integer port = redisStack.getMappedPort(6379);
        return "redis://:redis@" + host + ":" + port + "/0";
    }
}
