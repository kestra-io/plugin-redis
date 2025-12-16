package io.kestra.plugin.redis.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RedisCLITest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7-alpine");
    private static final GenericContainer<?> REDIS = new GenericContainer<>(REDIS_IMAGE)
        .withExposedPorts(6379)
        .withCommand("redis-server", "--requirepass", "redis");

    private static String host() { return REDIS.getHost(); }
    private static Integer port() { return REDIS.getMappedPort(6379); }

    @BeforeAll
    static void ensureDockerAndStartRedis() {
        boolean socketPresent = new java.io.File("/var/run/docker.sock").exists();
        boolean dockerHostSet = System.getenv("DOCKER_HOST") != null;

        Assumptions.assumeTrue(socketPresent || dockerHostSet,
            "Docker is required for RedisCLITest");

        if (!REDIS.isRunning()) {
            REDIS.start();
        }
    }

    @Test
    void testSetAndGet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "SET testkey_cli 'Hello from RedisCLI'",
                "GET testkey_cli"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    @Test
    void testListOperations() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "DEL testlist_cli",
                "LPUSH testlist_cli item1",
                "LPUSH testlist_cli item2",
                "LPUSH testlist_cli item3",
                "LRANGE testlist_cli 0 -1"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    @Test
    void testJsonOutput() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .jsonOutput(Property.ofValue(true))
            .commands(Property.ofValue(List.of(
                "SET jsonkey_cli 'test_value'",
                "GET jsonkey_cli"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    @Test
    void testDatabaseSelection() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .database(Property.ofValue(1))
            .commands(Property.ofValue(List.of(
                "SET db1_key 'value in db 1'",
                "GET db1_key"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    @Test
    void testInfoCommand() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "INFO server"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getStdOutLineCount(), greaterThan(0));
    }

    @Test
    void testMultipleCommands() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        RedisCLI task = RedisCLI.builder()
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "PING",
                "ECHO 'Hello World'",
                "DBSIZE"
            )))
            .build();

        RedisCLI.Output output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }
}
