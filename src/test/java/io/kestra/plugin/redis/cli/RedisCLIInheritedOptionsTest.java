package io.kestra.plugin.redis.cli;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the options RedisCLI inherits from AbstractExecScript.
 */
@KestraTest
class RedisCLIInheritedOptionsTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static RedisCLI.RedisCLIBuilder<?, ?> task() {
        return RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue("172.17.0.1"))
            .port(Property.ofValue(6379))
            .password(Property.ofValue("redis"));
    }

    @Test
    void existingDockerYamlStillBindsToDockerOptions() throws Exception {
        var task = JacksonMapper.ofYaml().readValue("""
            id: redis_cli
            type: io.kestra.plugin.redis.cli.RedisCLI
            host: localhost
            commands:
              - PING
            docker:
              image: redis:7.2-alpine
            env:
              MY_VAR: value
            """, RedisCLI.class);
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var rDockerOptions = runContext.render(task.getDockerOptions()).as(DockerOptions.class).orElseThrow();
        assertThat(rDockerOptions.getImage(), is("redis:7.2-alpine"));
        assertThat(task.getDocker(), is(nullValue()));
        assertThat(runContext.render(task.getEnv()).asMap(String.class, String.class), is(Map.of("MY_VAR", "value")));
    }

    @Test
    void beforeCommandsRunInTheSameContainerAsTheRedisCommands() throws Exception {
        var task = task()
            .beforeCommands(Property.ofValue(List.of("echo prepared > before.txt")))
            .commands(Property.ofValue(List.of("PING")))
            .outputFiles(Property.ofValue(List.of("before.txt")))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var output = task.run(runContext);

        assertThat(output.getVars().get("command_1"), is("PONG"));
        assertThat(output.getOutputFiles().containsKey("before.txt"), is(true));
    }

    @Test
    void failedCommandStopsTheRunEvenWithFailFastOff() {
        var task = task()
            .failFast(Property.ofValue(false))
            .commands(Property.ofValue(List.of("NO_SUCH_COMMAND", "PING")))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var error = assertThrows(RunnableTaskException.class, () -> task.run(runContext));

        assertThat(error.getMessage(), containsString("exit code 1"));
    }

    @Test
    void toStringDoesNotExposeEnvSecrets() throws Exception {
        var task = task()
            .commands(Property.ofValue(List.of("PING")))
            .env(Property.ofValue(Map.of("REDISCLI_AUTH", "super-secret-value")))
            .build();
        // render caches the value on the property, as run() does
        TestsUtils.mockRunContext(runContextFactory, task, Map.of()).render(task.getEnv()).asMap(String.class, String.class);

        assertThat(task.toString(), not(containsString("super-secret-value")));
    }
}
