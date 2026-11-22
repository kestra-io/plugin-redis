package io.kestra.plugin.redis.cli;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@KestraTest
class RedisCLIDurableTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void preservesExistingYamlPropertiesAndDefaults() throws Exception {
        RedisCLI task = JacksonMapper.ofYaml().readValue("""
            id: redis_cli
            type: io.kestra.plugin.redis.cli.RedisCLI
            host: localhost
            docker:
              image: redis:8-alpine
            env:
              REDISCLI_AUTH: from-env
            commands:
              - PING
            """, RedisCLI.class);
        RunContext context = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertEquals("redis:8-alpine", context.render(task.getDockerOptions()).as(DockerOptions.class).orElseThrow().getImage());
        assertEquals(Map.of("REDISCLI_AUTH", "from-env"), context.render(task.getEnv()).asMap(String.class, String.class));
        assertEquals("redis:7-alpine", context.render(task.getContainerImage()).as(String.class).orElseThrow());
        assertEquals(List.of(), assertInstanceOf(Docker.class, task.getTaskRunner()).getEntryPoint());
    }

    @Test
    void restoresCheckpointOnRetry() throws Exception {
        assumeTrue(
            Arrays.stream(AbstractExecScript.class.getMethods()).anyMatch(method -> method.getName().equals("getDurable")),
            "Checkpoints require Kestra 2.1 or later"
        );

        RedisCLI task = JacksonMapper.ofYaml().readValue("""
            id: redis_cli
            type: io.kestra.plugin.redis.cli.RedisCLI
            host: localhost
            taskRunner:
              type: io.kestra.plugin.core.runner.Process
            durable: true
            beforeCommands:
              - |
                if [ -z "${CHECKPOINTS_REDIS_CURSOR:-}" ]; then
                  printf '%s\\n' '::{"checkpoints":{"redis-cursor":"saved"}}::'
                  exit 1
                fi
              - |
                printf '%s\\n' '#!/bin/sh' 'printf "%s" "$CHECKPOINTS_REDIS_CURSOR"' > redis-cli
                chmod +x redis-cli
                export PATH="$PWD:$PATH"
            commands:
              - PING
            """, RedisCLI.class);

        RunContext context = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        assertThrows(RunnableTaskException.class, () -> task.run(context));
        // Both attempts use the same task-run checkpoint store.
        assertEquals("saved", task.run(context).getVars().get("command_1"));
    }
}
