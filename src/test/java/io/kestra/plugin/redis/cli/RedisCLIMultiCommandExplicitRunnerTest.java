package io.kestra.plugin.redis.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.runner.docker.Docker;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

@KestraTest
class RedisCLIMultiCommandExplicitRunnerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runsEveryCommandWithAnExplicitTaskRunner() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue("172.17.0.1"))
            .port(Property.ofValue(6379))
            .password(Property.ofValue("redis"))
            .taskRunner(Docker.builder()
                .type(Docker.class.getName())
                .entryPoint(List.of())
                .build())
            .commands(Property.ofValue(List.of("PING", "PING")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_2"));
        assertThat(output.getVars().get("command_2"), is("PONG"));
    }
}
