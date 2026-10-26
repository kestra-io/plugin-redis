package io.kestra.plugin.redis.cli;

import java.util.List;
import java.util.Map;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.lettuce.core.RedisClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RedisCLIErrorReplyTest {
    @Inject
    private RunContextFactory runContextFactory;

    private RedisCLI.RedisCLIBuilder<?, ?> task() {
        return RedisCLI.builder()
            .id(IdUtils.create()).type(RedisCLI.class.getName())
            .host(Property.ofValue("172.17.0.1")).port(Property.ofValue(6379))
            .password(Property.ofValue("redis"));
    }

    @Test
    void shouldFailForEveryRedisErrorReplyCodeInRawAndJsonModes() {
        for (var json : List.of(false, true)) {
            for (var code : List.of("WRONGTYPE", "NOPERM", "NOREPLICAS", "MISCONF", "READONLY")) {
                var task = task().jsonOutput(Property.ofValue(json))
                    .commands(Property.ofValue(List.of("EVAL_RO 'return redis.error_reply(\"" + code + " test refusal\")' 0")))
                    .build();
                var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

                var error = assertThrows(RunnableTaskException.class, () -> task.run(runContext));

                assertThat(error.getMessage(), containsString("exit code 1"));
            }
        }
    }

    @Test
    void shouldStopFollowingCommandsAfterWrongTypeError() {
        var key = "wrongtype_" + IdUtils.create();
        var after = key + "_after";
        var task = task().commands(Property.ofValue(List.of(
            "SET " + key + " scalar", "LPUSH " + key + " value", "SET " + after + " reached"
        ))).build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertThrows(RunnableTaskException.class, () -> task.run(runContext));

        var client = RedisClient.create("redis://:redis@localhost:6379/0");
        try (var connection = client.connect()) {
            assertThat(connection.sync().get(after), nullValue());
            assertThat(connection.sync().get(key), is("scalar"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    void shouldAcceptOrdinaryStringsThatLookLikeErrorMessages() throws Exception {
        for (var value : List.of("ERR ordinary text", "(error) ordinary text", "WRONGTYPE ordinary text")) {
            var task = task().commands(Property.ofValue(List.of("ECHO " + RedisCLI.shellQuote(value)))).build();
            var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

            var output = task.run(runContext);

            assertThat(output.getExitCode(), is(0));
            assertThat(output.getVars().get("command_1"), is(value));
        }
    }
}
