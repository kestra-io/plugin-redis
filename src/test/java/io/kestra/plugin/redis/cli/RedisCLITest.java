package io.kestra.plugin.redis.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RedisCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    private static String host() {
        return "172.17.0.1";
    }

    private static Integer port() {
        return 6379;
    }

    @Test
    void testSetAndGet() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "SET testkey_cli 'Hello from RedisCLI'",
                "GET testkey_cli"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars().get("command_2"), is("Hello from RedisCLI"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testListOperations() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
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

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_5"));
        assertThat(output.getVars().get("command_5").toString(), containsString("item"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testJsonOutput() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .jsonOutput(Property.ofValue(true))
            .commands(Property.ofValue(List.of(
                "SET jsonkey_cli 'test_value'",
                "GET jsonkey_cli"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_2"));
        assertThat(output.getVars().get("command_2").toString(), containsString("test_value"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testDatabaseSelection() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .database(Property.ofValue(1))
            .commands(Property.ofValue(List.of(
                "SET db1_key 'value in db 1'",
                "GET db1_key"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_2"));
        assertThat(output.getVars().get("command_2").toString(), containsString("value in db 1"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testInfoCommand() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "INFO server"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_1"));
        assertThat(output.getVars().get("command_1").toString(), containsString("redis_version"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testMultipleCommands() throws Exception {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "PING",
                "ECHO 'Hello World'",
                "DBSIZE"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars(), hasKey("command_2"));
        assertThat(output.getVars().get("command_2"), is("Hello World"));
        assertThat(output.getStdOutLineCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testCommandFailureUnknownCommandThrowsRunnableTaskException() {
        RedisCLI task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue(host()))
            .port(Property.ofValue(port()))
            .password(Property.ofValue("redis"))
            .commands(Property.ofValue(List.of(
                "PING",
                "NO_SUCH_COMMAND"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        RunnableTaskException ex = assertThrows(
            RunnableTaskException.class,
            () -> task.run(runContext)
        );

        assertThat(ex.getMessage(), containsString("Command failed with exit code 1"));
        assertThat(ex.getCause(), notNullValue());
        assertThat(ex.getCause().getMessage(), containsString("Command failed with exit code 1"));
    }
}
