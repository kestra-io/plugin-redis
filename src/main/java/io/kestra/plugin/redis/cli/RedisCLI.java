package io.kestra.plugin.redis.cli;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run Redis CLI commands in a container",
    description = """
        Executes the provided redis-cli commands sequentially using the configured task runner (Docker by default with image redis:7-alpine), failing on the first non-zero or detected Redis error.
        Supports TLS, username/password auth via REDISCLI_AUTH, and optional JSON output (Redis 7+).
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Execute Redis CLI commands to set and get a key.",
            full = true,
            code = """
                id: redis_cli_example
                namespace: company.team

                tasks:
                  - id: redis_cli
                    type: io.kestra.plugin.redis.cli.RedisCLI
                    host: localhost
                    port: 6379
                    commands:
                      - SET mykey "Hello World"
                      - GET mykey
                """
        ),
        @Example(
            title = "Execute Redis CLI commands with authentication and JSON output.",
            full = true,
            code = """
                id: redis_cli_auth
                namespace: company.team

                tasks:
                  - id: redis_cli
                    type: io.kestra.plugin.redis.cli.RedisCLI
                    host: localhost
                    port: 6379
                    password: "{{ secret('REDIS_PASSWORD') }}"
                    commands:
                      - LPUSH mylist "item1"
                      - LPUSH mylist "item2"
                      - LRANGE mylist 0 -1
                    jsonOutput: true
                """
        ),
        @Example(
            title = "Execute Redis CLI commands with custom Docker image.",
            full = true,
            code = """
                id: redis_cli_custom_image
                namespace: company.team

                tasks:
                  - id: redis_cli
                    type: io.kestra.plugin.redis.cli.RedisCLI
                    host: redis.example.com
                    port: 6379
                    database: 2
                    username: myuser
                    password: "{{ secret('REDIS_PASSWORD') }}"
                    containerImage: redis:7-alpine
                    commands:
                      - INFO server
                      - DBSIZE
                """
        )
    },
    metrics = {
        @Metric(
            name = "executed.commands.count",
            type = Counter.TYPE,
            unit = "commands",
            description = "Number of Redis CLI commands executed."
        )
    }
)
public class RedisCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    private static final String DEFAULT_IMAGE = "redis:7-alpine";

    @Schema(title = "Redis host")
    @NotNull
    private Property<String> host;

    @Schema(
        title = "Redis port",
        description = "Defaults to 6379."
    )
    @Builder.Default
    private Property<Integer> port = Property.ofValue(6379);

    @Schema(
        title = "Database index",
        description = "Defaults to 0."
    )
    @Builder.Default
    private Property<Integer> database = Property.ofValue(0);

    @Schema(
        title = "Username for ACL authentication",
        description = "Required when the Redis server enforces ACLs."
    )
    private Property<String> username;

    @Schema(
        title = "Password for authentication",
        description = "Sent via REDISCLI_AUTH env variable."
    )
    private Property<String> password;

    @Schema(
        title = "Enable TLS",
        description = "Defaults to false."
    )
    @Builder.Default
    private Property<Boolean> tls = Property.ofValue(false);

    @Schema(
        title = "Commands to run",
        description = "List of redis-cli commands executed in order; task stops on first failure or error output."
    )
    @NotNull
    private Property<List<String>> commands;

    @Schema(
        title = "Enable JSON output",
        description = "Adds redis-cli --json; requires Redis 7+."
    )
    @Builder.Default
    private Property<Boolean> jsonOutput = Property.ofValue(false);

    @Schema(
        title = "Container image",
        description = "Defaults to redis:7-alpine when docker image is not already set."
    )
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Task runner",
        description = "Defaults to Docker; other runners may require additional properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.builder()
        .type(Docker.class.getName())
        .entryPoint(new ArrayList<>())
        .build();

    @Schema(
        title = "Docker options",
        description = "Applied when using the Docker task runner; its image overrides containerImage if set."
    )
    @PluginProperty
    @Builder.Default
    protected Property<DockerOptions> docker = Property.ofValue(DockerOptions.builder().build());

    @Schema(
        title = "Additional environment variables",
        description = "Merged with REDISCLI_AUTH when password is provided."
    )
    protected Property<Map<String, String>> env;

    private NamespaceFiles namespaceFiles;
    private Object inputFiles;
    private Property<List<String>> outputFiles;

    private static String escapeForJson(String s) {
        if (s == null) return "";
        return s
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n");
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        String rHost = runContext.render(host).as(String.class).orElseThrow();
        Integer rPort = runContext.render(port).as(Integer.class).orElse(6379);
        Integer rDatabase = runContext.render(database).as(Integer.class).orElse(0);
        String rUsername = runContext.render(username).as(String.class).orElse(null);
        String rPassword = runContext.render(password).as(String.class).orElse(null);
        Boolean rTls = runContext.render(tls).as(Boolean.class).orElse(false);
        Boolean rJsonOutput = runContext.render(jsonOutput).as(Boolean.class).orElse(false);
        List<String> rCommands = runContext.render(commands).asList(String.class);
        String rContainerImage = runContext.render(containerImage).as(String.class).orElse(DEFAULT_IMAGE);

        if (rCommands.isEmpty()) {
            throw new IllegalArgumentException("At least one command must be provided");
        }

        logger.info("Executing {} Redis CLI command(s) against {}:{}", rCommands.size(), rHost, rPort);

        StringBuilder baseCommand = new StringBuilder("redis-cli");
        baseCommand.append(" -h ").append(rHost);
        baseCommand.append(" -p ").append(rPort);
        baseCommand.append(" -n ").append(rDatabase);

        if (rUsername != null && !rUsername.isEmpty()) {
            baseCommand.append(" --user ").append(rUsername);
        }
        if (rTls) {
            baseCommand.append(" --tls");
        }
        if (rJsonOutput) {
            baseCommand.append(" --json");
        }

        List<String> wrappedShellCommands = extractWrappedShellCommands(rCommands, baseCommand);

        Map<String, String> envVars = new HashMap<>();
        var rEnvMap = runContext.render(env).asMap(String.class, String.class);
        if (rPassword != null && !rPassword.isEmpty()) {
            envVars.put("REDISCLI_AUTH", rPassword);
        }
        if (!rEnvMap.isEmpty()) {
            envVars.putAll(rEnvMap);
        }

        var rOutputFiles = runContext.render(outputFiles).asList(String.class);

        DockerOptions dockerOptions = runContext.render(docker).as(DockerOptions.class).orElse(DockerOptions.builder().build());
        var dockerBuilder = dockerOptions.toBuilder();
        if (dockerOptions.getImage() == null) {
            dockerBuilder.image(rContainerImage);
        }

        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(envVars)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(rOutputFiles.isEmpty() ? null : rOutputFiles)
            .withContainerImage(rContainerImage)
            .withTaskRunner(taskRunner)
            .withDockerOptions(dockerBuilder.build())
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")));

        ScriptOutput lastOutput = null;
        Map<String, Object> mergedVars = new HashMap<>();

        for (String wrappedShellCommand : wrappedShellCommands) {
            lastOutput = commandsWrapper
                .withCommands(Property.ofValue(List.of(wrappedShellCommand)))
                .run();

            if (lastOutput.getExitCode() != 0) {
                throw new IllegalStateException("Redis CLI command failed with exit code: " + lastOutput.getExitCode());
            }

            if (lastOutput.getVars() != null && !lastOutput.getVars().isEmpty()) {
                mergedVars.putAll(lastOutput.getVars());
            }
        }

        runContext.metric(Counter.of("executed.commands.count", rCommands.size()));

        return lastOutput == null
            ? ScriptOutput.builder().vars(Map.of()).exitCode(0).build()
            : ScriptOutput.builder()
            .vars(mergedVars)
            .exitCode(lastOutput.getExitCode())
            .outputFiles(lastOutput.getOutputFiles())
            .stdOutLineCount(lastOutput.getStdOutLineCount())
            .stdErrLineCount(lastOutput.getStdErrLineCount())
            .taskRunner(lastOutput.getTaskRunner())
            .build();
    }

    private List<String> extractWrappedShellCommands(List<String> rCommands, StringBuilder baseCommand) {
        List<String> wrappedShellCommands = new ArrayList<>();

        int idx = 1;
        for (String redisCommand : rCommands) {
            String key = "command_" + idx++;

            String cmd =
                // 1) run redis-cli, capture stdout+stderr
                "OUT=$(" + baseCommand + " " + redisCommand + " 2>&1); " +
                    "RC=$?; " +

                    // 2) some redis-cli errors still exit 0 -> force RC=1 if output looks like an error
                    "printf '%s' \"$OUT\" | grep -Eiq '^(\\(error\\)|ERR )' && RC=1; " +

                    // 3) escape and flatten output to ONE line (no real newlines) using awk (busybox-friendly)
                    "OUT_ESC=$(printf '%s' \"$OUT\" | " +
                    "awk 'BEGIN{ORS=\"\"} {" +
                    "gsub(/\\\\\\\\/,\"\\\\\\\\\\\\\\\\\"); " +   // \  -> \\
                    "gsub(/\\\"/,\"\\\\\\\\\\\"\"); " +          // \" -> \"
                    "sub(/\\r$/,\"\"); " +                       // strip CR
                    "if (NR>1) printf \"\\\\\\\\n\"; " +         // join lines with literal \\n
                    "printf \"%s\", $0" +
                    "}'); " +

                    // 4) print the JSON marker on a single line so TaskLogLineMatcher can parse it
                    "printf '::{\"outputs\":{\"%s\":\"%s\"}}::\\n' \"" + key + "\" \"$OUT_ESC\"; " +

                    // 5) propagate RC so CommandsWrapper can throw RunnableTaskException
                    "exit $RC";

            wrappedShellCommands.add(cmd);
        }

        return wrappedShellCommands;
    }

    @Override
    public NamespaceFiles getNamespaceFiles() {
        return namespaceFiles;
    }

    @Override
    public Object getInputFiles() {
        return inputFiles;
    }

    @Override
    public Property<List<String>> getOutputFiles() {
        return outputFiles;
    }
}
