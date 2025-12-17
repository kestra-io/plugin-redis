package io.kestra.plugin.redis.cli;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Execute Redis CLI commands.",
    description = """
    This task allows running Redis CLI commands inside a Docker container with the official Redis image.
        Each command is executed sequentially. If a command fails, the task will fail immediately.
        The output from each command can be returned as JSON (requires Redis 7+) for easier parsing.
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
    aliases = "io.kestra.plugin.redis.RedisCLI",
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

    @Schema(
        title = "The Redis host to connect to."
    )
    @NotNull
    private Property<String> host;

    @Schema(
        title = "The Redis port to connect to."
    )
    @Builder.Default
    private Property<Integer> port = Property.ofValue(6379);

    @Schema(
        title = "The Redis database number to select.",
        description = "Default is 0."
    )
    @Builder.Default
    private Property<Integer> database = Property.ofValue(0);

    @Schema(
        title = "The Redis username for authentication.",
        description = "Required if your Redis server has ACL enabled."
    )
    private Property<String> username;

    @Schema(
        title = "The Redis password for authentication."
    )
    private Property<String> password;

    @Schema(
        title = "Enable TLS/SSL for the connection."
    )
    @Builder.Default
    private Property<Boolean> tls = Property.ofValue(false);

    @Schema(
        title = "The list of Redis CLI commands to execute.",
        description = "Each command is executed sequentially. If a command fails, the task fails immediately."
    )
    @NotNull
    private Property<List<String>> commands;

    @Schema(
        title = "Enable JSON output format.",
        description = "When enabled, commands output will be formatted as JSON. Requires Redis 7.0 or later."
    )
    @Builder.Default
    private Property<Boolean> jsonOutput = Property.ofValue(false);

    @Schema(
        title = "The Docker container image to use.",
        description = "Default is 'redis:7-alpine'."
    )
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.builder()
        .type(Docker.class.getName())
        .entryPoint(new ArrayList<>())
        .build();

    @Schema(
        title = "Docker options when using the Docker task runner."
    )
    @PluginProperty
    @Builder.Default
    protected Property<DockerOptions> docker = Property.ofValue(DockerOptions.builder().build());

    @Schema(
        title = "Additional environment variables for the task."
    )
    protected Property<Map<String, String>> env;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

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

        // Build the redis-cli base command with connection options
        StringBuilder baseCommand = new StringBuilder("redis-cli");
        baseCommand.append(" -h ").append(rHost);
        baseCommand.append(" -p ").append(rPort);
        baseCommand.append(" -n ").append(rDatabase);

        if (rUsername != null && !rUsername.isEmpty()) {
            baseCommand.append(" --user ").append(rUsername);
        }

        // Prefer REDISCLI_AUTH env var for password to avoid shell leaks

        if (rTls) {
            baseCommand.append(" --tls");
        }

        if (rJsonOutput) {
            baseCommand.append(" --json");
        }

        // Build shell commands for each Redis command
        List<String> shellCommands = new ArrayList<>();
        for (int i = 0; i < rCommands.size(); i++) {
            String redisCommand = rCommands.get(i);
            logger.debug("Preparing command {}", i + 1);

            // Each command needs to be executed and its exit code checked
            String fullCommand = baseCommand + " " + redisCommand;
            shellCommands.add(fullCommand);
        }

        // Prepare environment variables
        Map<String, String> envVars = new HashMap<>();
        var rEnvMap = runContext.render(env).asMap(String.class, String.class);
        if (rPassword != null && !rPassword.isEmpty()) {
            envVars.put("REDISCLI_AUTH", rPassword);
        }
        if (!rEnvMap.isEmpty()) {
            envVars.putAll(rEnvMap);
        }

        // Prepare output files list
        var rOutputFiles = runContext.render(outputFiles).asList(String.class);

        // Build Docker options with the container image
        DockerOptions dockerOptions = runContext.render(docker).as(DockerOptions.class).orElse(DockerOptions.builder().build());
        var dockerBuilder = dockerOptions.toBuilder();
        if (dockerOptions.getImage() == null) {
            dockerBuilder.image(rContainerImage);
        }

        // Create and run the CommandsWrapper
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(envVars)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(rOutputFiles.isEmpty() ? null : rOutputFiles)
            .withContainerImage(rContainerImage)
            .withTaskRunner(taskRunner)
            .withDockerOptions(dockerBuilder.build())
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")));

        // Execute each command with its own CommandsWrapper.run() and fail fast
        ScriptOutput lastOutput = null;
        for (String cmd : shellCommands) {
            lastOutput = commandsWrapper
                .withCommands(Property.ofValue(List.of(cmd)))
                .run();

            if (lastOutput.getExitCode() != 0) {
                throw new IllegalStateException("Redis CLI command failed with exit code: " + lastOutput.getExitCode());
            }
        }

        logger.info("Successfully executed {} Redis CLI command(s)", rCommands.size());
        runContext.metric(Counter.of("executed.commands.count", rCommands.size()));
        return lastOutput == null ? ScriptOutput.builder().exitCode(0).build() : lastOutput;
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
