package io.kestra.plugin.redis.json;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisJsonCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.*;
import io.lettuce.core.json.arguments.JsonSetArgs;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import java.time.ZonedDateTime;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write JSON value to a Redis key",
    description = "Runs `JSON.SET` on the rendered key and path (default `$`), optionally returning the previous value and supporting NX/XX-style existence checks."
)
@Plugin(
    examples = {
        @Example(
            title = "Set a JSON value.",
            full = true,
            code = """
                id: redis_json_set
                namespace: company.team

                inputs:
                  - id: key_name
                    type: STRING
                    displayName: Key Name

                  - id: key_value
                    type: STRING
                    displayName: Key Value

                tasks:
                  - id: set
                    type: io.kestra.plugin.redis.json.Set
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                    value: |
                      {
                        "name": "{{ inputs.key_value }}"
                      }
                """
        )
    }
)
public class Set extends AbstractRedisConnection implements RunnableTask<Set.Output> {

    @Schema(
        title = "Redis key to set",
        description = "Rendered before calling `JSON.SET`."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "JSON value to store",
        description = "Rendered and serialized with the JSON serde."
    )
    @NotNull
    private Property<Object> value;

    @Schema(
        title = "Set options",
        description = "NX/XX-like guards for JSON.SET; see [Redis documentation](https://redis.io/commands/json.set/)."
    )
    @PluginProperty
    @Builder.Default
    private Options options = Options.builder().build();

    @Schema(
        title = "Return existing value",
        description = "Defaults to false; when true, fetches the current value before overwriting (not supported on Redis 5.x)."
    )
    @Builder.Default
    private Property<Boolean> get = Property.ofValue(false);

    @Schema(
        title = "JSON path to set",
        description = "Defaults to `$` (root)."
    )
    @Builder.Default
    private Property<String> path = Property.ofValue("$");

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            String key = runContext.render(this.key).as(String.class).orElseThrow();
            String value = SerdeType.JSON.serialize(runContext.render(this.value).as(Object.class).orElseThrow());

            String renderedPath = runContext.render(this.path).as(String.class).orElse("$");

            Object oldValue = null;
            if (runContext.render(get).as(Boolean.class).orElse(false)) {
                oldValue = factory.getSyncCommands().jsonGet(key, JsonPath.of(renderedPath)).getFirst().toObject(Object.class);
            }

            factory.getSyncCommands().jsonSet(key, JsonPath.of(renderedPath), new DefaultJsonParser().createJsonValue(value),
                    options.asRedisSet(runContext));

            Output.OutputBuilder builder = Output.builder();

            if (oldValue != null) {
                builder.oldValue(oldValue);
            }

            return builder.build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Previous value",
            description = "Returned only when `get` is true."
        )
        private Object oldValue;
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Set only when key is absent",
            description = "Applies NX behavior; do not combine with mustExist."
        )
        @Builder.Default
        private Property<Boolean> mustNotExist = Property.ofValue(false);

        @Schema(
            title = "Set only when key exists",
            description = "Applies XX behavior; do not combine with mustNotExist."
        )
        @Builder.Default
        private Property<Boolean> mustExist = Property.ofValue(false);


        public JsonSetArgs asRedisSet(RunContext runContext) throws IllegalVariableEvaluationException {
            JsonSetArgs setArgs = new JsonSetArgs();

            runContext.render(mustNotExist).as(Boolean.class)
                .filter(b -> b)
                .ifPresent(b -> setArgs.nx());

            runContext.render(mustExist).as(Boolean.class)
                .filter(b -> b)
                .ifPresent(b -> setArgs.xx());

            return setArgs;
        }
    }
}
