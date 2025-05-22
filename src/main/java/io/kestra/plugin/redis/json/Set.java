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
    title = "Set a JSON type value for a given Redis key.",
    description = "Set a JSON type value for a new key or update the current key value with a new one."
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
        title = "The redis key you want to set."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "The value you want to set as type JSON."
    )
    @NotNull
    private Property<Object> value;

    @Schema(
        title = "Options available when setting a key in Redis.",
        description = "See [redis documentation](https://redis.io/commands/set/)."
    )
    @PluginProperty
    @Builder.Default
    private Options options = Options.builder().build();

    @Schema(
        title = "Define if you get the older value in response, does not work with Redis 5.X."
    )
    @Builder.Default
    private Property<Boolean> get = Property.ofValue(false);

    @Schema(
        title = "JSON path to extract value (default is root '$')"
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
            title = "Old value",
            description = "The old value if you replaced an existing key\n" +
                "Required Get to true"
        )
        private Object oldValue;
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Only set the key if it does not already exist."
        )
        @Builder.Default
        private Property<Boolean> mustNotExist = Property.ofValue(false);

        @Schema(
            title = "Only set the key if it already exist."
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
