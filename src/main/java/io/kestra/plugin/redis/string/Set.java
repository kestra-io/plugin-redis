package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.lettuce.core.SetArgs;
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
    title = "Set a string value for a given Redis key.",
    description = "Set a string value for a new key or update the current key value with a new one."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_set
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
                    type: io.kestra.plugin.redis.string.Set
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                    value: "{{ inputs.key_value }}"
                    serdeType: STRING
                """
        )
    },
    aliases = "io.kestra.plugin.redis.Set"
)
public class Set extends AbstractRedisConnection implements RunnableTask<Set.Output> {

    @Schema(
        title = "The redis key you want to set."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "The value you want to set.",
        description = "Must be a string for `serdeType: STRING` or can be an object or a json string `serdeType: JSON`"
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
    private Property<Boolean> get = Property.of(false);

    @Schema(
        title = "Format of the data contained in Redis."
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String oldValue = null;
            String key = runContext.render(this.key).as(String.class).orElseThrow();
            String value = runContext.render(serdeType).as(SerdeType.class).orElseThrow()
                .serialize(runContext.render(this.value).as(Object.class).orElseThrow());

            if (runContext.render(get).as(Boolean.class).orElse(false)) {
                oldValue = factory.getSyncCommands().setGet(key, value);
            } else {
                factory.getSyncCommands().set(key, value, options.asRedisSet());
            }

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
        private String oldValue;
    }

    @Builder
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Set the expiration duration."
        )
        private Duration expirationDuration;

        @Schema(
            title = "Set the expiration date."
        )
        private ZonedDateTime expirationDate;

        @Schema(
            title = "Only set the key if it does not already exist."
        )
        @Builder.Default
        private boolean mustNotExist = false;

        @Schema(
            title = "Only set the key if it already exist."
        )
        @Builder.Default
        private boolean mustExist = false;

        @Schema(
            title = "Retain the time to live associated with the key."
        )
        @Builder.Default
        private boolean keepTtl = false;

        public SetArgs asRedisSet() {
            SetArgs setArgs = new SetArgs();

            if (expirationDuration != null) {
                setArgs.px(expirationDuration);
            }

            if (expirationDate != null) {
                setArgs.pxAt(expirationDate.toInstant().toEpochMilli());
            }

            if (mustNotExist) {
                setArgs.nx();
            }

            if (mustExist) {
                setArgs.xx();
            }

            if (keepTtl) {
                setArgs.keepttl();
            }

            return setArgs;
        }
    }
}
