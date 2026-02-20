package io.kestra.plugin.redis.string;

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
    title = "Write a string value to Redis",
    description = "Runs `SET` on the rendered key using the selected serde (STRING or JSON), supports NX/XX guards, TTL options, keep-ttl, and can return the previous value."
)
@Plugin(
    examples = {
        @Example(
            title = "Set a string value.",
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
                    get: true
                    options:
                      mustExist: true
                      keepTtl: true
                """
        ),
        @Example(
            title = "Set a JSON value.",
            full = true,
            code = """
                id: redis_set_json
                namespace: company.team

                tasks:
                - id: set
                    type: io.kestra.plugin.redis.string.Set
                    url: "{{ secret('REDIS_URI')}}"
                    key: "key_json_{{ execution.id }}"
                    value: |
                    {{ {
                        "flow": flow.id,
                        "namespace": flow.namespace
                    } | toJson }}
                    serdeType: JSON
                - id: get
                    type: io.kestra.plugin.redis.string.Get
                    url: "{{ secret('REDIS_URI')}}"
                    serdeType: JSON
                    key: "key_json_{{ execution.id }}"
                """
        )
    },
    aliases = "io.kestra.plugin.redis.Set"
)
public class Set extends AbstractRedisConnection implements RunnableTask<Set.Output> {

    @Schema(
        title = "Redis key to set",
        description = "Rendered before calling `SET`."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Value to store",
        description = "Rendered then serialized with the chosen serde; STRING expects plain text, JSON accepts object or JSON string."
    )
    @NotNull
    private Property<Object> value;

    @Schema(
        title = "Set options",
        description = "TTL, NX/XX, and keepTtl flags; see [Redis documentation](https://redis.io/commands/set/)."
    )
    @PluginProperty
    @Builder.Default
    private Options options = Options.builder().build();

    @Schema(
        title = "Return existing value",
        description = "Defaults to false; when true, uses SETGET to return the prior value (not supported on Redis 5.x)."
    )
    @Builder.Default
    private Property<Boolean> get = Property.ofValue(false);

    @Schema(
        title = "Serialization format",
        description = "Defaults to STRING; set to JSON to serialize/deserialize structured values."
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String oldValue = null;
            String key = runContext.render(this.key).as(String.class).orElseThrow();
            String value = runContext.render(serdeType).as(SerdeType.class).orElseThrow()
                .serialize(runContext.render(this.value).as(Object.class).orElseThrow());

            if (runContext.render(get).as(Boolean.class).orElse(false)) {
                oldValue = factory.getSyncCommands().setGet(key, value, options.asRedisSet(runContext));
            } else {
                factory.getSyncCommands().set(key, value, options.asRedisSet(runContext));
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
            title = "Previous value",
            description = "Returned only when `get` is true."
        )
        private String oldValue;
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Expiration duration",
            description = "Relative TTL; cannot be combined with expirationDate when keepTtl is true."
        )
        private Property<Duration> expirationDuration;

        @Schema(
            title = "Expiration date",
            description = "Absolute timestamp; cannot be combined with expirationDuration when keepTtl is true."
        )
        private Property<ZonedDateTime> expirationDate;

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

        @Schema(
            title = "Keep existing TTL",
            description = "Keeps current TTL instead of resetting; cannot be used with expirationDuration or expirationDate."
        )
        @Builder.Default
        private Property<Boolean> keepTtl = Property.ofValue(false);

        public SetArgs asRedisSet(RunContext runContext) throws IllegalVariableEvaluationException {
            SetArgs setArgs = new SetArgs();

            Duration renderedExpirationDuration = runContext.render(expirationDuration).as(Duration.class).orElse(null);
            ZonedDateTime renderedExpirationDate = runContext.render(expirationDate).as(ZonedDateTime.class).orElse(null);
            Boolean renderedKeepTtl = runContext.render(keepTtl).as(Boolean.class).orElse(false);

            if (renderedKeepTtl && (renderedExpirationDuration != null || renderedExpirationDate != null)) {
                throw new IllegalArgumentException(
                    "Invalid Redis options: you can't use 'keepTtl' with 'expirationDuration' or 'expirationDate'.\n" +
                        "Use either keepTtl to keep existing TTL, or set a new expiration."
                );
            }

            if (renderedExpirationDuration != null) {
                setArgs.px(renderedExpirationDuration);
            }

            if (renderedExpirationDate != null) {
                setArgs.pxAt(renderedExpirationDate.toInstant().toEpochMilli());
            }

            runContext.render(mustNotExist).as(Boolean.class)
                .filter(b -> b)
                .ifPresent(b -> setArgs.nx());

            runContext.render(mustExist).as(Boolean.class)
                .filter(b -> b)
                .ifPresent(b -> setArgs.xx());

            runContext.render(keepTtl).as(Boolean.class)
                .filter(b -> b)
                .ifPresent(b -> setArgs.keepttl());

            return setArgs;
        }
    }
}
