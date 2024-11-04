package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.lettuce.core.SetArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import java.time.ZonedDateTime;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Set the string value of a key."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_set
                namespace: company.team

                tasks:
                  - id: set
                    type: io.kestra.plugin.redis.string.Set
                    url: redis://:redis@localhost:6379/0
                    key: mykey
                    value: myvalue
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
        title = "The value you want to set."
    )
    @NotNull
    private Property<String> value;

    @Schema(
        title = "Options available when setting a key in Redis.",
        description = "See [redis documentation](https://redis.io/commands/set/)."
    )
    @Builder.Default
    private Property<Options> options = Property.of(Options.builder().build());

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
            String oldValue = factory.set(
                runContext.render(key).as(String.class).orElseThrow(),
                runContext.render(serdeType).as(SerdeType.class).orElseThrow().serialize(runContext.render(value)),
                runContext.render(get).as(Boolean.class).orElse(false),
                runContext.render(options).as(Options.class).orElse(Options.builder().build()).asRedisSet()
            );

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
