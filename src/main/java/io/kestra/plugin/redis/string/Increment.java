package io.kestra.plugin.redis.string;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
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
    title = "Increment a Redis string number",
    description = "Runs `INCR`, `INCRBY`, or `INCRBYFLOAT` on the rendered key depending on the amount type, and can apply an expiration (absolute or relative, not both)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_increment
                namespace: company.team

                inputs:
                    - id: key_name
                      type: STRING
                      displayName: Key name to increment

                tasks:
                  - id: increment
                    type: io.kestra.plugin.redis.string.Increment
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                """
        )
    }
)
public class Increment extends AbstractRedisConnection implements RunnableTask<Increment.Output> {
    @Schema(
        title = "Redis key to increment",
        description = "Rendered before the increment command."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Increment amount",
        description = "Optional; defaults to 1. Integer uses INCR/INCRBY, decimal uses INCRBYFLOAT."
    )
    private Property<Number> amount;

    @Schema(
        title = "Expiration options",
        description = "Optional TTL settings; choose either duration or absolute date."
    )
    private Options options;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();

            Number increment = runContext.render(amount).as(Number.class)
                .map(number -> {
                    if (number instanceof Long) {
                        return factory.getSyncCommands().incrby(renderedKey, number.longValue());
                    } else {
                        return factory.getSyncCommands().incrbyfloat(renderedKey, number.doubleValue());
                    }
                })
                .orElseGet(() -> factory.getSyncCommands().incr(renderedKey));

            if (options != null) {
                options.applyExpiration(runContext, factory, renderedKey);
            }

            return Output.builder()
                .value(increment)
                .key(renderedKey)
                .build();
        }
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Expiration duration",
            description = "Relative TTL; cannot be combined with expirationDate."
        )
        private Property<Duration> expirationDuration;

        @Schema(
            title = "Expiration date",
            description = "Absolute timestamp; cannot be combined with expirationDuration."
        )
        private Property<ZonedDateTime> expirationDate;

        public void applyExpiration(RunContext runContext, RedisFactory factory, String key) throws IllegalVariableEvaluationException {
            var rExpirationDuration = runContext.render(expirationDuration).as(Duration.class).orElse(null);
            var rExpirationDate = runContext.render(expirationDate).as(ZonedDateTime.class).orElse(null);

            if (rExpirationDuration != null && rExpirationDate != null) {
                throw new IllegalArgumentException(
                    "Invalid Redis options: you can't use both 'expirationDuration' and 'expirationDate'.\n" +
                        "Use either expirationDuration for a relative TTL, or expirationDate for an absolute expiration time."
                );
            }

            if (rExpirationDuration != null) {
                long seconds = rExpirationDuration.getSeconds();
                if (seconds > 0) {
                    factory.getSyncCommands().expire(key, seconds);
                    runContext.logger().debug("Set TTL of {} seconds on key '{}'", seconds, key);
                }
            }

            if (rExpirationDate != null) {
                long epochSeconds = rExpirationDate.toEpochSecond();
                factory.getSyncCommands().expireat(key, epochSeconds);
                runContext.logger().debug("Set expiration at {} for key '{}'", rExpirationDate, key);
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Incremented value"
        )
        private Number value;

        @Schema(
            title = "Incremented key"
        )
        private String key;
    }
}
