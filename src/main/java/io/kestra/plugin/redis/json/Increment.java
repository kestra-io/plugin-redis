package io.kestra.plugin.redis.json;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.lettuce.core.json.JsonPath;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Increment a Redis item by key, JSON path and return its value.",
    description = "Increment for a key in a Redis database and return the associated value."
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
                    type: io.kestra.plugin.redis.json.Increment
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                    path: "$"
                """
        )
    }
)
public class Increment extends AbstractRedisConnection implements RunnableTask<Increment.Output> {
    @Schema(
        title = "The redis key you want to increment"
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "The amount to increment, default is 1"
    )
    @Builder.Default
    private Property<Number> amount = Property.ofValue(1);

    @Schema(
        title = "JSON path to increment value."
    )
    @NotNull
    private Property<String> path;

    @Schema(
        title = "Options for the increment operation.",
        description = "Configure settings for the key."
    )
    @PluginProperty
    private Options options;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
            String renderedPath = runContext.render(this.path).as(String.class).orElseThrow();

            Number increment = runContext.render(amount).as(Number.class).orElse(1);

            if (options != null) {
                options.applyExpiration(runContext, factory, renderedKey);
            }

            List<Number> result = factory.getSyncCommands().jsonNumincrby(renderedKey, JsonPath.of(renderedPath), increment);

            runContext.logger().info("Result: {}", result);

            return Output.builder()
                .value(result.getFirst())
                .key(renderedKey)
                .build();
        }
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Options {
        @Schema(
            title = "Set the expiration duration.",
            description = "Duration after which the key will automatically expire."
        )
        private Property<Duration> expirationDuration;

        @Schema(
            title = "Set the expiration date.",
            description = "Absolute timestamp at which the key will expire."
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
                factory.getSyncCommands().expireat(key, rExpirationDate.toEpochSecond());
                runContext.logger().debug("Set expiration at {} for key '{}'", rExpirationDate, key);
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The incremented value."
        )
        private Number value;

        @Schema(
            title = "The fetched key."
        )
        private String key;
    }
}
