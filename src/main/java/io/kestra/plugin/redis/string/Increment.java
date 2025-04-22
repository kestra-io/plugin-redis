package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Increment a Redis item by key and return its value.",
    description = "Increment for a key in a Redis database and return the associated value."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_get
                namespace: company.team

                inputs:
                    - id: key_name
                      type: STRING
                      displayName: Key name to search

                tasks:
                  - id: get
                    type: io.kestra.plugin.redis.string.Increment
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
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
        title = "The amount to increment"
    )
    private Property<Number> amount;

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

            return Output.builder()
                .value(increment)
                .key(renderedKey)
                .build();
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
