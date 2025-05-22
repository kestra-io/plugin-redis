package io.kestra.plugin.redis.json;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.lettuce.core.json.JsonPath;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
            String renderedPath = runContext.render(this.path).as(String.class).orElseThrow();

            Number increment = runContext.render(amount).as(Number.class).orElse(1);

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
