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
public class Ttl extends AbstractRedisConnection implements RunnableTask<Ttl.Output> {
    @Schema(
        title = "The redis key you want to increment"
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "The amount to increment, default is 1"
    )
    private Property<Number> amount;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();

            Long ttl = factory.getSyncCommands().ttl(renderedKey);
            return Output.builder()
                .ttl(ttl)
                .key(renderedKey)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The ttl value."
        )
        private Long ttl;

        @Schema(
            title = "The fetched key."
        )
        private String key;
    }
}
