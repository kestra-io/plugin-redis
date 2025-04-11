package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a key and return its value in Redis.",
    description = "Query for a key in a Redis database and return the associated value."
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
                    type: io.kestra.plugin.redis.string.Get
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                """
        )
    },
    aliases = "io.kestra.plugin.redis.Get"
)
public class Get extends AbstractRedisConnection implements RunnableTask<Get.Output> {
    @Schema(
        title = "The redis key you want to get"
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    @Schema(
        title = "If some keys are not defined, failed the task."
    )
    @Builder.Default
    private Property<Boolean> failedOnMissing = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
            String data = factory.get(renderedKey);


            if (data == null && runContext.render(failedOnMissing).as(Boolean.class).orElseThrow()) {
                throw new NullPointerException("Missing keys '" + renderedKey + "'");
            }

            return Output.builder()
                .data(runContext.render(this.serdeType).as(SerdeType.class).orElseThrow().deserialize(data))
                .key(renderedKey)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The fetched data."
        )
        private Object data;

        @Schema(
            title = "The fetched key."
        )
        private String key;
    }
}
