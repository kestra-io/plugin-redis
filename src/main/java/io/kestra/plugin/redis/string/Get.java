package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Get a key."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.redis.string.Get    
                    url: redis://:redis@localhost:6379/0
                    key: mykey
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
    @PluginProperty(dynamic = true)
    private String key;

    @Builder.Default
    @Schema(
        title = "Format of the data contained in Redis"
    )
    @NotNull
    @PluginProperty
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String key = runContext.render((this.key));
            String data = factory.get(key);

            factory.close();
            return Output.builder().data(serdeType.deserialize(data)).key(key).build();
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
