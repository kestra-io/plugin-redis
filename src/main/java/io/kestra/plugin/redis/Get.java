package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

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
            code = {
                "url: redis://:redis@localhost:6379/0",
                "key: mykey"
            }
        )
    }
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
