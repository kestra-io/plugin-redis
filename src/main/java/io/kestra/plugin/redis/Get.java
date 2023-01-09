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
    title = "Get the value of a key."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "uri: redis://:redis@localhost:6379/0",
                "key: mykey"
            }
        )
    }
)
public class Get extends AbstractRedisConnection implements RunnableTask<Get.Output> {

    @Schema(
        title = "Redis key",
        description = "The redis key you want to get"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Builder.Default
    @Schema(
        title = "Deserialization type",
        description = "Format of the data contained in Redis"
    )
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
            title = "Data from the key",
            description = "Data we get from the key"
        )
        private Object data;
        @Schema(
            title = "Key of the retrieved data"
        )
        private String key;
    }
}
