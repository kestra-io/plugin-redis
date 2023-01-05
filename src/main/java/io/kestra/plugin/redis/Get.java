package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisService;
import io.kestra.plugin.redis.services.SerdeType;
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
        RedisService connection = this.redisFactory(runContext);
        String data = connection.get(runContext.render(key));

        connection.close();

        return Output.builder().data(serdeType.deserialize(data)).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Data from the key",
            description = "Data we get from the key"
        )
        private Object data;
    }
}
