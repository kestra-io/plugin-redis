package io.kestra.plugin.redis;

import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.SerdeType;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
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
        title = "Redis Client Task",
        description = "Interact with REDIS"
)
public class Get extends AbstractRedisConnection implements RunnableTask<Get.Output>{

    @Schema(
            title = "Redis key",
            description = "The redis key you want to get"
    )
    @NotNull
    private String key;

    @Builder.Default
    private SerdeType serdeType = SerdeType.JSON;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);
        String data = connection.get(key);

        connection.close();
        return Output.builder().data(data).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Data from the key",
                description = "Data we get from the key"
        )
        private String data;
    }
}
