package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Delete one or more keys."
)
public class Delete extends AbstractRedisConnection implements RunnableTask<Delete.Output> {

    @Schema(
            title = "Redis keys",
            description = "The list of redis keys you want to delete"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private List<String> keys;

    @Schema(
            title = "failedOnMissing",
            description = "If some keys are not deleted, failed the task"
    )
    @Builder.Default
    private Boolean failedOnMissing = false;


    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);
        Long count;
        boolean AllKeyDeleted;

        count = connection.del(keys);
        AllKeyDeleted = count == keys.size();

        if(!AllKeyDeleted && failedOnMissing){
            connection.close();
            throw new NullPointerException("Missing keys, only " + count + " key deleted");
        }
        connection.close();
        runContext.metric(Counter.of("keyProcessed", count));
        return Output.builder().count(count.intValue()).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Number of key deleted"
        )
        private Integer count;
    }
}
