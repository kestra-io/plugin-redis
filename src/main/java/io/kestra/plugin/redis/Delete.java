package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisService;
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
@Plugin(
    examples = {
        @Example(
            code = {
                "uri: redis://:redis@localhost:6379/0",
                "keys:",
                "   - keyDelete1",
                "   - keyDelete2"
            }
        )
    }
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
        RedisService connection = this.redisFactory(runContext);

        Long count = connection.del(runContext.render(keys));
        boolean isAllKeyDeleted = count == keys.size();

        if (!isAllKeyDeleted && failedOnMissing) {
            connection.close();
            throw new NullPointerException("Missing keys, only " + count + " key deleted");
        }
        connection.close();
        runContext.metric(Counter.of("keys.deleted", count));

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
