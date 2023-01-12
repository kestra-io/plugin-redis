package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
                "url: redis://:redis@localhost:6379/0",
                "keys:",
                "   - keyDelete1",
                "   - keyDelete2"
            }
        )
    }
)
public class Delete extends AbstractRedisConnection implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The list of redis keys you want to delete"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private List<String> keys;

    @Schema(
        title = "If some keys are not deleted, failed the task"
    )
    @Builder.Default
    private Boolean failedOnMissing = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            long count = factory.del(runContext.render(keys));
            boolean isAllKeyDeleted = count == keys.size();

            if (!isAllKeyDeleted && failedOnMissing) {
                factory.close();
                throw new NullPointerException("Missing keys, only " + count + " key deleted");
            }

            runContext.metric(Counter.of("keys.deleted", count));
            factory.close();

            return Output.builder()
                .count((int) count)
                .build();
        }
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
