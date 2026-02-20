package io.kestra.plugin.redis.string;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete Redis string keys",
    description = "Runs `DEL` on the rendered key list, counts deletions, and can fail when not all keys are removed."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.redis.string.Delete
                    url: redis://:redis@localhost:6379/0
                    keys:
                      - keyDelete1
                      - keyDelete2
                """
        )
    },
    metrics = {
        @Metric(
            name = "deleted.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records deleted from Redis."
        )
    },
    aliases = "io.kestra.plugin.redis.Delete"
)
public class Delete extends AbstractRedisConnection implements RunnableTask<Delete.Output> {
    @Schema(
        title = "Keys to delete",
        description = "Rendered list passed to `DEL`."
    )
    @NotNull
    private Property<List<String>> keys;

    @Schema(
        title = "Fail when deletions are missing",
        description = "Defaults to false; when true, throws if fewer keys are deleted than requested."
    )
    @Builder.Default
    private Property<Boolean> failedOnMissing = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            final List<String> renderedKeys = runContext.render(keys).asList(String.class);
            long count = factory.getSyncCommands().del(renderedKeys.toArray(new String[0]));
            boolean isAllKeyDeleted = count == renderedKeys.size();

            if (!isAllKeyDeleted && runContext.render(failedOnMissing).as(Boolean.class).orElse(false)) {
                throw new NullPointerException("Missing keys, only " + count + " key deleted");
            }

            runContext.metric(Counter.of("deleted.records.count", count));

            return Output.builder()
                .count((int) count)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Deleted count",
            description = "Total deleted keys."
        )
        private Integer count;
    }
}
