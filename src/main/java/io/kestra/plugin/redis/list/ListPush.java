package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Add a new element to the head of a list in Redis."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_list_push
                namespace: company.team

                tasks:
                  - id: list_push
                    type: io.kestra.plugin.redis.list.ListPush
                    url: redis://:redis@localhost:6379/0
                    key: mykey
                    from:
                      - value1
                      - value2
                """
        )
    },
    metrics = {
        @Metric(
            name = "inserted.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records pushed to a Redis list."
        )
    },
    aliases = "io.kestra.plugin.redis.ListPush"
)
public class ListPush extends AbstractRedisConnection implements RunnableTask<ListPush.Output> {
    @Schema(
        title = "The Redis key for the list."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "The list of values to push at the head of the list.",
        anyOf = {String.class, List.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
             Object from = null;
            if (this.from instanceof String fromString) {
                String renderedFrom = runContext.render(fromString);
                try {
                    from = JacksonMapper.ofJson().readValue(renderedFrom, List.class);
                } catch (Exception e) {
                    from = renderedFrom;
                }
            } else {
                from = this.from;
            }
            Integer count = io.kestra.core.models.property.Data
                    .from(from).readAs(runContext, String.class, obj -> obj.toString()).map(throwFunction(row -> {
                factory.getSyncCommands().lpush(
                    runContext.render(key).as(String.class).orElseThrow(),
                    Collections.singletonList(runContext.render(serdeType).as(SerdeType.class).orElse(SerdeType.STRING).serialize(runContext.render(row))).toArray(new String[0])
                );

                return 1;
            })).reduce(Integer::sum).blockOptional().orElse(0);

            runContext.metric(Counter.of("inserted.records.count", count));
            return Output.builder().count(count).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "The number of values inserted"
        )
        private Integer count;
    }
}
