package io.kestra.plugin.redis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.models.SerdeType;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Prepend one or multiple values to a list"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: redis://:redis@localhost:6379/0",
                "key: mykey",
                "from:",
                "   - value1",
                "   - value2"
            }
        )
    }
)
public class ListPush extends AbstractRedisConnection implements RunnableTask<ListPush.Output> {
    @Schema(
        title = "The redis key for the list."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The list of value to push at head of the list",
        anyOf = {String.class, List.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            Integer count;
            if (this.from instanceof String fromUrl) {
                URI from = new URI(runContext.render(fromUrl));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    Flowable<Object> flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                    Flowable<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }
            } else if (this.from instanceof List<?> fromList) {
                Flowable<Object> flowable = Flowable.fromArray((fromList).toArray());
                Flowable<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                count = resultFlowable
                    .reduce(Integer::sum)
                    .blockingGet();
            }
            else {
                // should not occur as validation mandates String or List
                throw new IllegalVariableEvaluationException("Invalid 'from' property type :" + from.getClass());
            }

            runContext.metric(Counter.of("records", count));
            return Output.builder().count(count).build();
        }
    }

    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, RedisFactory factory) {
        return flowable
            .map(row -> {
                factory.listPush(
                    runContext.render(key),
                    Collections.singletonList(serdeType.serialize(row))
                );

                return 1;
            });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "The number of value inserted"
        )
        private Integer count;
    }
}
