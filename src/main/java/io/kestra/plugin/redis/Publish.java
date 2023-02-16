package io.kestra.plugin.redis;

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
    title = "Publish one or multiple values to a channel"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: redis://:redis@localhost:6379/0",
                "channel: mych",
                "from:",
                "   - value1",
                "   - value2"
            }
        )
    }
)
public class Publish extends AbstractRedisConnection implements RunnableTask<Publish.Output> {
    @Schema(
        title = "The redis channel to publish."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String channel;

    @Schema(
        title = "The list of value to publish to the channel"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            Integer count = 0;
            if (this.from instanceof String || this.from instanceof List) {
                Flowable<Object> flowable;
                Flowable<Integer> resultFlowable;
                if (this.from instanceof String) {
                    URI from = new URI(runContext.render((String) this.from));
                    try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                        flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                        resultFlowable = this.buildFlowable(flowable, runContext, factory);

                        count = resultFlowable
                            .reduce(Integer::sum)
                            .blockingGet();
                    }
                } else {
                    flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                    resultFlowable = this.buildFlowable(flowable, runContext, factory);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                    runContext.metric(Counter.of("records", count));
                }
            }

            Output output = Output.builder().count(count).build();

            factory.close();

            return output;
        }
    }

    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, RedisFactory factory) {
        return flowable
            .map(row -> {
                factory.publish(
                    runContext.render(channel),
                    Collections.singletonList(serdeType.serialize(String.valueOf(row)))
                );

                return 1;
            });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "The number of value published"
        )
        private Integer count;
    }
}
