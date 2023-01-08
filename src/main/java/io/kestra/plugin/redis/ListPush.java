package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.services.RedisService;
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
                "uri: redis://:redis@localhost:6379/0",
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
        title = "Redis key",
        description = "The redis key you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "Redis value",
        description = "The list of value you want to push"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisService connection = this.redisFactory(runContext);

        Integer count = 0;
        if (this.from instanceof String || this.from instanceof List) {
            Flowable<Object> flowable;
            Flowable<Integer> resultFlowable;
            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, runContext, connection);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }
            } else {
                flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                resultFlowable = this.buildFlowable(flowable, runContext, connection);

                count = resultFlowable
                    .reduce(Integer::sum)
                    .blockingGet();
                runContext.metric(Counter.of("lineProcessed", count));
            }
        }

        Output output = Output.builder().count(count).build();

        connection.close();

        return output;
    }

    @SuppressWarnings("unchecked")
    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, RedisService connection) {
        return flowable
            .map(row -> {
                connection.listPush(runContext.render(key), Arrays.asList(serdeType.serialize(String.valueOf(row))));

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
