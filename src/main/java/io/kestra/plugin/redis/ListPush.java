package io.kestra.plugin.redis;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
import io.kestra.plugin.redis.services.RedisService;
import io.kestra.plugin.redis.services.SetOptions;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Redis Client Task",
        description = "Interact with REDIS"
)
public class ListPush extends AbstractRedisConnection implements RunnableTask<ListPush.Output> {

    @Schema(
            title = "Redis key",
            description = "The redis key you want to set"
    )
    @NotNull
    private String key;

    @Schema(
            title = "Redis value",
            description = "The list of value you want to push"
    )
    @NotNull
    private Object from;


    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);

        Integer count = 0;
        if (this.from instanceof String || this.from instanceof List) {
            Flowable<Object> flowable;
            Flowable<Integer> resultFlowable;
            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, connection);

                    count = resultFlowable
                            .reduce(Integer::sum)
                            .blockingGet();
                }
            } else {
                flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                resultFlowable = this.buildFlowable(flowable, connection);

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
    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RedisInterface connection) {
        return flowable
                .map(row -> {
                    connection.listPush(key, Arrays.asList(String.valueOf(row)));
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
