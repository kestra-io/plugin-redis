package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.services.SerdeType;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Remove elements in a list"
)
public class ListPop extends AbstractRedisConnection implements RunnableTask<ListPop.Output> {

    @Schema(
            title = "Redis key",
            description = "The redis key you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
            title = "Count",
            description = "The number of value you want to retrieve"
    )
    @Builder.Default
    private Integer count = 1;

    @Schema(
            title = "Deserialization type",
            description = "Format of the data contained in Redis"
    )
    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);

        List<String> data = connection.listPop(key, count);
        Flowable<Object> flowable;
        Flowable<Integer> resultFlowable;
        File tempFile = runContext.tempFile(".ion").toFile();
        Integer countRes;
        try (OutputStream output = new FileOutputStream(tempFile)) {
            flowable = Flowable.fromArray(data.toArray());
            resultFlowable = flowable
                    .map(row -> {
                        FileSerde.write(output, getSerdeType().deserialize((String) row));
                        return 1;
                    });

            countRes = resultFlowable
                    .reduce(Integer::sum)
                    .blockingGet();
        }
        Output output = Output.builder().uri(runContext.putTempFile(tempFile)).count(countRes).build();
        runContext.metric(Counter.of("lineProcessed", count));

        connection.close();
        return output;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Number of data retrieved"
        )
        private Integer count;

        @Schema(
                title = "URI of a kestra internal storage file"
        )
        private URI uri;
    }
}
