package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.services.SerdeType;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwRunnable;

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
        title = "Deserialization type",
        description = "Format of the data contained in Redis"
    )
    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Schema(
        title = "The max number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    private Integer maxRecords;

    @Schema(
        title = "The max duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second."
    )
    private Duration maxDuration;

    @Schema(
        title = "Number of elements that should be pop at once"
    )
    @Builder.Default
    private Integer count = 1;

    @Override

    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);

        File tempFile = runContext.tempFile(".ion").toFile();
        Thread thread = null;

        if (this.maxDuration == null && this.maxRecords == null) {
            throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
        }

        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            Map<String, Integer> lineCount = new HashMap<>();
            AtomicInteger total = new AtomicInteger();
            ZonedDateTime started = ZonedDateTime.now();

            thread = new Thread(throwRunnable(() -> {
                while (true) {
                    List<String> data = connection.listPop(runContext.render(key), count);
                    for (String str : data) {
                        FileSerde.write(output, str);
                    }
                    total.getAndIncrement();
                    lineCount.compute(key, (s, integer) -> integer == null ? 1 : integer + 1);
                }
            }));

            lineCount.forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));
            thread.setDaemon(true);
            thread.setName("mqtt-subscribe");
            thread.start();

            while (!this.ended(total, started)) {
                //noinspection BusyWait
                Thread.sleep(100);
            }

            connection.close();
            return Output.builder().uri(runContext.putTempFile(tempFile)).count(total.get()).build();

        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start) {
        if (this.maxRecords != null && count.get() >= this.maxRecords) {
            return true;
        }

        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        return false;
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
