package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
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
@Plugin(
    examples = {
        @Example(
            code = {
                "url: redis://:redis@localhost:6379/0",
                "key: mypopkeyjson",
                "serdeType: JSON",
                "maxRecords: 1"
            }
        )
    }
)
public class ListPop extends AbstractRedisConnection implements RunnableTask<ListPop.Output>, ListPopInterface {

    private String key;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    private Integer maxRecords;

    private Duration maxDuration;

    @Builder.Default
    private Integer count = 100;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String key = runContext.render(this.key);
            File tempFile = runContext.tempFile(".ion").toFile();

            if (this.maxDuration == null && this.maxRecords == null) {
                throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
            }

            try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                AtomicInteger total = new AtomicInteger();
                ZonedDateTime started = ZonedDateTime.now();

                Thread thread = new Thread(throwRunnable(() -> {
                    while (!this.ended(total, started)) {
                        List<String> data = factory.listPop(key, count);
                        for (String str : data) {
                            FileSerde.write(output, this.serdeType.deserialize(str));
                        }
                        total.getAndIncrement();
                    }
                }));

                runContext.metric(Counter.of("records", total.get(), "key", key));
                thread.setDaemon(true);
                thread.setName("redis-listPop");
                thread.start();

                while (!this.ended(total, started)) {
                    //noinspection BusyWait
                    Thread.sleep(100);
                }
                thread.join();

                return Output.builder().uri(runContext.putTempFile(tempFile)).count(total.get()).build();

            }
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
            title = "Number of elements retrieved"
        )
        private Integer count;

        @Schema(
            title = "URI of a kestra internal storage file"
        )
        private URI uri;
    }
}
