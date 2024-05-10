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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Remove elements from a list."
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

                boolean empty;
                do {
                    List<String> data = factory.listPop(key, count);
                    empty = data.isEmpty();
                    for (String str : data) {
                        FileSerde.write(output, this.serdeType.deserialize(str));
                        total.getAndIncrement();
                    }
                }
                while (!this.ended(empty, total, started));

                output.flush();

                runContext.metric(Counter.of("records", total.get(), "key", key));

                return Output.builder().uri(runContext.putTempFile(tempFile)).count(total.get()).build();
            }
        }
    }

    public Publisher<Object> stream(RunContext runContext) throws Exception {
        return Flux.create(
                fluxSink -> {
                    try (RedisFactory factory = this.redisFactory(runContext)) {
                        String key = runContext.render(this.key);

                        while (true) {
                            factory.listPop(key, 1)
                                .forEach(throwConsumer(s -> fluxSink.next(this.serdeType.deserialize(s))));

                            Thread.sleep(100);
                        }
                    } catch (Throwable e) {
                        fluxSink.error(e);
                    } finally {
                        fluxSink.complete();
                    }
                },
                FluxSink.OverflowStrategy.BUFFER
            )
            .subscribeOn(Schedulers.boundedElastic());
    }


    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(boolean empty, AtomicInteger count, ZonedDateTime start) {
        if (empty) {
            return true;
        }

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
            title = "Number of elements retrieved."
        )
        private Integer count;

        @Schema(
            title = "URI of a Kestra internal storage file."
        )
        private URI uri;
    }
}
