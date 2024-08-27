package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Removes and returns an element from the head of a list."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_list_pop
                namespace: company.team

                tasks:
                  - id: list_pop
                    type: io.kestra.plugin.redis.list.ListPop
                    url: redis://:redis@localhost:6379/0
                    key: mypopkeyjson
                    serdeType: JSON
                    maxRecords: 1
                """
        )
    },
    aliases = "io.kestra.plugin.redis.ListPop"
)
public class ListPop extends AbstractRedisConnection implements RunnableTask<ListPop.Output>, ListPopInterface {

    private String key;

    @Schema(
        title = "Format of the data contained in Redis."
    )
    @Builder.Default
    @PluginProperty
    @NotNull
    private SerdeType serdeType = SerdeType.STRING;

    private Integer maxRecords;

    private Duration maxDuration;

    @Builder.Default
    private Integer count = 100;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String key = runContext.render(this.key);
            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

            if (this.maxDuration == null && this.maxRecords == null) {
                throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
            }

            try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                AtomicInteger total = new AtomicInteger();
                ZonedDateTime started = ZonedDateTime.now();

                boolean empty;
                do {
                    List<String> data = factory.listPop(key, count);
                    empty = data.isEmpty();
                    var flux = Flux.fromIterable(data).map(throwFunction(str -> this.serdeType.deserialize(str)));
                    Mono<Long> longMono = FileSerde.writeAll(output, flux);
                    total.addAndGet(longMono.block().intValue());
                }
                while (!this.ended(empty, total, started));

                output.flush();

                runContext.metric(Counter.of("records", total.get(), "key", key));

                return Output.builder().uri(runContext.storage().putFile(tempFile)).count(total.get()).build();
            }
        }
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
