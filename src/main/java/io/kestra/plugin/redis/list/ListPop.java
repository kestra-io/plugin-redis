package io.kestra.plugin.redis.list;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Remove and return an element from the head of a list in Redis."
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
    metrics = {
        @Metric(
            name = "deleted.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records retrieved from Redis List."
        )
    },
    aliases = "io.kestra.plugin.redis.ListPop"
)
public class ListPop extends AbstractRedisConnection implements RunnableTask<ListPop.Output>, ListPopInterface {

    private Property<String> key;

    @Schema(
        title = "Format of the data contained in Redis."
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<Integer> count = Property.ofValue(100);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();

            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

            if (runContext.render(this.maxDuration).as(Duration.class).isEmpty() &&
                runContext.render(this.maxRecords).as(Integer.class).isEmpty()) {
                throw new IllegalArgumentException("maxDuration or maxRecords must be set to avoid infinite loop");
            }

            try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                AtomicInteger total = new AtomicInteger();
                ZonedDateTime started = ZonedDateTime.now();

                boolean empty;
                do {
                    List<String> data = factory.getSyncCommands().lpop(renderedKey, runContext.render(this.count).as(Integer.class).orElse(100));
                    empty = data.isEmpty();

                    var flux = Flux
                        .fromIterable(data)
                        .map(throwFunction(str -> runContext
                            .render(this.serdeType)
                            .as(SerdeType.class)
                            .orElse(SerdeType.STRING)
                            .deserialize(str)
                        )
                    );

                    Mono<Long> longMono = FileSerde.writeAll(output, flux);

                    total.addAndGet(longMono.block().intValue());
                }
                while (!this.ended(runContext, empty, total, started));

                output.flush();

                runContext.metric(Counter.of("deleted.records.count", total.get(), "key", renderedKey));

                return Output.builder().uri(runContext.storage().putFile(tempFile)).count(total.get()).build();
            }
        }
    }


    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(RunContext runContext, boolean empty, AtomicInteger count, ZonedDateTime start) throws IllegalVariableEvaluationException {
        if (empty) {
            return true;
        }
        final Optional<Integer> renderedMaxRecords = runContext.render(this.maxRecords).as(Integer.class);
        if (renderedMaxRecords.isPresent() && count.get() >= renderedMaxRecords.get()) {
            return true;
        }

        final Optional<Duration> renderedMaxDuration = runContext.render(this.maxDuration).as(Duration.class);

        return renderedMaxDuration.isPresent() && ZonedDateTime.now().toEpochSecond() > start.plus(renderedMaxDuration.get()).toEpochSecond();
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
