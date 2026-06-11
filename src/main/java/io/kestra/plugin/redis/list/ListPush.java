package io.kestra.plugin.redis.list;

import java.io.BufferedInputStream;
import java.net.URI;
import java.util.List;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push values to a Redis list",
    description = "LPUSH rendered values to the head of the list; accepts literal lists or a Kestra storage URI, serializing with the selected serde (STRING by default)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_list_push
                namespace: company.team

                tasks:
                  - id: list_push
                    type: io.kestra.plugin.redis.list.ListPush
                    url: redis://:redis@localhost:6379/0
                    key: mykey
                    from:
                      - value1
                      - value2
                """
        )
    },
    metrics = {
        @Metric(
            name = "inserted.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records pushed to a Redis list."
        )
    },
    aliases = "io.kestra.plugin.redis.ListPush"
)
public class ListPush extends AbstractRedisConnection implements RunnableTask<ListPush.Output> {
    private static final int DEFAULT_BATCH_SIZE = 500;

    @PluginProperty(group = "main")
    @Schema(
        title = "Redis list key",
        description = "Rendered before pushing."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(dynamic = true, group = "main")
    @Schema(
        title = "Values to push",
        description = "String or list; a string may be parsed as JSON array or treated as a storage URI.",
        anyOf = { String.class, List.class }
    )
    @NotNull
    private Object from;

    @PluginProperty(group = "main")
    @Schema(
        title = "Serialization format",
        description = "Defaults to STRING; controls how values are encoded before LPUSH."
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @PluginProperty(group = "execution")
    @Schema(
        title = "Batch size",
        description = "Number of values sent per variadic LPUSH command. Defaults to 500. Lower it for large values, raise it to cut round-trips further on high-latency links."
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) Integer> batchSize = Property.ofValue(DEFAULT_BATCH_SIZE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            Integer count;

            Object from = null;
            if (this.from instanceof String fromString) {
                String renderedFrom = runContext.render(fromString);
                try {
                    from = JacksonMapper.ofJson().readValue(renderedFrom, List.class);
                } catch (Exception e) {
                    from = renderedFrom;
                }
            } else {
                from = this.from;
            }

            if (from instanceof String fromUrl) {
                URI fromURI = new URI(runContext.render(fromUrl));
                try (var inputStream = new BufferedInputStream(runContext.storage().getFile(fromURI), FileSerde.BUFFER_SIZE)) {
                    Flux<Object> flowable = FileSerde.readAll(inputStream);
                    Flux<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                    count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
                }
            } else if (from instanceof List<?> fromList) {
                Flux<Object> flowable = Flux.create(objectFluxSink ->
                {
                    for (Object o : fromList) {
                        try {
                            objectFluxSink.next(runContext.render((String) o));
                        } catch (Exception e) {
                            objectFluxSink.error(e);
                        }
                    }
                    objectFluxSink.complete();
                });
                Flux<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
            } else {
                // should not occur as validation mandates String or List
                throw new IllegalVariableEvaluationException("Invalid 'from' property type :" + from.getClass());
            }

            runContext.metric(Counter.of("inserted.records.count", count));
            return Output.builder().count(count).build();
        }
    }

    private Flux<Integer> buildFlowable(Flux<Object> flowable, RunContext runContext, RedisFactory factory) throws Exception {
        String rKey = runContext.render(key).as(String.class).orElseThrow();
        SerdeType rSerde = runContext.render(serdeType).as(SerdeType.class).orElse(SerdeType.STRING);
        int rBatchSize = runContext.render(batchSize).as(Integer.class).orElse(DEFAULT_BATCH_SIZE);

        return flowable
            .map(throwFunction(rSerde::serialize))
            // LPUSH is variadic, so one call per batch preserves the row order while cutting round-trips.
            .buffer(rBatchSize)
            .map(values ->
            {
                factory.getSyncCommands().lpush(rKey, values.toArray(new String[0]));

                return values.size();
            });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "Number of values inserted."
        )
        private Integer count;
    }
}
