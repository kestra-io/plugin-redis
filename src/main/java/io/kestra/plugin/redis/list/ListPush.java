package io.kestra.plugin.redis.list;

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
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.List;

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
    @Schema(
        title = "Redis list key",
        description = "Rendered before pushing."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Values to push",
        description = "String or list; a string may be parsed as JSON array or treated as a storage URI.",
        anyOf = {String.class, List.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "Serialization format",
        description = "Defaults to STRING; controls how values are encoded before LPUSH."
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

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
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(fromURI)))) {
                    Flux<Object> flowable = FileSerde.readAll(inputStream);
                    Flux<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                    count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
                }
            } else if (from instanceof List<?> fromList) {
                Flux<Object> flowable = Flux.create(objectFluxSink -> {
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
            }
            else {
                // should not occur as validation mandates String or List
                throw new IllegalVariableEvaluationException("Invalid 'from' property type :" + from.getClass());
            }

            runContext.metric(Counter.of("inserted.records.count", count));
            return Output.builder().count(count).build();
        }
    }

    private Flux<Integer> buildFlowable(Flux<Object> flowable, RunContext runContext, RedisFactory factory) throws Exception {
        return flowable
            .map(throwFunction(row -> {
                factory.getSyncCommands().lpush(
                    runContext.render(key).as(String.class).orElseThrow(),
                    Collections.singletonList(runContext.render(serdeType).as(SerdeType.class).orElse(SerdeType.STRING).serialize(row)).toArray(new String[0])
                );

                return 1;
            }));
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
