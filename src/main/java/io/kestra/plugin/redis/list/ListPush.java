package io.kestra.plugin.redis.list;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
    title = "Adds a new element to the head of a list."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: redis://:redis@localhost:6379/0",
                "key: mykey",
                "from:",
                "   - value1",
                "   - value2"
            }
        )
    },
    aliases = "io.kestra.plugin.redis.ListPush"
)
public class ListPush extends AbstractRedisConnection implements RunnableTask<ListPush.Output> {
    @Schema(
        title = "The redis key for the list."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The list of values to push at the head of the list.",
        anyOf = {String.class, List.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @PluginProperty
    @NotNull
    private SerdeType serdeType = SerdeType.STRING;

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
                    count = resultFlowable
                        .reduce(Integer::sum)
                        .block();
                }
            } else if (from instanceof List<?> fromList) {
                Flux<Object> flowable = Flux.fromArray((fromList).toArray());
                Flux<Integer> resultFlowable = this.buildFlowable(flowable, runContext, factory);
                count = resultFlowable
                    .reduce(Integer::sum)
                    .block();
            }
            else {
                // should not occur as validation mandates String or List
                throw new IllegalVariableEvaluationException("Invalid 'from' property type :" + from.getClass());
            }

            runContext.metric(Counter.of("records", count));
            return Output.builder().count(count).build();
        }
    }

    private Flux<Integer> buildFlowable(Flux<Object> flowable, RunContext runContext, RedisFactory factory) throws Exception {
        return flowable
            .map(throwFunction(row -> {
                factory.listPush(
                    runContext.render(key),
                    Collections.singletonList(serdeType.serialize(row))
                );

                return 1;
            }));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "The number of values inserted."
        )
        private Integer count;
    }
}
