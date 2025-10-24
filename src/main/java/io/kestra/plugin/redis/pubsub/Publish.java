package io.kestra.plugin.redis.pubsub;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish one or multiple values to a Redis channel."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.redis.pubsub.Publish
                    url: redis://:redis@localhost:6379/0
                    channel: mych
                    from:
                      - value1
                      - value2
                """
        )
    },
    metrics = {
        @Metric(
            name = "published.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records published to a Redis channel."
        )
    },
    aliases = "io.kestra.plugin.redis.Publish"
)
public class Publish extends AbstractRedisConnection implements RunnableTask<Publish.Output>, io.kestra.core.models.property.Data.From {
    @Schema(
        title = "The Redis channel to publish"
    )
    @NotNull
    private Property<String> channel;

    @Schema(
        title = "The list of values to publish to the channel",
        anyOf = {String.class, List.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            // Integer count;
            Integer count = io.kestra.core.models.property.Data.from(from)
            .readAs(runContext, String.class, obj -> obj.toString())
            .map(throwFunction(row -> {
                String channelRendered = runContext.render(this.channel).as(String.class).orElseThrow();
                
                List<String> values = Collections.singletonList(runContext.render(serdeType)
                    .as(SerdeType.class)
                    .orElse(SerdeType.STRING)
                    .serialize(row));
                // Did not understand why we are adding the result but at the end we are returning 1 anyway
                long result = 0;
                for (String value : values) {
                    result += factory.getSyncCommands().publish(channelRendered, value);
                }

                return 1;
            })).reduce(Integer::sum)
            .blockOptional().orElse(0);
            runContext.metric(Counter.of("published.records.count", count));
            return Output.builder().count(count).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Count",
            description = "The number of values published"
        )
        private Integer count;
    }
}
