package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React to and consume key of type list from a Redis database creating one executions for each key."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "id: list-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: echo",
                "    type: io.kestra.core.tasks.log.Log",
                "    message: \"Received '{{ trigger.value }}'\" ",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.redis.RealtimeTriggerList",
                "    url: redis://localhost:6379/0",
                "    key: mytriggerkey",
            },
            full = true
        )
    }
)
public class RealtimeTriggerList extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<RealtimeTriggerList.Output>, ListPopBaseInterface, RedisConnectionInterface {
    private String url;

    private String key;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        ListPop task = ListPop.builder()
            .url(this.url)
            .key(this.key)
            .count(1)
            .serdeType(this.serdeType)
            .build();

        return Flux.from(task.stream(conditionContext.getRunContext()))
            .map((record) -> TriggerService.generateRealtimeExecution(this, context, Output.of(record)));
    }

    @Builder
    @Getter
    @AllArgsConstructor(staticName = "of")
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The value."
        )
        private Object value;
    }
}
