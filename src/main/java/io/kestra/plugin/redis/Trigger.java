package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.services.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Wait for key of type list in Redis database"
)
@Plugin(
        examples = {
                @Example(
                        code = {
                                "id: watch",
                                "type: io.kestra.plugin.redis.Trigger",
                                "uri: redis://localhost:6379/0",
                                "key: mytriggerkey",
                                "count: 2"
                        }
                )
        }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ListPop.Output>, RedisConnectionInterface {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
            title = "Redis URI",
            description = "The URI to connect to the Redis Database."
    )
    private String uri;

    @Schema(
            title = "Redis key",
            description = "The redis key you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
            title = "Number of elements that should be pop at once"
    )
    @Builder.Default
    private Integer count = 1;

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

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        ListPop task = ListPop.builder()
                .uri(runContext.render(this.uri))
                .key(runContext.render(this.key))
                .count(this.count)
                .maxRecords(this.maxRecords)
                .maxDuration(this.maxDuration)
                .serdeType(this.serdeType)
                .build();
        ListPop.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' data.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
                this,
                run
        );

        Execution execution = Execution.builder()
                .id(executionId)
                .namespace(context.getNamespace())
                .flowId(context.getFlowId())
                .flowRevision(context.getFlowRevision())
                .state(new State())
                .trigger(executionTrigger)
                .build();

        return Optional.of(execution);
    }

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

}
