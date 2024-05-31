package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.RedisConnectionInterface;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Removes and returns an element from the head of a list periodically and create one execution per batch.",
    description = "If you would like to consume each message from a list in real-time and create one execution per message, you can use the [io.kestra.plugin.redis.list.RealtimeTrigger](https://kestra.io/plugins/plugin-redis/triggers/io.kestra.plugin.redis.list.realtimetrigger) instead."
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
                "    type: io.kestra.plugin.core.log.Log",
                "    message: \"{{ trigger.uri }} containing {{ trigger.count }} lines\" ",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.redis.list.Trigger",
                "    url: redis://localhost:6379/0",
                "    key: mytriggerkey",
                "    maxRecords: 2",
            },
            full = true
        )
    },
    aliases = "io.kestra.plugin.redis.TriggerList"
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ListPop.Output>, ListPopInterface, RedisConnectionInterface {
    private String url;

    private String key;

    @Builder.Default
    private Integer count = 100;

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @PluginProperty
    @NotNull
    private SerdeType serdeType = SerdeType.STRING;

    private Integer maxRecords;

    private Duration maxDuration;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        ListPop task = ListPop.builder()
            .url(runContext.render(this.url))
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

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

}
