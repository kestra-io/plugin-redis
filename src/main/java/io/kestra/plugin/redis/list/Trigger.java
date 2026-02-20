package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
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
    title = "Batch trigger from a Redis list",
    description = "Periodically pops list items in batches using `LPOP` (default batch size 100) until `maxRecords` or `maxDuration` is reached, then starts one Execution. Use [RealtimeTrigger](https://kestra.io/plugins/plugin-redis/triggers/io.kestra.plugin.redis.list.realtimetrigger) instead for per-message executions."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: list_listen
                namespace: company.team

                tasks:
                  - id: echo
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uri }} containing {{ trigger.count }} lines"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.list.Trigger
                    url: redis://localhost:6379/0
                    key: mytriggerkey
                    maxRecords: 2
                """
        )
    },
    aliases = "io.kestra.plugin.redis.TriggerList"
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ListPop.Output>, ListPopInterface, RedisConnectionInterface {
    private Property<String> url;

    private Property<String> key;

    @Schema(
        title = "Batch size per evaluation",
        description = "Defaults to 100."
    )
    @Builder.Default
    private Property<Integer> count = Property.ofValue(100);

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        ListPop task = ListPop.builder()
            .url(this.url)
            .key(this.key)
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
