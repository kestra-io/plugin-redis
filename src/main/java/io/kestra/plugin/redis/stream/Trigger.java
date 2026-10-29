package io.kestra.plugin.redis.stream;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.redis.RedisConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start an execution for new entries of a Redis stream consumer group",
    description = """
        Periodically reads one batch of new entries with `XREADGROUP` as `consumer` inside `group`, using the same \
        contract as [ReadGroup](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.readgroup), \
        and starts one execution when the batch is not empty. The execution gets the stream name, the exact stream IDs \
        and the field-value maps of the entries as `trigger.ids`, `trigger.messages` or `trigger.uri`, depending on \
        `fetchType`. The group must already exist: create it once with `GroupCreate`.

        The trigger never acknowledges anything. Delivery is at-least-once: the entries of a batch are pending for \
        `consumer` as soon as they are read, and stay so until the flow runs \
        [Ack](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.ack) with \
        `{{ trigger.ids }}` after the business step. When the execution fails before that, the entries are not read \
        again by this trigger, because it only asks for new entries. Recover them with a flow that runs \
        [Claim](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.claim). Handlers must \
        be idempotent, because a failure between a business write and the acknowledgement replays the entry.

        Every evaluation is a single read that returns immediately, so a quiet stream costs one command per \
        `interval` and never holds a scheduler thread. Use a stable `consumer` name for the trigger and a different \
        one for any other reader of the same group. Killing the trigger closes the connection of a read in flight and \
        starts no execution."""
)
@Plugin(
    examples = {
        @Example(
            title = "Start an execution for each batch of new orders, process it and acknowledge it.",
            full = true,
            code = """
                id: redis_stream_listen
                namespace: company.team

                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "handling {{ trigger.count }} entries: {{ trigger.messages }}"

                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    ids: "{{ trigger.ids }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.stream.Trigger
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_trigger
                    count: 50
                    interval: PT30S
                """
        ),
        @Example(
            title = "Keep large batches out of the execution by storing them, and still acknowledge by ID.",
            full = true,
            code = """
                id: redis_stream_listen_store
                namespace: company.team

                tasks:
                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    ids: "{{ trigger.ids }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.stream.Trigger
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_trigger
                    fetchType: STORE
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ReadGroup.Output>, RedisConnectionInterface {
    @PluginProperty(group = "main", secret = true)
    private Property<String> url;

    @PluginProperty(group = "main")
    @Schema(
        title = "Redis stream key",
        description = "Rendered key passed to `XREADGROUP`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer group name",
        description = "Rendered group name; the group must already exist, so create it with `GroupCreate` first."
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer name inside the group",
        description = """
            Rendered consumer name. Entries read by the trigger are pending for this consumer until they are \\
            acknowledged. Use a stable name that no other reader of the group uses."""
    )
    @NotNull
    private Property<String> consumer;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum entries per evaluation",
        description = """
            Defaults to 100 and maps to `COUNT`. Must be between 1 and 1000. Forced to 1 when `fetchType` is \\
            `FETCH_ONE`, so no entry is marked as delivered without its ID reaching the execution."""
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) @Max(1000) Integer> count = Property.ofValue(100);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How entries are passed to the execution",
        description = """
            Defaults to `FETCH`, which puts every entry of the batch in `trigger.messages`. `FETCH_ONE` passes a \\
            single entry in `trigger.message`. `STORE` writes the batch as an ION file and passes its `trigger.uri`, \\
            which keeps large payloads out of the execution. `NONE` passes no entry body. `trigger.ids` and \\
            `trigger.count` are always set, so the batch can be acknowledged in every mode."""
    )
    @Builder.Default
    @NotNull
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<ReadGroup> current = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rCount = runContext.render(this.count).as(Integer.class).orElse(100);
        if (rCount < 1 || rCount > 1000) {
            // @Min and @Max on a Property type argument do not bound a literal written in flow YAML
            throw new IllegalArgumentException("`count` must be between 1 and 1000 but was " + rCount);
        }

        // no blockDuration: a blocking read would hold a scheduler thread for the whole wait
        var read = ReadGroup.builder()
            .id(this.id)
            .type(ReadGroup.class.getName())
            .url(this.url)
            .key(this.key)
            .group(this.group)
            .consumer(this.consumer)
            .count(Property.ofValue(rCount))
            .fetchType(this.fetchType)
            .build();
        current.set(read);

        ReadGroup.Output output;
        try {
            // checked after the read is published, so a kill() before that point is never missed
            if (!isActive.get()) {
                return Optional.empty();
            }

            output = read.run(runContext);
        } catch (CancellationException e) {
            // kill() closed the connection of a read in flight: an expected shutdown, not a trigger error
            return Optional.empty();
        } catch (Exception e) {
            if (!isActive.get()) {
                return Optional.empty();
            }

            throw e;
        } finally {
            current.set(null);
        }

        if (output.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    // closing the connection of the read in flight fails it without waiting; evaluate() does the full cleanup
    public void kill() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        var read = current.get();
        if (read != null) {
            read.kill();
        }
    }
}
