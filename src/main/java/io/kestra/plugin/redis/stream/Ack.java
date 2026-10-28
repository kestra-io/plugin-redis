package io.kestra.plugin.redis.stream;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Acknowledge entries read from a Redis stream consumer group",
    description = """
        Runs `XACK` for the given stream IDs, which removes them from the group's Pending Entries List and ends the \
        group's responsibility for them.

        Acknowledging is a separate step on purpose. Put this task after the step that actually finished the work, so a \
        failure anywhere before it leaves the entries pending and available for a later run instead of silently dropping \
        them. Feed it the `ids` output of \
        [ReadGroup](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.readgroup), which \
        carries the exact IDs that were delivered.

        `XACK` is idempotent and counts only the entries it actually removed, so an ID that was already acknowledged, \
        never pending for this group, or trimmed out of the stream is simply not counted. Running the same \
        acknowledgement twice is therefore harmless, but it also means a short count is not an error by itself: compare \
        `count` with `size`, or set `failedOnMissing` to fail when they differ."""
)
@Plugin(
    examples = {
        @Example(
            title = "Acknowledge the entries of a read once the business step succeeded.",
            full = true,
            code = """
                id: redis_stream_ack
                namespace: company.team

                tasks:
                  - id: read
                    type: io.kestra.plugin.redis.stream.ReadGroup
                    url: redis://:redis@localhost:6379/0
                    key: orders
                    group: billing
                    consumer: billing_worker_1
                    count: 10

                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "handling {{ outputs.read.count }} entries"

                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: redis://:redis@localhost:6379/0
                    key: orders
                    group: billing
                    ids: "{{ outputs.read.ids }}"
                    runIf: "{{ outputs.read.count > 0 }}"
                """
        ),
        @Example(
            title = "Acknowledge a single known entry and fail if it was not pending.",
            full = true,
            code = """
                id: redis_stream_ack_strict
                namespace: company.team

                tasks:
                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: redis://:redis@localhost:6379/0
                    key: orders
                    group: billing
                    ids:
                      - "1700000000000-0"
                    failedOnMissing: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "acknowledged.entries.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of entries removed from the pending list by XACK."
        )
    }
)
public class Ack extends AbstractRedisConnection implements RunnableTask<Ack.Output> {
    @PluginProperty(group = "main")
    @Schema(
        title = "Redis stream key",
        description = "Rendered key passed to `XACK`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer group name",
        description = "Rendered group name whose pending list the entries are removed from. `XACK` on a group that does not exist acknowledges nothing rather than failing."
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Stream IDs to acknowledge",
        description = "Rendered list of exact stream IDs, typically the `ids` output of `ReadGroup`. Duplicates are removed before the call. An empty list is a no-op and does not call Redis."
    )
    @NotNull
    private Property<List<String>> ids;

    @PluginProperty(group = "reliability")
    @Schema(
        title = "Fail when some IDs were not pending",
        description = "Defaults to false, which reports the acknowledged `count` without failing; this keeps a retried acknowledgement harmless. When true, the task throws if fewer entries were removed than requested, which catches IDs that were already acknowledged, never pending for this group, or trimmed out of the stream."
    )
    @Builder.Default
    @NotNull
    private Property<Boolean> failedOnMissing = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
        final String renderedGroup = runContext.render(this.group).as(String.class).orElseThrow();
        final List<String> renderedIds = runContext.render(this.ids).asList(String.class).stream()
            .distinct()
            .toList();

        if (renderedIds.isEmpty()) {
            runContext.logger().info("No stream ID to acknowledge on stream '{}' for group '{}'.", renderedKey, renderedGroup);

            return Output.builder().count(0L).size(0).build();
        }

        try (RedisFactory factory = this.redisFactory(runContext)) {
            long count = factory.getSyncCommands().xack(
                renderedKey,
                renderedGroup,
                renderedIds.toArray(new String[0])
            );

            if (count < renderedIds.size() && runContext.render(this.failedOnMissing).as(Boolean.class).orElse(false)) {
                throw new IllegalStateException(
                    "Only " + count + " of " + renderedIds.size() + " entries acknowledged on stream '" + renderedKey
                        + "' for group '" + renderedGroup + "'. The remaining IDs were not pending for this group: they "
                        + "were already acknowledged, never delivered to this group, or trimmed out of the stream. Set "
                        + "`failedOnMissing` to false to accept that."
                );
            }

            runContext.metric(
                Counter.of(
                    "acknowledged.entries.count",
                    count,
                    "stream", renderedKey,
                    "group", renderedGroup
                )
            );

            runContext.logger().info(
                "Acknowledged {} of {} entries on stream '{}' for group '{}'.",
                count,
                renderedIds.size(),
                renderedKey,
                renderedGroup
            );

            return Output.builder()
                .count(count)
                .size(renderedIds.size())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of entries actually removed from the pending list",
            description = "As reported by `XACK`. Lower than `size` when some IDs were not pending for the group."
        )
        private Long count;

        @Schema(
            title = "Number of distinct IDs that were requested"
        )
        private Integer size;
    }
}
