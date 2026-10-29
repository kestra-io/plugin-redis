package io.kestra.plugin.redis.stream;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;

import io.lettuce.core.Consumer;
import io.lettuce.core.XReadArgs;
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
    title = "Read entries from a Redis stream consumer group",
    description = """
        Runs one bounded `XREADGROUP` for the given consumer and returns each entry with its stream name, its exact \
        stream ID and its field-value map. Field names and values are returned exactly as Redis stores them, with no \
        serialization format applied, so an ID can be fed straight back into \
        [Ack](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.ack).

        The read is bounded on both axes. `count` caps how many entries a single call delivers, and `blockDuration` caps \
        how long the call waits when the group has nothing to deliver; leaving `blockDuration` unset makes the call return \
        immediately, and it cannot exceed one hour. `XREADGROUP BLOCK 0`, which waits forever, is deliberately not reachable. Killing the task closes the \
        connection, so a waiting read stops instead of holding a worker thread.

        Delivery is at-least-once. Every entry this task returns is added to the group's Pending Entries List for this \
        consumer and stays there until `Ack` removes it, so a flow that fails after the read leaves the entry pending and \
        a later run can pick it up again. The consumer name is therefore part of the contract: entries delivered to one \
        consumer name remain owned by that name. Use a stable name per worker role, keep processing idempotent because a \
        crash between a business write and the acknowledgement replays the entry, and set `offset` to `0` to re-read this \
        consumer's own unacknowledged entries instead of new ones."""
)
@Plugin(
    examples = {
        @Example(
            title = "Read, process and acknowledge entries from a stream consumer group.",
            full = true,
            code = """
                id: redis_stream_consume
                namespace: company.team

                tasks:
                  - id: create_group
                    type: io.kestra.plugin.redis.stream.GroupCreate
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    startId: "0"

                  - id: read
                    type: io.kestra.plugin.redis.stream.ReadGroup
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_worker_1
                    count: 10
                    blockDuration: PT5S

                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "handling {{ outputs.read.count }} entries: {{ outputs.read.messages }}"

                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    ids: "{{ outputs.read.ids }}"
                    runIf: "{{ outputs.read.count > 0 }}"
                """
        ),
        @Example(
            title = "Recover this consumer's own entries that were read but never acknowledged.",
            full = true,
            code = """
                id: redis_stream_recover_own
                namespace: company.team

                tasks:
                  - id: read_pending
                    type: io.kestra.plugin.redis.stream.ReadGroup
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_worker_1
                    offset: "0"
                    count: 100
                """
        ),
        @Example(
            title = "Store a batch in Kestra internal storage instead of returning it inline.",
            full = true,
            code = """
                id: redis_stream_store
                namespace: company.team

                tasks:
                  - id: read
                    type: io.kestra.plugin.redis.stream.ReadGroup
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_worker_1
                    fetchType: STORE
                    count: 500
                """
        )
    },
    metrics = {
        @Metric(
            name = "read.entries.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of entries delivered by XREADGROUP."
        )
    }
)
public class ReadGroup extends AbstractStreamTask implements RunnableTask<ReadGroup.Output> {
    // BLOCK can outlast Lettuce's default command timeout, so the client timeout is raised above it.
    private static final Duration COMMAND_TIMEOUT_MARGIN = Duration.ofSeconds(10);

    private static final Duration MAX_BLOCK_DURATION = Duration.ofHours(1);

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
        description = """
            Rendered group name; the group must already exist, so create it with `GroupCreate` first. \
            Reading from a missing group fails with `NOGROUP`."""
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer name inside the group",
        description = """
            Rendered consumer name. Redis tracks pending entries per consumer, so entries delivered under this name \
            stay owned by it until they are acknowledged. Use a stable name per worker role rather than a value that \
            changes on every execution, otherwise unacknowledged entries are stranded under a name nothing reads again."""
    )
    @NotNull
    private Property<String> consumer;

    @PluginProperty(group = "main")
    @Schema(
        title = "Stream ID to read from",
        description = """
            Defaults to `>`, which delivers entries never handed to any consumer of the group and adds them to this \
            consumer's pending list. Use `0` to re-read this consumer's own already delivered but unacknowledged \
            entries, oldest first, or any explicit stream ID to re-read its pending entries after that ID. Only `>` \
            delivers new entries."""
    )
    @Builder.Default
    @NotNull
    private Property<String> offset = Property.ofValue(">");

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum entries per read",
        description = """
            Defaults to 100 and maps to `COUNT`. Must be between 1 and 10000. Forced to 1 when `fetchType` is \
            `FETCH_ONE`, so the task never makes Redis mark entries as delivered that the flow cannot see or \
            acknowledge."""
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) @Max(10000) Integer> count = Property.ofValue(100);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How long to wait for entries",
        description = """
            Maps to `BLOCK`. Unset by default, which makes the read return immediately with whatever is available. \
            When set it must be strictly positive and at most 1 hour (`PT1H`); `BLOCK 0`, which waits forever, is not \
            supported because the read has to stay bounded. The client command timeout is raised above this value for \
            the call. The task-level `timeout` can cap the wait further."""
    )
    private Property<Duration> blockDuration;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Skip the pending list for this read",
        description = """
            Defaults to false. When true the read adds `NOACK`, so Redis never records the entries as pending and \
            `Ack` has nothing to remove. This downgrades delivery to at-most-once: an entry lost by a failing flow is \
            not redelivered. Leave it false whenever the work must not be lost."""
    )
    @Builder.Default
    @NotNull
    private Property<Boolean> noAck = Property.ofValue(false);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How entries are returned",
        description = """
            Defaults to `FETCH`, which returns every entry of the batch in `messages`. `FETCH_ONE` returns a single \
            entry in `message` and forces `count` to 1. `STORE` writes the batch as an ION file to Kestra internal \
            storage and returns its `uri`. `NONE` returns no entry body; `count` and `ids` are still populated, so the \
            batch can be acknowledged or counted without carrying the payload."""
    )
    @Builder.Default
    @NotNull
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        return this.cancellable(runContext, "Read on a Redis stream consumer group", factory -> this.read(runContext, factory));
    }

    // Lettuce's xreadgroup takes a generic varargs StreamOffset, an unavoidable unchecked array creation.
    @SuppressWarnings("unchecked")
    private Output read(RunContext runContext, RedisFactory factory) throws Exception {
        var rKey = runContext.render(this.key).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`key` rendered to an empty value; set the Redis stream key to read"));
        var rGroup = runContext.render(this.group).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`group` rendered to an empty value; set a consumer group name"));
        var rConsumer = runContext.render(this.consumer).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`consumer` rendered to an empty value; set a consumer name"));
        var rOffset = runContext.render(this.offset).as(String.class).orElse(">");
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        var rCount = requireBetween("count", runContext.render(this.count).as(Integer.class).orElse(100), 1, 10000);
        if (rFetchType == FetchType.FETCH_ONE && rCount > 1) {
            // more entries than the output carries would sit pending with no ID reaching the flow
            runContext.logger().debug("fetchType is FETCH_ONE, reading a single entry instead of {}.", rCount);
            rCount = 1;
        }

        var readArgs = new XReadArgs().count(rCount);

        var rBlock = runContext.render(this.blockDuration).as(Duration.class);
        if (rBlock.isPresent()) {
            var block = rBlock.get();
            if (block.isZero() || block.isNegative()) {
                throw new IllegalArgumentException(
                    "blockDuration must be strictly positive but was " + block
                        + "; leave it unset for a read that returns immediately. Blocking forever is not supported because the read must stay bounded."
                );
            }
            if (block.compareTo(MAX_BLOCK_DURATION) > 0) {
                throw new IllegalArgumentException(
                    "blockDuration must be at most " + MAX_BLOCK_DURATION + " but was " + block
                        + "; use a shorter block and read again, or cap the task with the task-level `timeout`."
                );
            }

            readArgs = readArgs.block(block);
            factory.setTimeout(block.plus(COMMAND_TIMEOUT_MARGIN));
        }

        if (runContext.render(this.noAck).as(Boolean.class).orElse(false)) {
            readArgs = readArgs.noack(true);
        }

        var read = factory.getSyncCommands().xreadgroup(
            Consumer.from(rGroup, rConsumer),
            readArgs,
            XReadArgs.StreamOffset.from(rKey, rOffset)
        );

        var messages = toMessages(read);

        var ids = messages.stream().map(Message::getId).toList();

        runContext.metric(Counter.of("read.entries.count", messages.size(), "stream", rKey, "group", rGroup));

        var output = Output.builder()
            .stream(rKey)
            .group(rGroup)
            .consumer(rConsumer)
            .count(messages.size())
            .ids(ids);

        switch (rFetchType) {
            case FETCH -> output.messages(messages);
            case FETCH_ONE -> output.message(messages.isEmpty() ? null : messages.getFirst());
            case STORE -> output.uri(store(runContext, messages));
            case NONE -> {
                // ids and count are still returned so the batch stays acknowledgeable
            }
        }

        return output.build();
    }

    @Builder
    @Getter
    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Message {
        @Schema(
            title = "Stream the entry was read from"
        )
        private String stream;

        @Schema(
            title = "Exact stream ID of the entry",
            description = "Pass it to `Ack` to remove the entry from the group's pending list."
        )
        private String id;

        @Schema(
            title = "Field-value map of the entry",
            description = "Returned exactly as stored in Redis, with no serialization format applied."
        )
        private Map<String, String> fields;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stream that was read"
        )
        private String stream;

        @Schema(
            title = "Consumer group that was read"
        )
        private String group;

        @Schema(
            title = "Consumer name the entries are now pending for"
        )
        private String consumer;

        @Schema(
            title = "Number of entries delivered by this read"
        )
        private Integer count;

        @Schema(
            title = "Exact stream IDs delivered by this read",
            description = "In delivery order, ready to be passed to `Ack`. Populated for every `fetchType`."
        )
        private List<String> ids;

        @Schema(
            title = "Entries delivered by this read",
            description = "Only set when `fetchType` is `FETCH`."
        )
        private List<Message> messages;

        @Schema(
            title = "Single entry delivered by this read",
            description = "Only set when `fetchType` is `FETCH_ONE`; null when the read returned nothing."
        )
        private Message message;

        @Schema(
            title = "URI of the ION file holding the entries",
            description = "Only set when `fetchType` is `STORE`."
        )
        private URI uri;
    }
}
