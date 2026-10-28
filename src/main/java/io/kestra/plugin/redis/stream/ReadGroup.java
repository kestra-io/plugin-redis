package io.kestra.plugin.redis.stream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.lettuce.core.Consumer;
import io.lettuce.core.XReadArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

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
        immediately. `XREADGROUP BLOCK 0`, which waits forever, is deliberately not reachable. Killing the task closes the \
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
                    url: redis://:redis@localhost:6379/0
                    key: orders
                    group: billing
                    startId: "0"

                  - id: read
                    type: io.kestra.plugin.redis.stream.ReadGroup
                    url: redis://:redis@localhost:6379/0
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
                    url: redis://:redis@localhost:6379/0
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
                    url: redis://:redis@localhost:6379/0
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
                    url: redis://:redis@localhost:6379/0
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
public class ReadGroup extends AbstractRedisConnection implements RunnableTask<ReadGroup.Output> {
    /**
     * Added on top of {@code blockDuration} so Lettuce's own command timeout never fires before the server returns from
     * {@code BLOCK}; without it a block longer than the default client timeout would fail an otherwise healthy read.
     */
    private static final Duration COMMAND_TIMEOUT_MARGIN = Duration.ofSeconds(10);

    /**
     * Upper bound on how long {@link #kill()} waits for the in-flight read to unwind after the connection is closed.
     * Lettuce's {@code shutdown()} itself blocks for up to two seconds, so the margin is deliberate.
     */
    private static final Duration KILL_AWAIT_TIMEOUT = Duration.ofSeconds(5);

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
        description = "Rendered group name; the group must already exist, so create it with `GroupCreate` first. Reading from a missing group fails with `NOGROUP`."
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer name inside the group",
        description = "Rendered consumer name. Redis tracks pending entries per consumer, so entries delivered under this name stay owned by it until they are acknowledged. Use a stable name per worker role rather than a value that changes on every execution, otherwise unacknowledged entries are stranded under a name nothing reads again."
    )
    @NotNull
    private Property<String> consumer;

    @PluginProperty(group = "main")
    @Schema(
        title = "Stream ID to read from",
        description = "Defaults to `>`, which delivers entries never handed to any consumer of the group and adds them to this consumer's pending list. Use `0` to re-read this consumer's own already delivered but unacknowledged entries, oldest first, or any explicit stream ID to re-read its pending entries after that ID. Only `>` delivers new entries."
    )
    @Builder.Default
    @NotNull
    private Property<String> offset = Property.ofValue(">");

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum entries per read",
        description = "Defaults to 100 and maps to `COUNT`. Must be at least 1. Forced to 1 when `fetchType` is `FETCH_ONE`, so the task never makes Redis mark entries as delivered that the flow cannot see or acknowledge."
    )
    @Builder.Default
    @NotNull
    private Property<Integer> count = Property.ofValue(100);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How long to wait for entries",
        description = "Maps to `BLOCK`. Unset by default, which makes the read return immediately with whatever is available. When set it must be strictly positive; `BLOCK 0`, which waits forever, is not supported because the read has to stay bounded. The client command timeout is raised above this value for the call."
    )
    private Property<Duration> blockDuration;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Skip the pending list for this read",
        description = "Defaults to false. When true the read adds `NOACK`, so Redis never records the entries as pending and `Ack` has nothing to remove. This downgrades delivery to at-most-once: an entry lost by a failing flow is not redelivered. Leave it false whenever the work must not be lost."
    )
    @Builder.Default
    @NotNull
    private Property<Boolean> noAck = Property.ofValue(false);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How entries are returned",
        description = "Defaults to `FETCH`, which returns every entry of the batch in `messages`. `FETCH_ONE` returns a single entry in `message` and forces `count` to 1. `STORE` writes the batch as an ION file to Kestra internal storage and returns its `uri`. `NONE` returns no entry body; `count` and `ids` are still populated, so the batch can be acknowledged or counted without carrying the payload."
    )
    @Builder.Default
    @NotNull
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<AbstractRedisConnection.RedisFactory> factoryRef = new AtomicReference<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<CountDownLatch> waitForTermination = new AtomicReference<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<Logger> loggerRef = new AtomicReference<>();

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        loggerRef.set(logger);

        // Publish the latch before anything else, including the isActive check: a kill() landing between the check and
        // the publish would otherwise return without waiting for this read at all. The outer finally always counts it
        // down exactly once, on every return and throw path.
        CountDownLatch latch = new CountDownLatch(1);
        waitForTermination.set(latch);

        try {
            if (!isActive.get()) {
                throw new CancellationException("Read on a Redis stream consumer group was cancelled before it started.");
            }

            RedisFactory factory = this.redisFactory(runContext);
            factoryRef.set(factory);

            try {
                // Re-check right after publishing the connection: this closes the window where a kill() between opening
                // the connection and storing it would leave a live connection behind with nothing able to close it.
                if (!isActive.get()) {
                    throw new CancellationException("Read on a Redis stream consumer group was cancelled before it started.");
                }

                return this.read(runContext, factory);
            } catch (Exception e) {
                if (!isActive.get()) {
                    // kill() closed the connection underneath the blocked command. Nothing was delivered by a command
                    // that failed, and acknowledgement is a separate task, so no entry can have been acknowledged here.
                    logger.info("Read on a Redis stream consumer group was cancelled; no entry was acknowledged.");

                    throw new CancellationException("Read on a Redis stream consumer group was cancelled while waiting for entries.");
                }

                throw e;
            } finally {
                // Close on every path. The atomic hand-off in releaseFactory() guarantees exactly one of this block and
                // kill() ever closes the connection, so the normal path does not leak a Lettuce client either.
                this.releaseFactory();
            }
        } finally {
            latch.countDown();
        }
    }

    // Lettuce's xreadgroup takes StreamOffset<K> as a generic varargs parameter, which is an unavoidable
    // unchecked generic array creation at the call site below.
    @SuppressWarnings("unchecked")
    private Output read(RunContext runContext, RedisFactory factory) throws Exception {
        String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
        String renderedGroup = runContext.render(this.group).as(String.class).orElseThrow();
        String renderedConsumer = runContext.render(this.consumer).as(String.class).orElseThrow();
        String renderedOffset = runContext.render(this.offset).as(String.class).orElse(">");
        FetchType renderedFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        int renderedCount = runContext.render(this.count).as(Integer.class).orElse(100);
        if (renderedCount < 1) {
            throw new IllegalArgumentException("count must be at least 1 but was " + renderedCount);
        }
        if (renderedFetchType == FetchType.FETCH_ONE && renderedCount > 1) {
            // Delivering more entries than the output can carry would mark them as pending under this consumer with no
            // id reaching the flow, so they could never be acknowledged from it.
            runContext.logger().debug("fetchType is FETCH_ONE, reading a single entry instead of {}.", renderedCount);
            renderedCount = 1;
        }

        XReadArgs readArgs = new XReadArgs().count(renderedCount);

        Optional<Duration> renderedBlock = runContext.render(this.blockDuration).as(Duration.class);
        if (renderedBlock.isPresent()) {
            Duration block = renderedBlock.get();
            if (block.isZero() || block.isNegative()) {
                throw new IllegalArgumentException(
                    "blockDuration must be strictly positive but was " + block
                        + "; leave it unset for a read that returns immediately. Blocking forever is not supported because the read must stay bounded."
                );
            }

            readArgs = readArgs.block(block);
            factory.setTimeout(block.plus(COMMAND_TIMEOUT_MARGIN));
        }

        if (runContext.render(this.noAck).as(Boolean.class).orElse(false)) {
            readArgs = readArgs.noack(true);
        }

        List<io.lettuce.core.StreamMessage<String, String>> read = factory.getSyncCommands().xreadgroup(
            Consumer.from(renderedGroup, renderedConsumer),
            readArgs,
            XReadArgs.StreamOffset.from(renderedKey, renderedOffset)
        );

        // Redis answers a timed-out BLOCK with a nil reply, which Lettuce surfaces as null on some versions and as an
        // empty list on others; both mean the same empty batch here.
        List<Message> messages = (read == null ? List.<io.lettuce.core.StreamMessage<String, String>> of() : read)
            .stream()
            .map(
                message -> Message.builder()
                    .stream(message.getStream())
                    .id(message.getId())
                    // Redis field names and values are returned untouched, so a payload survives the round trip byte for byte.
                    .fields(message.getBody() == null ? Map.<String, String> of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(message.getBody())))
                    .build()
            )
            .toList();

        List<String> ids = messages.stream().map(Message::getId).toList();

        runContext.metric(Counter.of("read.entries.count", messages.size(), "stream", renderedKey, "group", renderedGroup));

        Output.OutputBuilder output = Output.builder()
            .stream(renderedKey)
            .group(renderedGroup)
            .consumer(renderedConsumer)
            .count(messages.size())
            .ids(ids);

        switch (renderedFetchType) {
            case FETCH -> output.messages(messages);
            case FETCH_ONE -> output.message(messages.isEmpty() ? null : messages.getFirst());
            case STORE -> output.uri(this.store(runContext, messages));
            case NONE -> {
                // ids and count are still returned so the batch stays acknowledgeable.
            }
        }

        return output.build();
    }

    private URI store(RunContext runContext, List<Message> messages) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var stream = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            FileSerde.writeAll(stream, Flux.fromIterable(messages)).block();
            stream.flush();
        }

        return runContext.storage().putFile(tempFile);
    }

    /**
     * Atomically hands off the live connection so exactly one of {@link #run(RunContext)}'s {@code finally} and
     * {@link #kill()} closes it, whichever gets there first; the other finds the reference already cleared.
     */
    private void releaseFactory() {
        RedisFactory factory = factoryRef.getAndSet(null);
        if (factory == null) {
            return;
        }

        try {
            factory.close();
        } catch (Exception e) {
            Logger logger = loggerRef.get();
            if (logger != null) {
                logger.warn("Failed to close the Redis connection of task id={}", this.id, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Closing the connection is what unblocks an in-flight {@code XREADGROUP ... BLOCK}: the blocked command fails
     * instead of holding the worker thread for the rest of its block duration.
     */
    @Override
    public void kill() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        this.releaseFactory();

        CountDownLatch latch = this.waitForTermination.get();
        if (latch == null) {
            return;
        }

        try {
            if (!latch.await(KILL_AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                Logger logger = loggerRef.get();
                if (logger != null) {
                    logger.warn("kill() of task id={} timed out after {} waiting for the in-flight read to stop; proceeding.", this.id, KILL_AWAIT_TIMEOUT);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
