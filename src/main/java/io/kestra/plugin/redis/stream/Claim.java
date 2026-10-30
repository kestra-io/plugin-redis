package io.kestra.plugin.redis.stream;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.regex.Pattern;

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
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
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
    title = "Reclaim abandoned deliveries of a Redis stream consumer group",
    description = """
        Runs `XAUTOCLAIM` to take over entries that were delivered to a consumer and never acknowledged, for example \
        because the worker or the execution that read them failed. The entries become pending for `consumer`, and are \
        returned with their stream name, exact stream ID and field-value map exactly as \
        [ReadGroup](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.readgroup) \
        returns them, so the same flow can process and then \
        [Ack](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.ack) them.

        Only entries idle for at least `minIdleTime` are taken. Choose a value longer than the slowest legitimate \
        handler: an entry that is still being processed looks idle to Redis, and taking it makes two workers handle \
        it. The task cannot tell the two cases apart, which is why `minIdleTime` is required and has no default.

        The work is bounded three ways. `count` is the page size of one `XAUTOCLAIM`, `maxRecords` caps how many \
        entries one run takes, and `maxPages` caps how many pages one run scans. Redis may answer a page with no entry \
        and a cursor other than `0-0`, because it inspects a limited number of pending entries per call, so the task \
        keeps following the cursor until it reaches `0-0` or a budget is spent. `cursor` and `completed` in the output \
        tell the two apart: pass `cursor` as `startId` of a later run to continue the scan.

        Taking an entry increments its delivery counter and resets its idle time. Entries that were deleted or \
        trimmed from the stream while pending cannot be delivered; Redis 7.0 or later removes them from the pending \
        list and they are listed in `deletedIds` instead of being returned as empty entries. Needs Redis 6.2 or later."""
)
@Plugin(
    examples = {
        @Example(
            title = "Take over entries that no worker acknowledged for 10 minutes, process them and acknowledge them.",
            full = true,
            code = """
                id: redis_stream_recover
                namespace: company.team

                tasks:
                  - id: claim
                    type: io.kestra.plugin.redis.stream.Claim
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_recovery
                    minIdleTime: PT10M
                    maxRecords: 100

                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "recovering {{ outputs.claim.count }} entries: {{ outputs.claim.messages }}"
                    runIf: "{{ outputs.claim.count > 0 }}"

                  - id: acknowledge
                    type: io.kestra.plugin.redis.stream.Ack
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    ids: "{{ outputs.claim.ids }}"
                    runIf: "{{ outputs.claim.count > 0 }}"
                """
        ),
        @Example(
            title = "Scan a large pending list in several runs by keeping the cursor of the previous run.",
            full = true,
            code = """
                id: redis_stream_recover_cursor
                namespace: company.team

                inputs:
                  - id: start
                    type: STRING
                    defaults: "0-0"

                tasks:
                  - id: claim
                    type: io.kestra.plugin.redis.stream.Claim
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    consumer: billing_recovery
                    minIdleTime: PT10M
                    startId: "{{ inputs.start }}"
                    count: 50
                    maxPages: 5
                    fetchType: NONE

                  - id: report
                    type: io.kestra.plugin.core.log.Log
                    message: "claimed {{ outputs.claim.count }}, scan completed: {{ outputs.claim.completed }}, resume from {{ outputs.claim.cursor }}"
                """
        )
    },
    metrics = {
        @Metric(
            name = "claim.entries.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of entries taken over by XAUTOCLAIM."
        )
    }
)
public class Claim extends AbstractStreamTask implements RunnableTask<Claim.Output> {
    private static final Pattern STREAM_ID = Pattern.compile("^\\d+(-\\d+)?$");

    private static final String SCAN_COMPLETE = "0-0";

    private static final int MAX_DELETED_IDS = 1000;

    @PluginProperty(group = "main")
    @Schema(
        title = "Redis stream key",
        description = "Rendered key passed to `XAUTOCLAIM`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer group name",
        description = "Rendered group name whose pending entries are scanned. The group must already exist."
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer that takes the entries over",
        description = """
            Rendered consumer name. Claimed entries become pending for this consumer, and stay so until they are \
            acknowledged. Use a stable name for the recovery role, not the name of the consumer that lost them."""
    )
    @NotNull
    private Property<String> consumer;

    @PluginProperty(group = "main")
    @Schema(
        title = "Minimum idle time before an entry can be taken",
        description = """
            Only pending entries not delivered or claimed for at least this long are taken. Must be strictly positive \
            and is required: choose a value longer than the slowest handler that reads from this group, otherwise an \
            entry that is still being processed is processed a second time. Truncated to whole milliseconds."""
    )
    @NotNull
    private Property<Duration> minIdleTime;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Stream ID to start the scan from",
        description = """
            Defaults to `0-0`, the start of the pending list. Pass the `cursor` output of a previous run to continue \
            its scan. Must be `0-0` or a stream ID such as `1700000000000-0`."""
    )
    @Builder.Default
    @NotNull
    private Property<String> startId = Property.ofValue(SCAN_COMPLETE);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Page size of one scan step",
        description = """
            Defaults to 100 and maps to `COUNT`. Must be between 1 and 10000. Redis inspects a limited number of \
            pending entries per call, so a page can hold fewer entries than this, or none. Lowered to the remaining \
            `maxRecords` budget so a run never takes more than it can return."""
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) @Max(10000) Integer> count = Property.ofValue(100);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum entries taken by one run",
        description = """
            Defaults to 100. Must be between 1 and 10000. The run stops as soon as this many entries were taken. \
            Forced to 1 when `fetchType` is `FETCH_ONE`, so no entry is taken that the flow cannot see."""
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) @Max(10000) Integer> maxRecords = Property.ofValue(100);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum scan steps of one run",
        description = """
            Defaults to 10. Must be between 1 and 1000. Each step is one `XAUTOCLAIM`. This bounds the work when \
            Redis keeps answering with empty pages and a non-final cursor. When it is spent before the cursor \
            reached `0-0`, `completed` is false and `cursor` is where the next run should continue."""
    )
    @Builder.Default
    @NotNull
    private Property<@Min(1) @Max(1000) Integer> maxPages = Property.ofValue(10);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "How entries are returned",
        description = """
            Defaults to `FETCH`, which returns every taken entry in `messages`. `FETCH_ONE` returns a single entry in \
            `message` and forces `maxRecords` and `count` to 1. `STORE` writes the entries as an ION file to Kestra \
            internal storage and returns its `uri`. `NONE` returns no entry body; `count`, `ids` and the cursor are \
            still populated."""
    )
    @Builder.Default
    @NotNull
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        return this.cancellable(runContext, "Recovery on a Redis stream consumer group", factory -> this.claim(runContext, factory));
    }

    private Output claim(RunContext runContext, RedisFactory factory) throws Exception {
        var rKey = runContext.render(this.key).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`key` rendered to an empty value; set the Redis stream key to recover"));
        var rGroup = runContext.render(this.group).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`group` rendered to an empty value; set a consumer group name"));
        var rConsumer = runContext.render(this.consumer).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`consumer` rendered to an empty value; set the consumer that takes the entries over"));
        var rMinIdle = runContext.render(this.minIdleTime).as(Duration.class)
            .orElseThrow(() -> new IllegalArgumentException("`minIdleTime` is required: set how long an entry must be idle before it can be taken, for example PT10M"));
        if (rMinIdle.toMillis() < 1) {
            throw new IllegalArgumentException(
                "minIdleTime must be at least 1 millisecond but was " + rMinIdle
                    + "; a zero threshold would take entries that another worker is processing right now."
            );
        }

        var rStartId = runContext.render(this.startId).as(String.class).orElse(SCAN_COMPLETE);
        if (!STREAM_ID.matcher(rStartId).matches()) {
            throw new IllegalArgumentException("startId must be `0-0` or a stream ID such as 1700000000000-0 but was `" + rStartId + "`");
        }

        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        var rCount = requireBetween("count", runContext.render(this.count).as(Integer.class).orElse(100), 1, 10000);
        var rMaxRecords = requireBetween("maxRecords", runContext.render(this.maxRecords).as(Integer.class).orElse(100), 1, 10000);
        var rMaxPages = requireBetween("maxPages", runContext.render(this.maxPages).as(Integer.class).orElse(10), 1, 1000);
        if (rFetchType == FetchType.FETCH_ONE) {
            // more entries than the output carries would sit pending with no ID reaching the flow
            runContext.logger().debug("fetchType is FETCH_ONE, taking a single entry instead of {}.", rMaxRecords);
            rMaxRecords = 1;
            rCount = 1;
        }

        var claimed = new ArrayList<StreamMessage<String, String>>();
        var deleted = new ArrayList<String>();
        var deletedTotal = 0L;
        var cursor = rStartId;
        var pages = 0;
        var completed = false;

        while (true) {
            if (isCancelled()) {
                throw new CancellationException("Recovery on a Redis stream consumer group was cancelled.");
            }

            // never ask for more than the run is allowed to keep, and never for zero: the loop ends once the budget is spent
            var pageSize = Math.max(1, Math.min(rCount, rMaxRecords - claimed.size()));
            var page = factory.getSyncCommands().xautoclaim(
                rKey,
                new XAutoClaimArgs<String>()
                    .consumer(Consumer.from(rGroup, rConsumer))
                    .minIdleTime(rMinIdle)
                    .startId(cursor)
                    .count(pageSize)
            );
            pages++;

            if (page.getMessages() != null) {
                claimed.addAll(page.getMessages());
            }
            if (page.getDeletedIds() != null) {
                deletedTotal += page.getDeletedIds().size();
                page.getDeletedIds().stream().limit(Math.max(MAX_DELETED_IDS - deleted.size(), 0)).forEach(deleted::add);
            }
            cursor = page.getId();

            if (SCAN_COMPLETE.equals(cursor)) {
                completed = true;
                break;
            }
            if (claimed.size() >= rMaxRecords || pages >= rMaxPages) {
                break;
            }
        }

        var messages = toMessages(claimed);
        var ids = messages.stream().map(ReadGroup.Message::getId).toList();

        runContext.metric(Counter.of("claim.entries.count", messages.size(), "stream", rKey, "group", rGroup));
        runContext.logger().info(
            "Took over {} entries of group '{}' on stream '{}' in {} scan steps, {} trimmed from the stream, scan {}.",
            messages.size(), rGroup, rKey, pages, deletedTotal, completed ? "completed" : "stopped at " + cursor
        );

        var output = Output.builder()
            .stream(rKey)
            .group(rGroup)
            .consumer(rConsumer)
            .count(messages.size())
            .ids(ids)
            .deletedIds(List.copyOf(deleted))
            .deletedCount(deletedTotal)
            .cursor(cursor)
            .completed(completed)
            .pages(pages);

        switch (rFetchType) {
            case FETCH -> output.messages(messages);
            case FETCH_ONE -> output.message(messages.isEmpty() ? null : messages.getFirst());
            case STORE -> output.uri(store(runContext, messages));
            case NONE -> {
                // ids, count and the cursor are still returned so the batch stays acknowledgeable
            }
        }

        return output.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stream that was scanned"
        )
        private String stream;

        @Schema(
            title = "Consumer group that was scanned"
        )
        private String group;

        @Schema(
            title = "Consumer the entries are now pending for"
        )
        private String consumer;

        @Schema(
            title = "Number of entries taken over by this run"
        )
        private Integer count;

        @Schema(
            title = "Exact stream IDs taken over by this run",
            description = "In delivery order, ready to be passed to `Ack`. Populated for every `fetchType`."
        )
        private List<String> ids;

        @Schema(
            title = "Stream IDs that were pending but no longer exist in the stream",
            description = """
                Entries deleted or trimmed while pending. Redis 7.0 or later removes them from the pending list, so \
                they are not returned as entries and need no acknowledgement. Lists at most the first 1000 IDs, see `deletedCount` for \
                the total. Always empty on older servers."""
        )
        private List<String> deletedIds;

        @Schema(
            title = "Number of pending entries that no longer exist in the stream",
            description = "Total across all scan steps. `deletedIds` lists the first 1000 only, so a heavily trimmed stream does not flood the output."
        )
        private long deletedCount;

        @Schema(
            title = "Where the scan stopped",
            description = "`0-0` when the whole pending list was scanned, otherwise the `startId` to give the next run."
        )
        private String cursor;

        @Schema(
            title = "Whether the scan reached the end of the pending list",
            description = "False when `maxRecords` or `maxPages` was spent first; `cursor` is where to continue."
        )
        private Boolean completed;

        @Schema(
            title = "Number of scan steps this run needed"
        )
        private Integer pages;

        @Schema(
            title = "Entries taken over by this run",
            description = "Only set when `fetchType` is `FETCH`."
        )
        private List<ReadGroup.Message> messages;

        @Schema(
            title = "Single entry taken over by this run",
            description = "Only set when `fetchType` is `FETCH_ONE`; null when nothing was taken."
        )
        private ReadGroup.Message message;

        @Schema(
            title = "URI of the ION file holding the entries",
            description = "Only set when `fetchType` is `STORE`."
        )
        private URI uri;
    }
}
