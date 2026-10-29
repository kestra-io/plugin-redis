package io.kestra.plugin.redis.stream;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
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
    title = "Create a consumer group on a Redis stream",
    description = """
        Runs `XGROUP CREATE` so that [ReadGroup](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.readgroup) and \
        [Ack](https://kestra.io/plugins/plugin-redis/tasks/stream/io.kestra.plugin.redis.stream.ack) have a group to work with.

        `startId` decides what the group considers already handled and is never guessed: `$` starts the group at the current \
        end of the stream so only entries added afterwards are delivered, while `0` replays the whole retained backlog. \
        Any explicit stream ID is also accepted, so a group can start just after a known entry.

        The task is safe to run on every execution: `XGROUP CREATE` fails with `BUSYGROUP` once the group exists, and \
        `ignoreExisting` (on by default) turns that into a no-op while `created` reports whether this run actually created \
        the group. `makeStream` adds `MKSTREAM`, which creates an empty stream when the key does not exist yet, instead of \
        failing on a missing key."""
)
@Plugin(
    examples = {
        @Example(
            title = "Create a consumer group that only receives entries added after the group was created.",
            full = true,
            code = """
                id: redis_stream_group_create
                namespace: company.team

                tasks:
                  - id: create_group
                    type: io.kestra.plugin.redis.stream.GroupCreate
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing
                    startId: "$"
                """
        ),
        @Example(
            title = "Create a consumer group that replays the whole retained backlog of the stream.",
            full = true,
            code = """
                id: redis_stream_group_create_backlog
                namespace: company.team

                tasks:
                  - id: create_group
                    type: io.kestra.plugin.redis.stream.GroupCreate
                    url: "{{ secret('REDIS_URI') }}"
                    key: orders
                    group: billing_backfill
                    startId: "0"
                """
        )
    }
)
public class GroupCreate extends AbstractRedisConnection implements RunnableTask<GroupCreate.Output> {
    @PluginProperty(group = "main")
    @Schema(
        title = "Redis stream key",
        description = "Rendered key passed to `XGROUP CREATE`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Consumer group name",
        description = "Rendered group name passed to `XGROUP CREATE`."
    )
    @NotNull
    private Property<String> group;

    @PluginProperty(group = "main")
    @Schema(
        title = "Stream ID the group starts from",
        description = """
            Defaults to `$`: the group is positioned at the current end of the stream, so it only receives entries added \
            after this task ran and the existing backlog is treated as already handled. Use `0` to deliver the whole \
            retained backlog instead, or any explicit stream ID (for example `1700000000000-0`) to start right after that \
            entry. Entries trimmed out of the stream are never delivered, whatever the value."""
    )
    @Builder.Default
    @NotNull
    private Property<String> startId = Property.ofValue("$");

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Create the stream when the key is missing",
        description = """
            Defaults to true, which adds `MKSTREAM` so an empty stream is created instead of failing on a missing \
            key. Set to false to require the stream to exist already."""
    )
    @Builder.Default
    @NotNull
    private Property<Boolean> makeStream = Property.ofValue(true);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Succeed when the group already exists",
        description = """
            Defaults to true, which turns the `BUSYGROUP` error of a second run into a no-op and leaves the existing \
            group untouched, including its current position and pending entries. Set to false to fail instead. The \
            `created` output tells the two cases apart."""
    )
    @Builder.Default
    @NotNull
    private Property<Boolean> ignoreExisting = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var factory = this.redisFactory(runContext)) {
            var rKey = runContext.render(this.key).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("`key` rendered to an empty value; set the Redis stream key"));
            var rGroup = runContext.render(this.group).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("`group` rendered to an empty value; set a consumer group name"));
            var rStartId = runContext.render(this.startId).as(String.class).orElse("$");
            var rMakeStream = runContext.render(this.makeStream).as(Boolean.class).orElse(true);
            var rIgnoreExisting = runContext.render(this.ignoreExisting).as(Boolean.class).orElse(true);

            try {
                factory.getSyncCommands().xgroupCreate(
                    XReadArgs.StreamOffset.from(rKey, rStartId),
                    rGroup,
                    XGroupCreateArgs.Builder.mkstream(rMakeStream)
                );
            } catch (RedisBusyException e) {
                if (!rIgnoreExisting) {
                    throw e;
                }

                runContext.logger().info(
                    "Consumer group '{}' already exists on stream '{}', leaving it untouched.",
                    rGroup,
                    rKey
                );

                return Output.builder()
                    .key(rKey)
                    .group(rGroup)
                    .startId(rStartId)
                    .created(false)
                    .build();
            }

            runContext.logger().info(
                "Created consumer group '{}' on stream '{}' starting at '{}'.",
                rGroup,
                rKey,
                rStartId
            );

            return Output.builder()
                .key(rKey)
                .group(rGroup)
                .startId(rStartId)
                .created(true)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stream key the group was created on"
        )
        private String key;

        @Schema(
            title = "Consumer group name"
        )
        private String group;

        @Schema(
            title = "Stream ID the group was requested to start from"
        )
        private String startId;

        @Schema(
            title = "Whether this run created the group",
            description = "False when the group already existed and `ignoreExisting` is true."
        )
        private Boolean created;
    }
}
