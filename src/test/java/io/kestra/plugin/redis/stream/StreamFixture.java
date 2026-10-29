package io.kestra.plugin.redis.stream;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessage;

// Seeds streams and reads delivery state back on its own Lettuce connection, so assertions do not trust task output.
final class StreamFixture implements AutoCloseable {
    private final RedisClient client;

    private final StatefulRedisConnection<String, String> connection;

    private final RedisCommands<String, String> commands;

    StreamFixture(String uri) {
        this.client = RedisClient.create(uri);
        this.connection = this.client.connect();
        this.commands = this.connection.sync();
    }

    /** Appends an entry and returns the ID Redis assigned to it. */
    String add(String key, Map<String, String> fields) {
        return this.commands.xadd(key, fields);
    }

    void createGroup(String key, String group, String startId) {
        this.commands.xgroupCreate(
            XReadArgs.StreamOffset.from(key, startId),
            group,
            XGroupCreateArgs.Builder.mkstream(true)
        );
    }

    /** The IDs currently pending for the whole group, oldest first. */
    List<String> pendingIds(String key, String group) {
        return this.commands.xpending(key, group, Range.unbounded(), Limit.unlimited())
            .stream()
            .map(PendingMessage::getId)
            .toList();
    }

    /** The IDs currently pending for one consumer of the group, oldest first. */
    List<String> pendingIds(String key, String group, String consumer) {
        return this.commands.xpending(key, Consumer.from(group, consumer), Range.unbounded(), Limit.unlimited())
            .stream()
            .map(PendingMessage::getId)
            .toList();
    }

    // a worker that read entries and then died: they are pending for the consumer and nothing will acknowledge them
    List<String> deliver(String key, String group, String consumer, int count) {
        var read = this.commands.xreadgroup(
            Consumer.from(group, consumer),
            new XReadArgs().count(count),
            XReadArgs.StreamOffset.from(key, ">")
        );

        return read == null ? List.of() : read.stream().map(StreamMessage::getId).toList();
    }

    // backdates the idle time of pending entries without changing who owns them
    void makeIdle(String key, String group, Duration idle, String... ids) {
        for (var entry : this.owners(key, group).entrySet()) {
            if (List.of(ids).contains(entry.getKey())) {
                this.commands.xclaim(key, Consumer.from(group, entry.getValue()), new XClaimArgs().minIdleTime(0).idle(idle), entry.getKey());
            }
        }
    }

    // pending entry ID to the consumer that owns it, in pending order
    Map<String, String> owners(String key, String group) {
        var owners = new LinkedHashMap<String, String>();
        for (var pending : this.commands.xpending(key, group, Range.unbounded(), Limit.unlimited())) {
            owners.put(pending.getId(), pending.getConsumer());
        }

        return owners;
    }

    long deliveryCount(String key, String group, String id) {
        return this.commands.xpending(key, group, Range.create(id, id), Limit.unlimited()).getFirst().getRedeliveryCount();
    }

    void remove(String key, String... ids) {
        this.commands.xdel(key, ids);
    }

    long streamLength(String key) {
        return this.commands.xlen(key);
    }

    // control: one XREADGROUP BLOCK on this fixture's own connection, to show a short client timeout aborts it
    List<String> blockingRead(String key, String group, String consumer, Duration block) {
        var read = this.commands.xreadgroup(
            Consumer.from(group, consumer),
            new XReadArgs().count(10).block(block),
            XReadArgs.StreamOffset.from(key, ">")
        );

        return read == null ? List.of() : read.stream().map(StreamMessage::getId).toList();
    }

    void delete(String... keys) {
        this.commands.del(keys);
    }

    @Override
    public void close() {
        this.connection.close();
        this.client.shutdown();
    }
}
