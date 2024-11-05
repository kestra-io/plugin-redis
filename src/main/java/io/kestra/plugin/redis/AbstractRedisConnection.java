package io.kestra.plugin.redis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRedisConnection extends Task implements RedisConnectionInterface {
    private Property<String> url;

    public RedisFactory redisFactory(RunContext runContext) throws Exception {
        RedisFactory factory = new RedisFactory();
        factory.connect(runContext);
        return factory;
    }

    public class RedisFactory implements AutoCloseable {
        @Getter(AccessLevel.NONE)
        private RedisClient redisClient;

        @Getter(AccessLevel.NONE)
        private StatefulRedisConnection<String, String> redisConnection;

        @Getter(AccessLevel.NONE)
        private RedisCommands<String, String> syncCommands;

        public void connect(RunContext runContext) throws IllegalVariableEvaluationException {
            redisClient = RedisClient.create(runContext.render(url).as(String.class).orElseThrow());
            redisConnection = redisClient.connect();
            syncCommands = redisConnection.sync();
        }

        public String set(String key, String value, Boolean get, SetArgs setArgs) {
            if (Boolean.TRUE.equals(get)) {
                return syncCommands.setGet(key, value);
            } else {
                syncCommands.set(key, value, setArgs);
                return null;
            }
        }

        public String get(String key) {
            return syncCommands.get(key);
        }

        public long del(List<String> keys) {
            return syncCommands.del(keys.toArray(new String[0]));
        }

        public long listPush(String key, List<String> values) {
            return syncCommands.lpush(key, values.toArray(new String[0]));
        }

        public List<String> listPop(String key, Integer count) {
            return syncCommands.lpop(key, count);
        }

        public long publish(String channel, List<String> values) {
            long result = 0;
            for (String value : values) {
                result += syncCommands.publish(channel, value);
            }
            return result;
        }

        public void close() {
            this.redisConnection.close();
            this.redisClient.shutdown();
        }
    }
}
