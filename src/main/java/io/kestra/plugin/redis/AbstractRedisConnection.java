package io.kestra.plugin.redis;

import java.time.Duration;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRedisConnection extends Task implements RedisConnectionInterface {
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
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

        @Getter
        private RedisCommands<String, String> syncCommands;

        public void connect(RunContext runContext) throws IllegalVariableEvaluationException {
            redisClient = RedisClient.create(runContext.render(url).as(String.class).orElseThrow());
            redisConnection = redisClient.connect();
            syncCommands = redisConnection.sync();
        }

        // lets a blocking command such as XREADGROUP BLOCK outlast Lettuce's default command timeout
        public void setTimeout(Duration timeout) {
            this.redisConnection.setTimeout(timeout);
        }

        // non-blocking: fails in-flight commands without waiting for the client shutdown done by close()
        public void closeAsync() {
            this.redisConnection.closeAsync();
        }

        public void close() {
            this.redisConnection.close();
            this.redisClient.shutdown();
        }
    }
}
