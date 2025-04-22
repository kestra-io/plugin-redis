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

        @Getter
        private RedisCommands<String, String> syncCommands;

        public void connect(RunContext runContext) throws IllegalVariableEvaluationException {
            redisClient = RedisClient.create(runContext.render(url).as(String.class).orElseThrow());
            redisConnection = redisClient.connect();
            syncCommands = redisConnection.sync();
        }

        public void close() {
            this.redisConnection.close();
            this.redisClient.shutdown();
        }
    }
}
