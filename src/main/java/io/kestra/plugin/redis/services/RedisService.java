package io.kestra.plugin.redis.services;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;

public class RedisService implements RedisInterface {
    RedisClient redisClient;
    StatefulRedisConnection redisConnection;
    RedisCommands<String, String> syncCommands;

    @Override
    @SuppressWarnings("unchecked")
    public void connect(RunContext runContext, AbstractRedisConnection connection) {
        redisClient = RedisClient.create(connection.getUri());
        redisConnection = redisClient.connect();
        syncCommands = redisConnection.sync();
    }

    @Override
    public String set(String key, String value, Boolean get, SetArgs setArgs) {
        if (get) {
            return syncCommands.setGet(key, value);
        } else {
            syncCommands.set(key, value, setArgs);
            return null;
        }
    }

    @Override
    public String get(String key) {
        return syncCommands.get(key);
    }

    @Override
    public long del(List<String> keys) {
        return syncCommands.del(keys.toArray(new String[0]));
    }

    @Override
    public long listPush(String key, List<String> values) {
        return syncCommands.lpush(key, values.toArray(new String[0]));
    }

    @Override
    public List<String> listPop(String key, Integer count) {
        return syncCommands.lpop(key, count);
    }

    @Override
    public void close() {
        this.redisConnection.close();
        this.redisClient.shutdown();
    }

}
