package io.kestra.plugin.redis.services;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.lettuce.core.SetArgs;

import java.util.List;

public interface RedisInterface {
    void connect(RunContext runContext, AbstractRedisConnection connection) throws Exception;

    String set(String key, String value, Boolean get, SetArgs setArgs);

    String get(String key);

    long del(List<String> keys);

    long listPush(String key, List<String> values);

    List<String> listPop(String key, Integer count);

    void close() throws Exception;

    default RedisInterface create() {
        return new RedisService();
    }
}
