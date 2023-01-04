package io.kestra.plugin.redis.services;


import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

public abstract class RedisFactory {
    public static RedisInterface create(RunContext runContext, AbstractRedisConnection connection) throws Exception {
        RedisService redisService = new RedisService();
        redisService.connect(runContext, connection);
        return redisService;
    }
}
