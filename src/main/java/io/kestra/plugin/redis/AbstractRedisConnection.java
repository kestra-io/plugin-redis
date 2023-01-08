package io.kestra.plugin.redis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisService;
import lombok.*;
import lombok.experimental.SuperBuilder;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRedisConnection extends Task implements RedisConnectionInterface {

    private String uri;

    public RedisService redisFactory(RunContext runContext) throws IllegalVariableEvaluationException {
        RedisService redisService = new RedisService();
        redisService.connect(runContext, this);

        return redisService;
    }
}
