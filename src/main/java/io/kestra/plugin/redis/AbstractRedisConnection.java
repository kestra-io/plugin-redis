package io.kestra.plugin.redis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRedisConnection extends Task {

    @NotNull
    @PluginProperty(dynamic = true)    @Schema(
        title = "The connection string"
    )
    private String uri;

    public RedisService redisFactory(RunContext runContext) throws IllegalVariableEvaluationException {
        RedisService redisService = new RedisService();
        redisService.connect(runContext, this);

        return redisService;
    }
}
