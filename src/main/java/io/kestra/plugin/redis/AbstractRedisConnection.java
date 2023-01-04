package io.kestra.plugin.redis;

import io.kestra.core.models.tasks.Task;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class AbstractRedisConnection extends Task implements RedisConnectionInterface {

    @NotNull
    private String uri;

}
