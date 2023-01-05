package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisFactory;
import io.kestra.plugin.redis.services.RedisInterface;
import io.kestra.plugin.redis.services.SetOptions;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Set the string value of a key."
)
public class Set extends AbstractRedisConnection implements RunnableTask<Set.Output> {

    @Schema(
            title = "Redis key",
            description = "The redis key you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
            title = "Redis value",
            description = "The value you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String value;

    @Schema(
            title = "Redis Set options",
            description = "Options available when setting a key in Redis\n" +
                    "See https://redis.io/commands/set/"
    )
    @Builder.Default
    private SetOptions setOptions = SetOptions.builder().build();

    @Schema(
            title = "Redis get options",
            description = "Define if you get the older value in response, does not work with Redis 5.X"
    )
    @Builder.Default
    private Boolean get = false;


    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisInterface connection = RedisFactory.create(runContext, this);

        String oldValue = connection.set(runContext.render(key), runContext.render(value), get, setOptions.getRedisSetArgs());

        Output output = Output.builder().build();
        if(oldValue != null){
            output.oldValue = oldValue;
        }

        connection.close();
        return output;
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Old value",
                description = "The old value if you replaced an existing key\n" +
                        "Required Get to true"
        )
        private String oldValue;
    }
}
