package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.services.RedisService;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.plugin.redis.models.Options;
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
@Plugin(
    examples = {
        @Example(
            code = {
                "uri: amqp://guest:guest@localhost:5672/my_vhost",
                "key: mykey",
                "value: myvalue",
                "serdeType: STRING"
            }
        )
    }
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
    private Options options = Options.builder().build();

    @Schema(
        title = "Redis get options",
        description = "Define if you get the older value in response, does not work with Redis 5.X"
    )
    @Builder.Default
    private Boolean get = false;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RedisService connection = this.redisFactory(runContext);

        String oldValue = connection.set(runContext.render(key), serdeType.serialize(runContext.render(value)), get, options.asRedisSet());

        Output output = Output.builder().build();
        if (oldValue != null) {
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
