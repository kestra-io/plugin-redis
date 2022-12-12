package io.kestra.plugin.templates;

import io.kestra.plugin.templates.client.RedisApiService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

import static io.micronaut.runtime.Micronaut.build;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Redis Client Task",
    description = "Interact with REDIS"
)
public class RedisTask extends Task implements RunnableTask<RedisTask.Output> {

    @Inject
    RedisApiService redisApiService;

    @Schema(
            title = "Redis Operation",
            description = "The redis operation you want to run"
    )
    @NotBlank
    @PluginProperty(dynamic = true) // If the variables will be rendered with template {{ }}
    private String operation;

    @Schema(
            title = "Redis keys",
            description = "The redis key you want to fetch"
    )
    private String key;

    @Schema(
            title = "Redis value to store",
            description = "The value you want to store with key"
    )
    private String value;

    @Override
    public RedisTask.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String render = runContext.render(operation);
        logger.debug(render);
        RedisTask.Output output = null;


        switch (render) {
            case "GET": {
                output = Output.builder()
                        .child(new OutputChild(redisApiService.get(key)))
                        .build();
                break;
            }
            case "SET": {
                output = Output.builder()
                        .child(new OutputChild(redisApiService.set(key, value)))
                        .build();
                break;
            }
            default:
                break;
        }

        return output;
    }

    /**
     * Input or Output can nested as you need
     */
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Short description for this output",
            description = "Full description of this output"
        )
        private final OutputChild child;
    }

    @Builder
    @Getter
    public static class OutputChild implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Short description for this output",
            description = "Full description of this output"
        )
        private final String value;
    }
}
