package io.kestra.plugin.redis.json;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonObject;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.json.arguments.JsonGetArgs;
import io.lettuce.core.json.arguments.JsonSetArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch a Redis item by key with type `JSON` and return its value.",
    description = "Query for a key in a Redis database and return the associated JSON value."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_json_get
                namespace: company.team

                inputs:
                    - id: key_name
                      type: STRING
                      displayName: Key name to search

                tasks:
                  - id: get
                    type: io.kestra.plugin.redis.json.Get
                    url: redis://:redis@localhost:6379/0
                    key: "{{ inputs.key_name }}"
                """
        )
    }
)
public class Get extends AbstractRedisConnection implements RunnableTask<Get.Output> {
    @Schema(
        title = "The redis key you want to get,"
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "If some keys are not defined, fail the task."
    )
    @Builder.Default
    private Property<Boolean> failedOnMissing = Property.ofValue(false);

    @Schema(
        title = "JSON path to extract value (default is root '$')"
    )
    @Builder.Default
    private Property<String> path = Property.ofValue("$");


    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            final String renderedKey = runContext.render(this.key).as(String.class).orElseThrow();
            String renderedPath = runContext.render(this.path).as(String.class).orElse("$");

            Object result = factory.getSyncCommands().jsonGet(renderedKey, JsonPath.of(renderedPath)).getFirst().toObject(Object.class);

            if (result instanceof List<?> list && list.size() == 1) {
                result = list.getFirst();
            }

            if (result == null && runContext.render(failedOnMissing).as(Boolean.class).orElseThrow()) {
                throw new NullPointerException("Missing keys '" + renderedKey + "'");
            }

            runContext.logger().info("Result: {}", result);

            return Output.builder()
                .data(result)
                .key(renderedKey)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The fetched data."
        )
        private Object data;

        @Schema(
            title = "The fetched key."
        )
        private String key;
    }
}
