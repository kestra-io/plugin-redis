package io.kestra.plugin.redis.vector;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Remove elements from a Redis vector set",
    description = "Runs `VREM` once per rendered element (VREM has no multi-element form, unlike `DEL`), counts removals, and can fail when not all elements are removed."
)
@Plugin(
    examples = {
        @Example(
            title = "Remove an embedding from a vector set.",
            full = true,
            code = """
                id: remove_embedding
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.redis.vector.Delete
                    url: "redis://:{{ secret('REDIS_PASSWORD') }}@{{ secret('REDIS_HOST') }}:6379"
                    key: doc_embeddings
                    elements:
                      - doc_42
                """
        )
    }
)
public class Delete extends AbstractRedisConnection implements RunnableTask<Delete.Output> {

    @PluginProperty(group = "main")
    @Schema(
        title = "Vector set key",
        description = "Rendered before calling `VREM`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Element ids to remove",
        description = "Rendered list of element ids; each one is removed with its own `VREM` call."
    )
    @NotNull
    private Property<List<String>> elements;

    @PluginProperty(group = "reliability")
    @Schema(
        title = "Fail when removals are missing",
        description = "Defaults to false; when true, throws if fewer elements are removed than requested."
    )
    @Builder.Default
    private Property<Boolean> failedOnMissing = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String rKey = runContext.render(this.key).as(String.class).orElseThrow();
            List<String> rElements = runContext.render(this.elements).asList(String.class);

            long count = rElements.stream()
                .filter(element -> factory.getSyncCommands().vrem(rKey, element))
                .count();

            boolean isAllRemoved = count == rElements.size();

            if (!isAllRemoved && runContext.render(failedOnMissing).as(Boolean.class).orElse(false)) {
                throw new NullPointerException("Missing elements, only " + count + " element removed");
            }

            runContext.logger().info("Removed {} element(s) from vector set '{}'", count, rKey);

            return Output.builder()
                .count((int) count)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Removed count",
            description = "Total number of elements actually removed."
        )
        private Integer count;
    }
}
