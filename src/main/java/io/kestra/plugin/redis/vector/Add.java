package io.kestra.plugin.redis.vector;

import java.util.List;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.models.SerdeType;

import io.lettuce.core.VAddArgs;
import io.lettuce.core.vector.QuantizationType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Add an element to a Redis vector set",
    description = """
        Runs `VADD` to attach an embedding to an element id inside a Redis vector set, creating the set on first use. \
        See the [Redis vector set documentation](https://redis.io/docs/latest/develop/data-types/vector-sets/) for details."""
)
@Plugin(
    examples = {
        @Example(
            title = "Add an embedding vector to a Redis vector set.",
            full = true,
            code = """
                id: store_embedding
                namespace: company.team

                inputs:
                  - id: embedding
                    type: ARRAY
                    itemType: FLOAT

                tasks:
                  - id: add_vector
                    type: io.kestra.plugin.redis.vector.Add
                    url: "redis://:{{ secret('REDIS_PASSWORD') }}@{{ secret('REDIS_HOST') }}:6379"
                    key: "doc_embeddings"
                    element: "doc_42"
                    vector: "{{ inputs.embedding }}"
                """
        )
    }
)
public class Add extends AbstractRedisConnection implements RunnableTask<Add.Output> {

    @PluginProperty(group = "main")
    @Schema(
        title = "Vector set key",
        description = "Rendered before calling `VADD`. The vector set is created automatically the first time an element is added."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Element id",
        description = "Unique identifier of the member the vector is attached to inside the vector set."
    )
    @NotNull
    private Property<String> element;

    @PluginProperty(group = "main")
    @Schema(
        title = "Vector",
        description = "The embedding to store, as a list of numbers. All elements added to a given vector set must share the same dimensionality unless `reduceDim` is used."
    )
    @NotNull
    private Property<List<Double>> vector;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Reduce dimensionality",
        description = "Maps to the VADD `REDUCE` option. When set, the vector is randomly projected down to this many dimensions before being stored, trading precision for reduced memory usage. Must be less than `vector`'s dimensionality. When left unset, the property is omitted from the `VADD` call and the vector is stored at its original dimensionality."
    )
    @Min(1)
    private Property<Integer> reduceDim;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Quantization type",
        description = "Maps to the VADD quantization option. Reuses Lettuce's own `io.lettuce.core.vector.QuantizationType` enum, whose constants are `NO_QUANTIZATION` (full-precision floats), `BINARY` (1-bit per component), and `Q8` (8-bit integers). When left unset, the property is omitted from the `VADD` call and Redis applies its own default (`Q8`)."
    )
    private Property<QuantizationType> quantization;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Exploration factor",
        description = "Maps to the VADD `EF` option. Controls the search effort used while inserting the new node into the underlying HNSW graph; higher values improve insertion quality at the cost of speed. When left unset, the property is omitted from the `VADD` call and Redis applies its own default."
    )
    @Min(1)
    private Property<Integer> explorationFactor;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Max connections per node",
        description = "Maps to the VADD `M` option. Maximum number of connections each node of the underlying HNSW graph can have; higher values improve recall at the cost of memory. When left unset, the property is omitted from the `VADD` call and Redis applies its own default."
    )
    @Min(1)
    private Property<Integer> maxNodes;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Check and set",
        description = "Maps to the VADD `CAS` option. When true, performs the insertion as a check-and-set operation to reduce contention under concurrent writes."
    )
    private Property<Boolean> checkAndSet;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Attributes",
        description = "Arbitrary JSON object attached to the element. Serialized and stored alongside the vector, it can later be referenced in a `Similarity` task's `filter` expression."
    )
    private Property<Object> attributes;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String rKey = runContext.render(this.key).as(String.class).orElseThrow();
            String rElement = runContext.render(this.element).as(String.class).orElseThrow();
            List<Double> rVector = runContext.render(this.vector).asList(Double.class);

            if (rVector.isEmpty()) {
                throw new IllegalArgumentException("`vector` must contain at least one dimension");
            }

            VAddArgs args = new VAddArgs();
            runContext.render(this.quantization).as(QuantizationType.class).ifPresent(args::quantizationType);
            runContext.render(this.explorationFactor).as(Integer.class).ifPresent(v -> args.explorationFactor(v.longValue()));
            runContext.render(this.maxNodes).as(Integer.class).ifPresent(v -> args.maxNodes(v.longValue()));
            runContext.render(this.checkAndSet).as(Boolean.class).ifPresent(args::checkAndSet);

            Optional<Object> rAttributes = runContext.render(this.attributes).as(Object.class);
            if (rAttributes.isPresent()) {
                args.attributes(SerdeType.JSON.serialize(rAttributes.get()));
            }

            Double[] vectorArray = rVector.toArray(new Double[0]);
            Optional<Integer> rReduceDim = runContext.render(this.reduceDim).as(Integer.class);

            Boolean added = rReduceDim.isPresent()
                ? factory.getSyncCommands().vadd(rKey, rReduceDim.get(), rElement, args, vectorArray)
                : factory.getSyncCommands().vadd(rKey, rElement, args, vectorArray);

            runContext.logger().info("Added element '{}' to vector set '{}': {}", rElement, rKey, added);

            return Output.builder()
                .added(added)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Whether the element was added",
            description = "Raw `VADD` reply: true only when the element id did not previously exist in the vector set. False means the element already existed, whether or not its vector or attributes were changed by this call."
        )
        private Boolean added;
    }
}
