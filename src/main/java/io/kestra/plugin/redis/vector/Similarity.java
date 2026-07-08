package io.kestra.plugin.redis.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.lettuce.core.VSimArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
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
    title = "Run a similarity search on a Redis vector set",
    description = """
        Runs `VSIM` to find the elements of a Redis vector set closest to a query vector or to an existing element, ranked by similarity score. \
        See the [Redis VSIM documentation](https://redis.io/docs/latest/commands/vsim/) for details."""
)
@Plugin(
    examples = {
        @Example(
            title = "Find the 5 embeddings most similar to a query vector.",
            full = true,
            code = """
                id: similarity_search
                namespace: company.team

                inputs:
                  - id: queryVector
                    type: ARRAY
                    itemType: FLOAT

                tasks:
                  - id: search
                    type: io.kestra.plugin.redis.vector.Similarity
                    url: "redis://:{{ secret('REDIS_PASSWORD') }}@{{ secret('REDIS_HOST') }}:6379"
                    key: "doc_embeddings"
                    vector: "{{ inputs.queryVector }}"
                    count: 5

                  - id: log_matches
                    type: io.kestra.plugin.core.log.Log
                    message: "Top matches: {{ outputs.search.matches }}"
                """
        )
    }
)
public class Similarity extends AbstractRedisConnection implements RunnableTask<Similarity.Output> {

    @PluginProperty(group = "main")
    @Schema(
        title = "Vector set key",
        description = "Rendered before calling `VSIM`."
    )
    @NotNull
    private Property<String> key;

    @PluginProperty(group = "main")
    @Schema(
        title = "Query vector",
        description = "The vector to search similar elements for, as a list of numbers. Set exactly one of `vector` or `element`."
    )
    private Property<List<Double>> vector;

    @PluginProperty(group = "main")
    @Schema(
        title = "Query element",
        description = "The id of an existing element to search similar elements for. Set exactly one of `vector` or `element`. " +
            "If the id does not exist in the vector set, `VSIM` returns an empty result rather than an error."
    )
    private Property<String> element;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Result count",
        description = "Maps to the VSIM `COUNT` option; the maximum number of matches to return, between 1 and 10000. When left unset, the property is omitted from the `VSIM` call and Redis applies its own default (10)."
    )
    private Property<@Min(1) @Max(10000) Integer> count;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Filter expression",
        description = "Maps to the VSIM `FILTER` option; restricts matches to elements whose attributes satisfy this expression, see [attribute filtering](https://redis.io/docs/latest/develop/data-types/vector-sets/#filtered-search). When left unset, the property is omitted from the `VSIM` call and no filtering is applied."
    )
    private Property<String> filter;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Filter efficiency",
        description = "Maps to the VSIM `FILTER-EF` option; caps the effort spent scanning candidates to satisfy `filter` before giving up. Only meaningful together with `filter`. When left unset, the property is omitted from the `VSIM` call and Redis applies its own default."
    )
    private Property<@Min(1) Integer> filterEfficiency;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Exploration factor",
        description = "Maps to the VSIM `EF` option; controls the search effort in the underlying HNSW graph, higher values improve recall at the cost of speed. When left unset, the property is omitted from the `VSIM` call and Redis applies its own default."
    )
    private Property<@Min(1) Integer> explorationFactor;

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Epsilon",
        description = "Maps to the VSIM `EPSILON` option; a distance threshold controlling the range of the graph exploration. Must be a non-negative number. When left unset, the property is omitted from the `VSIM` call and Redis applies its own default."
    )
    private Property<@DecimalMin("0.0") Double> epsilon;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {
            String rKey = runContext.render(this.key).as(String.class).orElseThrow();
            List<Double> rVector = runContext.render(this.vector).asList(Double.class);
            Optional<String> rElement = runContext.render(this.element).as(String.class);

            boolean hasVector = !rVector.isEmpty();
            boolean hasElement = rElement.isPresent();

            if (hasVector == hasElement) {
                throw new IllegalArgumentException("Exactly one of `vector` or `element` must be set to run a similarity search");
            }

            VSimArgs args = new VSimArgs();
            runContext.render(this.count).as(Integer.class).ifPresent(v -> args.count(v.longValue()));
            runContext.render(this.filter).as(String.class).ifPresent(args::filter);
            runContext.render(this.filterEfficiency).as(Integer.class).ifPresent(v -> args.filterEfficiency(v.longValue()));
            runContext.render(this.explorationFactor).as(Integer.class).ifPresent(v -> args.explorationFactor(v.longValue()));
            runContext.render(this.epsilon).as(Double.class).ifPresent(args::epsilon);

            Map<String, Double> scores = hasVector
                ? factory.getSyncCommands().vsimWithScore(rKey, args, rVector.toArray(new Double[0]))
                : factory.getSyncCommands().vsimWithScore(rKey, args, rElement.get());

            List<String> matches = new ArrayList<>(scores.keySet());

            runContext.logger().info("Found {} match(es) in vector set '{}'", matches.size(), rKey);

            return Output.builder()
                .matches(matches)
                .scores(scores)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Matching element ids",
            description = "Element ids returned by `VSIM`, ranked from most to least similar."
        )
        private List<String> matches;

        @Schema(
            title = "Similarity scores",
            description = "Similarity score for each element id in `matches`, in the same order."
        )
        private Map<String, Double> scores;
    }
}
