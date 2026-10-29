package io.kestra.plugin.redis.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.lettuce.core.json.JsonPath;
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
    title = "Delete Redis JSON keys or paths",
    description = "Runs `JSON.DEL` for each rendered key and JSON path (defaults to `$` when paths are empty), sums deleted elements, and can fail when a requested key or path deletes nothing."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: redis_json_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.redis.json.Delete
                    url: redis://:redis@localhost:6379/0
                    failedOnMissing: true
                    keys:
                      keyDelete1:
                         - path1
                         - path2
                """
        )
    },
    metrics = {
        @Metric(
            name = "deleted.records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records deleted from Redis."
        )
    }
)
public class Delete extends AbstractRedisConnection implements RunnableTask<Delete.Output> {
    @PluginProperty(group = "main")
    @Schema(
        title = "Keys and JSON paths to delete",
        description = "Map of key → paths; each path list defaults to `$` to drop the whole value."
    )
    @NotNull
    private Property<Map<String, List<String>>> keys;

    @PluginProperty(group = "reliability")
    @Schema(
        title = "Fail when deletions are missing",
        description = "Defaults to false; when true, the task fails if any requested key or path deletes nothing (for example a missing key or path, a path listed twice, or a child path listed after its parent). Paths that exist are deleted before the task fails."
    )
    @Builder.Default
    private Property<Boolean> failedOnMissing = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (RedisFactory factory = this.redisFactory(runContext)) {

            Map<String, List<String>> renderedKeys = runContext.render(keys).asMap(String.class, List.class);

            long totalDeleted = 0;
            var requested = 0;
            var missing = new ArrayList<String>();

            for (Map.Entry<String, List<String>> entry : renderedKeys.entrySet()) {
                String redisKey = entry.getKey();
                List<String> paths = entry.getValue();

                if (paths.isEmpty()) {
                    paths = List.of("$");
                }

                for (String path : paths) {
                    long count = factory.getSyncCommands().jsonDel(redisKey, JsonPath.of(path));
                    totalDeleted += count;
                    requested++;
                    if (count == 0) {
                        missing.add(redisKey + " " + path);
                    }
                }
            }

            if (!missing.isEmpty() && runContext.render(failedOnMissing).as(Boolean.class).orElse(false)) {
                throw new IllegalStateException(
                    missing.size() + " of " + requested + " requested paths deleted nothing: " + String.join(", ", missing)
                        + ". Paths that exist were still deleted. Set `failedOnMissing` to false to accept that."
                );
            }

            runContext.metric(Counter.of("deleted.records.count", totalDeleted));

            return Output.builder()
                .count((int) totalDeleted)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Deleted count",
            description = "Total number of deleted elements across all keys/paths."
        )
        private Integer count;
    }
}
