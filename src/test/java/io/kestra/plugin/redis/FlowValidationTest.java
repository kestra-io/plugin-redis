package io.kestra.plugin.redis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.FlowSource;
import io.kestra.core.plugins.PluginScanner;
import io.kestra.core.services.FlowService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertNull;

@KestraTest
class FlowValidationTest {
    @Inject
    private FlowService flowService;

    @Test
    void allFullPluginExamplesValidate() {
        var classes = new PluginScanner(FlowValidationTest.class.getClassLoader()).scan().allClass().stream()
            .filter(c -> c.getPackageName().startsWith("io.kestra.plugin.redis"))
            .toList();
        assertThat(classes, hasItem(io.kestra.plugin.redis.list.RealtimeTrigger.class));

        for (var type : classes) {
            Plugin plugin = (Plugin) type.getAnnotation(Plugin.class);
            if (plugin == null) {
                continue;
            }
            for (var example : plugin.examples()) {
                if (!example.full()) {
                    continue;
                }
                var results = flowService.validate("main", List.of(new FlowSource("test.yaml", String.join("\n", example.code()))));
                assertNull(results.getFirst().getConstraints(), type.getName() + ": " + results);
            }
        }
    }
}
