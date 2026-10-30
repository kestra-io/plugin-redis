package io.kestra.plugin.redis.list;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.redis.string.Delete;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class TriggerJsonTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_json";
    }

    @BeforeEach
    void setUp() throws Exception {
        var delete = Delete.builder()
            .id(TriggerJsonTest.class.getSimpleName())
            .type(Delete.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(List.of(getKey())))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));

        push(List.of("{\"city\":\"Paris\"}", "{\"city\":\"London\"}", "not-json", "{\"city\":\"Rome\"}", "{\"city\":\"Berlin\"}", "{\"city\":\"Madrid\"}"));
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger-json.yaml", triggerId = "watch")
    void shouldTriggerWithValidItemsWhenOneIsNotJson(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        assertThat(optionalExecution.get().getTrigger().getVariables().get("count"), is(6));
    }
}
