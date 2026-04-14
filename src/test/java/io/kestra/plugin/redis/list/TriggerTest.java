package io.kestra.plugin.redis.list;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.models.executions.Execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_trigger";
    }

    @BeforeEach
    void setUp() throws Exception {
        push();
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        Integer count = (Integer) execution.getTrigger().getVariables().get("count");
        assertThat(count, greaterThanOrEqualTo(2));
    }
}
