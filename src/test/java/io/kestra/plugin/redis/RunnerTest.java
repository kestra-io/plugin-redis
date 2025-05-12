package io.kestra.plugin.redis;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {
    @Test
    @ExecuteFlow("sanity-checks/increment.yaml")
    void increment(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(5));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/keys-string.yaml")
    void keysString(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(9));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/keys-json.yaml")
    void keysJson(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(3));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/list-string.yaml")
    void listString(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(3));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/list-json.yaml")
    void listJson(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(3));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
