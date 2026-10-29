package io.kestra.plugin.redis.list;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.kestra.core.utils.TestsUtils;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

class RealtimeTriggerJsonTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_realtime_json";
    }

    @Test
    void shouldStartAnExecutionForAnItemThatIsNotJson() throws Exception {
        var queueCount = new CountDownLatch(3);
        var valuesByExecution = new ConcurrentHashMap<String, Object>();
        var receive = TestsUtils.receive(executionQueue, either ->
        {
            var execution = either.getLeft();
            if (
                execution.getFlowId().equals("realtime-json")
                    && valuesByExecution.putIfAbsent(execution.getId(), execution.getTrigger().getVariables().get("value")) == null
            ) {
                queueCount.countDown();
            }
        });

        this.run("realtime-json.yaml", throwRunnable(() ->
        {
            push(List.of("{\"city\":\"Paris\"}", "not-json", "{\"city\":\"Rome\"}"));

            assertThat(queueCount.await(30, TimeUnit.SECONDS), is(true));
            receive.collectList().block();

            assertThat(valuesByExecution.values(), hasItems(Map.of("city", "Paris"), "not-json", Map.of("city", "Rome")));
        }));
    }
}
