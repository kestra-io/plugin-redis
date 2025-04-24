package io.kestra.plugin.redis;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.utils.TestsUtils;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class RealtimeTriggerTest extends AbstractTriggerTest {

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(4);
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            assertThat(execution.getLeft().getFlowId(), is("realtime"));
            queueCount.countDown();
        });

        this.run("realtime.yaml", throwRunnable(() -> {
            push();
            push();

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            List<Execution> executionList = receive.collectList().block();
            assertThat(executionList.size(), is(4));
            assertThat(executionList.stream().filter(execution -> execution.getTrigger().getVariables().get("value").equals("value2")).count(), is(2L));
        }));
    }
}

