package io.kestra.plugin.redis.list;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;

import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;

@KestraTest(startRunner = true, startScheduler = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractTriggerTest {
    protected static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    protected void run(String filename, Runnable runnable) throws IOException, URISyntaxException, InterruptedException {
        repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(AbstractTriggerTest.class.getClassLoader().getResource("flows/" + filename)));
        runnable.run();
    }

    protected String getKey() {
        return "mytriggerkey";
    }

    protected ListPush.Output push() throws Exception {
        ListPush task = ListPush.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(ListPush.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(getKey()))
            .from(Arrays.asList("value1", "value2"))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
