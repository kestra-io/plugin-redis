package io.kestra.plugin.redis.list;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractTriggerTest {
    protected static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    protected void run(String filename, Runnable runnable) throws IOException, URISyntaxException, InterruptedException {
        try (
            AbstractScheduler scheduler = new JdbcScheduler(this.applicationContext, this.flowListenersService);
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        ) {
            worker.run();
            scheduler.run();

            repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(AbstractTriggerTest.class.getClassLoader().getResource("flows/" + filename)));

            runnable.run();
        }
    }

    protected ListPush.Output push() throws Exception {
        ListPush task = ListPush.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(ListPush.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mytriggerkey"))
            .from(Arrays.asList("value1", "value2"))
            .build();


        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}

