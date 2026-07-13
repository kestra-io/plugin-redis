package io.kestra.plugin.redis.list;

import java.util.Arrays;

import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractTriggerTest {
    protected static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Inject
    protected RunContextFactory runContextFactory;

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
