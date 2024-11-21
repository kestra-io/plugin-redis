package io.kestra.plugin.redis;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.redis.list.ListPop;
import io.kestra.plugin.redis.list.ListPush;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.plugin.redis.string.Delete;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListPopTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testListPop() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        ListPop task = ListPop.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("mypopkey"))
            .count(Property.of(2))
            .maxRecords(Property.of(2))
            .build();

        ListPop.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));

        // second lpop should return the thrid item
        runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(1));

        // third lpop should return nothing
        runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(0));

    }

    @Test
    void testListPopJson() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        ListPop task = ListPop.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("mypopkeyjson"))
            .serdeType(Property.of(SerdeType.JSON))
            .maxRecords(Property.of(1))
            .count(Property.of(1))
            .build();

        ListPop.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(1));
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Delete.builder()
            .url(Property.of(REDIS_URI))
            .keys(Property.of(Arrays.asList("mypopkey")))
            .build().run(runContext);
        Delete.builder()
            .url(Property.of(REDIS_URI))
            .keys(Property.of(Arrays.asList("mypopkeyjson")))
            .build().run(runContext);
        ListPush.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("mypopkey"))
            .from(Arrays.asList("value1", "value2", "value3"))
            .build().run(runContext);
        ListPush.builder()
            .url(Property.of(REDIS_URI))
            .key(Property.of("mypopkeyjson"))
            .from(Arrays.asList("{\"city\":\"Paris\"}", "{\"city\":\"London\"}"))
            .build().run(runContext);
    }
}
