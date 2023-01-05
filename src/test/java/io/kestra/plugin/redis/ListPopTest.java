package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.redis.services.SerdeType;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListPopTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testListPop() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        ListPop task = ListPop.builder()
            .uri(REDIS_URI)
            .key("mypopkey")
            .count(2)
            .maxRecords(1)
            .build();

        ListPop.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void testListPopJson() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        ListPop task = ListPop.builder()
            .uri(REDIS_URI)
            .key("mypopkeyjson")
            .serdeType(SerdeType.JSON)
            .maxRecords(1)
            .build();

        ListPop.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), greaterThanOrEqualTo(1));
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Delete.builder()
            .uri(REDIS_URI)
            .keys(Arrays.asList("mypopkey"))
            .build().run(runContext);
        ListPush.builder()
            .uri(REDIS_URI)
            .key("mypopkey")
            .from(Arrays.asList("value1", "value2", "value3"))
            .build().run(runContext);
        ListPush.builder()
            .uri(REDIS_URI)
            .key("mypopkeyjson")
            .from(Arrays.asList("{\"city\":\"Paris\"}", "{\"city\":\"London\"}"))
            .build().run(runContext);
    }
}
