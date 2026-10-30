package io.kestra.plugin.redis.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.models.SerdeType;
import io.kestra.plugin.redis.string.Delete;

import jakarta.inject.Inject;

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
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mypopkey"))
            .count(Property.ofValue(2))
            .maxRecords(Property.ofValue(2))
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
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mypopkeyjson"))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxRecords(Property.ofValue(1))
            .count(Property.ofValue(1))
            .build();

        ListPop.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(1));
    }

    @Test
    void shouldKeepValidItemsWhenOneIsNotJson() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "mypopkeyjsonmixed_%s".formatted(IdUtils.create());

        ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .from(List.of("{\"city\":\"Paris\"}", "{\"city\":\"London\"}", "not-json", "{\"city\":\"Rome\"}", "{\"city\":\"Berlin\"}", "{\"city\":\"Madrid\"}"))
            .build().run(runContext);

        var runOutput = ListPop.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxRecords(Property.ofValue(10))
            .build()
            .run(runContext);

        assertThat(runOutput.getCount(), is(6));
        assertThat(
            runContext.metrics().stream().filter(metric -> metric.getName().equals("popped.records.undecodable.count")).findFirst().map(metric -> metric.getValue()).orElse(null),
            is(1.0)
        );

        var items = new ArrayList<>();
        try (var inputStream = runContext.storage().getFile(runOutput.getUri())) {
            FileSerde.read(inputStream, items::add);
        }
        assertThat(
            items, containsInAnyOrder(
                Map.of("city", "Paris"),
                Map.of("city", "London"),
                "not-json",
                Map.of("city", "Rome"),
                Map.of("city", "Berlin"),
                Map.of("city", "Madrid")
            )
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(Arrays.asList("mypopkey")))
            .build().run(runContext);
        Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(Arrays.asList("mypopkeyjson")))
            .build().run(runContext);
        ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mypopkey"))
            .from(Arrays.asList("value1", "value2", "value3"))
            .build().run(runContext);
        ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mypopkeyjson"))
            .from(Arrays.asList("{\"city\":\"Paris\"}", "{\"city\":\"London\"}"))
            .build().run(runContext);
    }
}
