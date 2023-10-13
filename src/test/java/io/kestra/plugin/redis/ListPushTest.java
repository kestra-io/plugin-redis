package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListPushTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testListPushAsList() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        ListPush task = ListPush.builder()
            .url(REDIS_URI)
            .key("mykey")
            .from(Arrays.asList("value1", "value2"))
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @Test
    void testListPushAsFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URI uri = createTestFile();

        ListPush task = ListPush.builder()
            .url(REDIS_URI)
            .key("mykeyFile")
            .from(uri.toString())
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(5));
    }

    @Test
    void testListPushAsString() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URI uri = createTestFile();

        ListPush task = ListPush.builder()
            .url(REDIS_URI)
            .key("mykeyFile")
            .from("[\"value1\", \"value2\"]")
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Delete.builder()
            .url(REDIS_URI)
            .keys(List.of("mykey"))
            .build().run(runContext);
        Delete.builder()
            .url(REDIS_URI)
            .keys(List.of("mykeyFile"))
            .build().run(runContext);
    }

    URI createTestFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.tempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, i);
        }
        return storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
