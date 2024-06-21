package io.kestra.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.pubsub.Publish;
import io.kestra.plugin.redis.string.Delete;
import io.kestra.core.junit.annotations.KestraTest;
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

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PublishTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testPublishAsList() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Publish task = Publish.builder()
            .url(REDIS_URI)
            .channel("mych")
            .from(Arrays.asList("value1", "value2"))
            .build();

        Publish.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @Test
    void testPublishAsFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URI uri = createTestFile();

        Publish task = Publish.builder()
            .url(REDIS_URI)
            .channel("mychFile")
            .from(uri.toString())
            .build();

        Publish.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(5));
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Delete.builder()
            .url(REDIS_URI)
            .keys(List.of("mych"))
            .build().run(runContext);
        Delete.builder()
            .url(REDIS_URI)
            .keys(List.of("mychFile"))
            .build().run(runContext);
    }

    URI createTestFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, i);
        }
        return storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
