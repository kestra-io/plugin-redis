package io.kestra.plugin.redis.list;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.string.Delete;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListPushTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void testListPushAsList() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        ListPush task = ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mykey"))
            .from(Arrays.asList("value1", "value2"))
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @Test
    void testListPushAsFile() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        URI uri = createTestFile();

        ListPush task = ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mykeyFile"))
            .from(uri.toString())
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(5));
    }

    @Test
    void testListPushAsString() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        createTestFile();

        ListPush task = ListPush.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue("mykeyFile"))
            .from("[\"value1\", \"value2\"]")
            .build();

        ListPush.Output runOutput = task.run(runContext);

        assertThat(runOutput.getCount(), is(2));
    }

    @BeforeEach
    void setUp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(List.of("mykey")))
            .build().run(runContext);
        Delete.builder()
            .url(Property.ofValue(REDIS_URI))
            .keys(Property.ofValue(List.of("mykeyFile")))
            .build().run(runContext);
    }

    URI createTestFile() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 5; i++) {
                FileSerde.write(output, i);
            }
            output.flush();
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        // Read back ION from storage and assert count and content
        try (InputStream is = new BufferedInputStream(storageInterface.get(TenantService.MAIN_TENANT, null, uri), FileSerde.BUFFER_SIZE)) {
            List<Object> result = new ArrayList<>();
            FileSerde.read(is, result::add);
            assertThat(result.size(), is(5));
            for (int i = 0; i < 5; i++) {
                assertThat(result.get(i), is(i));
            }
        }

        return uri;
    }
}
