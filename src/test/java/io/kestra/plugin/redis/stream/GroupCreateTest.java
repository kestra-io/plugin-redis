package io.kestra.plugin.redis.stream;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandExecutionException;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GroupCreateTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Test
    void createsTheGroupAndIsIdempotentOnTheSecondRun() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_group_create_" + IdUtils.create();

        var task = GroupCreate.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue("billing"))
            .build();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            var first = task.run(runContext);

            assertThat(first.getCreated(), is(true));
            assertThat(first.getKey(), is(key));
            assertThat(first.getGroup(), is("billing"));
            assertThat(first.getStartId(), is("$"));
            // makeStream defaults to true, so MKSTREAM created the key even though nothing was added to it
            assertThat(fixture.streamLength(key), is(0L));

            // the second run hits BUSYGROUP; ignoreExisting defaults to true, so it is reported, not thrown
            var second = task.run(runContext);

            assertThat(second.getCreated(), is(false));
            assertThat(second.getGroup(), is("billing"));

            // the existing group is untouched: an entry added after the first create is still deliverable
            var id = fixture.add(key, Map.of("field", "value"));
            assertThat(fixture.pendingIds(key, "billing"), is(empty()));

            var read = ReadGroup.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue("billing"))
                .consumer(Property.ofValue("worker"))
                .build()
                .run(runContext);

            assertThat(read.getIds(), contains(id));

            fixture.delete(key);
        }
    }

    @Test
    void failsOnAnExistingGroupWhenIgnoreExistingIsFalse() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_group_busy_" + IdUtils.create();

        var strict = GroupCreate.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue("billing"))
            .ignoreExisting(Property.ofValue(false))
            .build();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            assertThat(strict.run(runContext).getCreated(), is(true));

            var e = assertThrows(RedisBusyException.class, () -> strict.run(runContext));
            assertThat(e.getMessage(), containsString("BUSYGROUP"));

            fixture.delete(key);
        }
    }

    @Test
    void failsOnAMissingStreamWhenMakeStreamIsFalse() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_group_nostream_" + IdUtils.create();

        var task = GroupCreate.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue("billing"))
            .makeStream(Property.ofValue(false))
            .build();

        var e = assertThrows(RedisCommandExecutionException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("requires the key to exist"));
    }

    @Test
    void startIdDollarSkipsTheBacklogWhileZeroReplaysIt() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_group_startid_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            var backlogId = fixture.add(key, Map.of("order", "1"));

            GroupCreate.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue("from_now"))
                .startId(Property.ofValue("$"))
                .build()
                .run(runContext);

            GroupCreate.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue("from_start"))
                .startId(Property.ofValue("0"))
                .build()
                .run(runContext);

            // `$` treats the existing entry as already handled
            var fromNow = readOnce(runContext, key, "from_now");
            assertThat(fromNow.getCount(), is(0));
            assertThat(fromNow.getIds(), is(empty()));

            // `0` replays the retained backlog
            var fromStart = readOnce(runContext, key, "from_start");
            assertThat(fromStart.getIds(), contains(backlogId));

            // and an entry added afterwards reaches the `$` group
            var laterId = fixture.add(key, Map.of("order", "2"));
            assertThat(readOnce(runContext, key, "from_now").getIds(), contains(laterId));

            fixture.delete(key);
        }
    }

    @Test
    void startsTheGroupAfterAnExplicitStreamId() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_group_explicit_id_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            var firstId = fixture.add(key, Map.of("order", "1"));
            var secondId = fixture.add(key, Map.of("order", "2"));

            var output = GroupCreate.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue("after_first"))
                .startId(Property.ofValue(firstId))
                .build()
                .run(runContext);

            assertThat(output.getStartId(), is(firstId));

            // only the entry after the explicit id is delivered
            assertThat(readOnce(runContext, key, "after_first").getIds(), contains(secondId));

            fixture.delete(key);
        }
    }

    private ReadGroup.Output readOnce(RunContext runContext, String key, String group) throws Exception {
        return ReadGroup.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(group))
            .consumer(Property.ofValue("worker"))
            .build()
            .run(runContext);
    }
}
