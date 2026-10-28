package io.kestra.plugin.redis.stream;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AckTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String GROUP = "billing";

    @Test
    void removesTheExactIdFromThePendingList() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_exact_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String firstId = fixture.add(key, Map.of("order", "1"));
            String secondId = fixture.add(key, Map.of("order", "2"));

            ReadGroup.Output read = readAll(runContext, key);
            assertThat(read.getIds(), contains(firstId, secondId));
            assertThat(fixture.pendingIds(key, GROUP), contains(firstId, secondId));

            Ack.Output output = ack(runContext, key, List.of(firstId));

            assertThat(output.getCount(), is(1L));
            assertThat(output.getSize(), is(1));

            // verified on a separate connection with XPENDING: exactly the acknowledged id is gone,
            // and the one that was not acknowledged is still there
            assertThat(fixture.pendingIds(key, GROUP), contains(secondId));
            assertThat(fixture.pendingIds(key, GROUP, "worker_1"), contains(secondId));

            // the entry itself is still in the stream: XACK ends the group's responsibility, it does not delete data
            assertThat(fixture.streamLength(key), is(2L));

            fixture.delete(key);
        }
    }

    @Test
    void failureBeforeTheAcknowledgementLeavesTheEntryPending() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_failure_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String id = fixture.add(key, Map.of("order", "1"));

            ReadGroup.Output read = readAll(runContext, key);
            assertThat(read.getIds(), contains(id));

            // the business step fails, so the flow never reaches Ack
            assertThrows(IllegalStateException.class, () ->
            {
                throw new IllegalStateException("business step failed after the read");
            });

            // the entry is still pending for this consumer, so the work is not lost
            assertThat(fixture.pendingIds(key, GROUP), contains(id));
            assertThat(fixture.pendingIds(key, GROUP, "worker_1"), contains(id));

            // a later run of the same consumer picks it up again with offset 0 and can then acknowledge it
            ReadGroup.Output replay = ReadGroup.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .consumer(Property.ofValue("worker_1"))
                .offset(Property.ofValue("0"))
                .build()
                .run(runContext);

            assertThat(replay.getIds(), contains(id));

            assertThat(ack(runContext, key, replay.getIds()).getCount(), is(1L));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void acknowledgingTwiceIsHarmlessAndCountsOnlyWhatItRemoved() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_twice_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String id = fixture.add(key, Map.of("order", "1"));

            readAll(runContext, key);

            assertThat(ack(runContext, key, List.of(id)).getCount(), is(1L));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            // the retry removes nothing because the entry is no longer pending, and it does not fail
            Ack.Output retry = ack(runContext, key, List.of(id));

            assertThat(retry.getCount(), is(0L));
            assertThat(retry.getSize(), is(1));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void failsOnAShortCountWhenFailedOnMissingIsTrue() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_strict_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String pendingId = fixture.add(key, Map.of("order", "1"));
            readAll(runContext, key);

            // one real pending id and one that was never delivered to this group
            Ack strict = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .ids(Property.ofValue(List.of(pendingId, "1-1")))
                .failedOnMissing(Property.ofValue(true))
                .build();

            IllegalStateException e = assertThrows(IllegalStateException.class, () -> strict.run(runContext));
            assertThat(e.getMessage(), containsString("Only 1 of 2 entries acknowledged"));

            // XACK is not transactional: the id that was pending has still been removed
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void acceptsAShortCountByDefault() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_lenient_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String pendingId = fixture.add(key, Map.of("order", "1"));
            readAll(runContext, key);

            Ack.Output output = ack(runContext, key, List.of(pendingId, "1-1"));

            assertThat(output.getCount(), is(1L));
            assertThat(output.getSize(), is(2));

            fixture.delete(key);
        }
    }

    @Test
    void deduplicatesIdsBeforeCalling() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_dedup_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String id = fixture.add(key, Map.of("order", "1"));
            readAll(runContext, key);

            // the duplicate must not make the requested size 2 and the count 1, which would look like a
            // missing entry and fail a strict acknowledgement for no reason
            Ack.Output output = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .ids(Property.ofValue(List.of(id, id)))
                .failedOnMissing(Property.ofValue(true))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(1L));
            assertThat(output.getSize(), is(1));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void anEmptyIdListIsANoOp() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_empty_" + IdUtils.create();

        Ack.Output output = Ack.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .ids(Property.ofValue(List.of()))
            // even strict mode must not fail on an empty batch, which is what a flow gets after an empty read
            .failedOnMissing(Property.ofValue(true))
            .build()
            .run(runContext);

        assertThat(output.getCount(), is(0L));
        assertThat(output.getSize(), is(0));
    }

    @Test
    void acknowledgesNothingOnAGroupThatDoesNotExist() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String key = "r236_ack_nogroup_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            String id = fixture.add(key, Map.of("order", "1"));

            // XACK on a missing group is not an error in Redis: it acknowledges nothing
            Ack.Output output = ack(runContext, key, List.of(id));
            assertThat(output.getCount(), is(0L));

            // which is exactly what failedOnMissing is for
            Ack strict = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .ids(Property.ofValue(List.of(id)))
                .failedOnMissing(Property.ofValue(true))
                .build();

            IllegalStateException e = assertThrows(IllegalStateException.class, () -> strict.run(runContext));
            assertThat(e.getMessage(), containsString("Only 0 of 1 entries acknowledged"));

            fixture.delete(key);
        }
    }

    @Test
    void rendersEveryPebbleInput() throws Exception {
        String key = "r236_ack_rendered_" + IdUtils.create();

        try (StreamFixture fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            String id = fixture.add(key, Map.of("order", "1"));

            RunContext readContext = runContextFactory.of(Map.of());
            ReadGroup.Output read = readAll(readContext, key);
            assertThat(read.getIds(), contains(id));

            RunContext runContext = runContextFactory.of(
                Map.of(
                    "streamKey", key,
                    "groupName", GROUP,
                    "pendingIds", read.getIds()
                )
            );

            Ack.Output output = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofExpression("{{ streamKey }}"))
                .group(Property.ofExpression("{{ groupName }}"))
                .ids(Property.ofExpression("{{ pendingIds }}"))
                .failedOnMissing(Property.ofExpression("{{ true }}"))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(1L));
            assertThat(output.getSize(), is(1));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    private ReadGroup.Output readAll(RunContext runContext, String key) throws Exception {
        return ReadGroup.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .consumer(Property.ofValue("worker_1"))
            .build()
            .run(runContext);
    }

    private Ack.Output ack(RunContext runContext, String key, List<String> ids) throws Exception {
        return Ack.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .ids(Property.ofValue(ids))
            .build()
            .run(runContext);
    }
}
