package io.kestra.plugin.redis.stream;

import java.io.BufferedInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadGroupTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String GROUP = "billing";

    @Test
    void deliversExactIdsAndMakesThemPendingForTheConsumer() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_basic_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            var secondId = fixture.add(key, Map.of("order", "2"));

            var output = read(runContext, key).build().run(runContext);

            assertThat(output.getStream(), is(key));
            assertThat(output.getGroup(), is(GROUP));
            assertThat(output.getConsumer(), is("worker_1"));
            assertThat(output.getCount(), is(2));
            assertThat(output.getIds(), contains(firstId, secondId));
            assertThat(output.getMessages(), hasSize(2));
            assertThat(output.getMessages().getFirst().getStream(), is(key));
            assertThat(output.getMessages().getFirst().getId(), is(firstId));
            assertThat(output.getMessages().getFirst().getFields(), is(Map.of("order", "1")));
            assertThat(output.getMessage(), is(nullValue()));
            assertThat(output.getUri(), is(nullValue()));

            // independently verified through XPENDING on a separate connection
            assertThat(fixture.pendingIds(key, GROUP), contains(firstId, secondId));
            assertThat(fixture.pendingIds(key, GROUP, "worker_1"), contains(firstId, secondId));

            fixture.delete(key);
        }
    }

    @Test
    void preservesRawFieldNamesAndValues() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_raw_" + IdUtils.create();

        // values a serde would damage: JSON text, numeric-looking strings, empty, whitespace, non-ASCII
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("payload", "{\"amount\": 10.50, \"nested\": {\"ok\": true}}");
        fields.put("amount", "0010.50");
        fields.put("empty", "");
        fields.put("spaced", "  two  spaces  ");
        fields.put("unicode", "héllo wörld \u00e9\u00e8 \u4e2d\u6587 \ud83d\ude80");
        fields.put("newline", "line1\nline2");
        fields.put("quote", "he said \"hi\" and 'bye'");

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, fields);

            var output = read(runContext, key).build().run(runContext);

            assertThat(output.getIds(), contains(id));
            assertThat(output.getMessages().getFirst().getFields(), is(fields));

            fixture.delete(key);
        }
    }

    @Test
    void returnsAnEmptyBatchWhenTheGroupHasNothingToDeliver() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_empty_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            var output = read(runContext, key).build().run(runContext);

            assertThat(output.getCount(), is(0));
            assertThat(output.getIds(), is(empty()));
            assertThat(output.getMessages(), is(empty()));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void blockedReadReturnsEmptyAfterItsOwnTimeoutInsteadOfWaitingForever() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_block_timeout_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            var startedAt = System.nanoTime();
            var output = assertTimeoutPreemptively(
                Duration.ofSeconds(20),
                () -> read(runContext, key).blockDuration(Property.ofValue(Duration.ofSeconds(2))).build().run(runContext)
            );
            var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(output.getCount(), is(0));
            assertThat(output.getIds(), is(empty()));
            // it really waited (so BLOCK reached the server) and it really stopped (so the bound holds)
            assertThat(elapsed.toMillis(), greaterThanOrEqualTo(1_500L));
            assertThat(elapsed.toMillis(), lessThan(15_000L));

            fixture.delete(key);
        }
    }

    @Test
    void blockedReadIsUnblockedByKillAndAcknowledgesNothing() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_cancel_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            // a block far longer than the test is willing to wait: only kill() can end this read early
            var task = read(runContext, key)
                .blockDuration(Property.ofValue(Duration.ofMinutes(5)))
                .build();

            var reading = CompletableFuture.supplyAsync(() ->
            {
                try {
                    return task.run(runContext);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });

            // give the command time to reach the server and actually block
            Thread.sleep(2_000);
            assertThat(reading.isDone(), is(false));

            var killedAt = System.nanoTime();
            task.kill();
            var killDuration = Duration.ofNanos(System.nanoTime() - killedAt);

            var thrown = assertThrows(ExecutionException.class, () -> reading.get(20, TimeUnit.SECONDS));
            assertThat(getRootCause(thrown), instanceOf(CancellationException.class));

            // kill() only fails the in-flight command, it never waits for the client shutdown
            assertThat(killDuration.toMillis(), lessThan(1_000L));

            // a cancelled read delivered nothing, so nothing is pending and nothing was acknowledged
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void failsWithAClearErrorWhenTheGroupDoesNotExist() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_nogroup_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            // the stream exists, only the group is missing
            fixture.add(key, Map.of("order", "1"));

            var e = assertThrows(
                RedisCommandExecutionException.class,
                () -> read(runContext, key).build().run(runContext)
            );

            assertThat(e.getMessage(), containsString("NOGROUP"));

            fixture.delete(key);
        }
    }

    @Test
    void failsWhenTheStreamDoesNotExistEither() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_nostream_" + IdUtils.create();

        var e = assertThrows(
            RedisCommandExecutionException.class,
            () -> read(runContext, key).build().run(runContext)
        );

        assertThat(e.getMessage(), containsString("NOGROUP"));
    }

    @Test
    void offsetZeroRereadsThisConsumerOwnUnacknowledgedEntries() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_own_pending_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));

            assertThat(read(runContext, key).build().run(runContext).getIds(), contains(id));

            // `>` has nothing left: the entry was already delivered to this group
            assertThat(read(runContext, key).build().run(runContext).getIds(), is(empty()));

            // `0` hands this consumer its own unacknowledged entry again, with the same id and fields
            var replay = read(runContext, key)
                .offset(Property.ofValue("0"))
                .build()
                .run(runContext);

            assertThat(replay.getIds(), contains(id));
            assertThat(replay.getMessages().getFirst().getFields(), is(Map.of("order", "1")));

            // a different consumer of the same group does not see another consumer's pending entry
            var otherConsumer = read(runContext, key)
                .consumer(Property.ofValue("worker_2"))
                .offset(Property.ofValue("0"))
                .build()
                .run(runContext);

            assertThat(otherConsumer.getIds(), is(empty()));
            assertThat(fixture.pendingIds(key, GROUP, "worker_1"), contains(id));
            assertThat(fixture.pendingIds(key, GROUP, "worker_2"), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void countBoundsTheBatch() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_count_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            fixture.add(key, Map.of("order", "2"));
            fixture.add(key, Map.of("order", "3"));

            var output = read(runContext, key)
                .count(Property.ofValue(1))
                .build()
                .run(runContext);

            assertThat(output.getIds(), contains(firstId));
            // the entries beyond the bound were not delivered, so they are not pending
            assertThat(fixture.pendingIds(key, GROUP), contains(firstId));

            fixture.delete(key);
        }
    }

    @Test
    void fetchOneReturnsASingleEntryAndDoesNotDeliverAnyOther() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_fetch_one_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            fixture.add(key, Map.of("order", "2"));

            var output = read(runContext, key)
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .count(Property.ofValue(50))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(1));
            assertThat(output.getMessage(), is(notNullValue()));
            assertThat(output.getMessage().getId(), is(firstId));
            assertThat(output.getMessage().getFields(), is(Map.of("order", "1")));
            assertThat(output.getMessages(), is(nullValue()));
            assertThat(output.getIds(), contains(firstId));

            // count was lowered to 1, so no entry is stranded in the pending list without its id reaching the flow
            assertThat(fixture.pendingIds(key, GROUP), contains(firstId));

            fixture.delete(key);
        }
    }

    @Test
    void fetchOneReturnsNoEntryOnAnEmptyRead() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_fetch_one_empty_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            var output = read(runContext, key)
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(0));
            assertThat(output.getMessage(), is(nullValue()));
            assertThat(output.getIds(), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void storeWritesTheBatchToInternalStorageWithIdsAndRawFields() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_store_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1", "payload", "{\"a\": 1}"));
            var secondId = fixture.add(key, Map.of("order", "2"));

            var output = read(runContext, key)
                .fetchType(Property.ofValue(FetchType.STORE))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(2));
            assertThat(output.getIds(), contains(firstId, secondId));
            assertThat(output.getMessages(), is(nullValue()));
            assertThat(output.getUri(), is(notNullValue()));

            var rows = readStored(runContext, output);

            assertThat(rows, hasSize(2));
            assertThat(rows.getFirst().get("id"), is(firstId));
            assertThat(rows.getFirst().get("stream"), is(key));
            assertThat(rows.getFirst().get("fields"), is(Map.of("order", "1", "payload", "{\"a\": 1}")));
            assertThat(rows.get(1).get("id"), is(secondId));

            assertThat(fixture.pendingIds(key, GROUP), contains(firstId, secondId));

            fixture.delete(key);
        }
    }

    @Test
    void noneReturnsIdsWithoutBodiesSoTheBatchStaysAcknowledgeable() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_none_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));

            var output = read(runContext, key)
                .fetchType(Property.ofValue(FetchType.NONE))
                .build()
                .run(runContext);

            assertThat(output.getCount(), is(1));
            assertThat(output.getIds(), contains(id));
            assertThat(output.getMessages(), is(nullValue()));
            assertThat(output.getMessage(), is(nullValue()));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(fixture.pendingIds(key, GROUP), contains(id));

            fixture.delete(key);
        }
    }

    @Test
    void noAckSkipsThePendingListEntirely() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_noack_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));

            var output = read(runContext, key)
                .noAck(Property.ofValue(true))
                .build()
                .run(runContext);

            assertThat(output.getIds(), contains(id));
            // NOACK means Redis never recorded the delivery, so there is nothing to acknowledge later
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void rejectsACountOutsideTheAllowedRange() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_bad_count_" + IdUtils.create();

        for (int invalid : new int[] { 0, -1, 10_001 }) {
            var task = read(runContext, key).id("read").type(ReadGroup.class.getName()).count(Property.ofValue(invalid)).build();

            assertThat("count " + invalid, modelValidator.isValid(task).isPresent(), is(true));
        }

        for (int valid : new int[] { 1, 10_000 }) {
            var task = read(runContext, key).id("read").type(ReadGroup.class.getName()).count(Property.ofValue(valid)).build();

            assertThat("count " + valid, modelValidator.isValid(task).isPresent(), is(false));
        }
    }

    @Test
    void rejectsABlockDurationThatWouldWaitForever() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_bad_block_" + IdUtils.create();

        var zero = read(runContext, key).blockDuration(Property.ofValue(Duration.ZERO)).build();
        var zeroError = assertThrows(IllegalArgumentException.class, () -> zero.run(runContext));
        assertThat(zeroError.getMessage(), containsString("blockDuration must be strictly positive"));

        var negative = read(runContext, key).blockDuration(Property.ofValue(Duration.ofSeconds(-1))).build();
        var negativeError = assertThrows(IllegalArgumentException.class, () -> negative.run(runContext));
        assertThat(negativeError.getMessage(), containsString("blockDuration must be strictly positive"));
    }

    @Test
    void rejectsABlockDurationAboveOneHour() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_long_bad_block_" + IdUtils.create();

        var tooLong = read(runContext, key).blockDuration(Property.ofValue(Duration.ofDays(1))).build();
        var e = assertThrows(IllegalArgumentException.class, () -> tooLong.run(runContext));
        assertThat(e.getMessage(), containsString("blockDuration must be at most PT1H"));
    }

    @Test
    void namesTheMissingRequiredProperty() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_empty_group_" + IdUtils.create();

        var task = read(runContext, key).group(null).build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("`group` rendered to an empty value"));
    }

    @Test
    void blockOutlastingTheClientCommandTimeoutStillWaitsInsteadOfFailing() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_long_block_" + IdUtils.create();

        // a 3s command timeout mimics a default Lettuce client reading with a longer BLOCK
        var shortTimeoutUri = REDIS_URI + "?timeout=3s";

        try (var fixture = new StreamFixture(shortTimeoutUri)) {
            fixture.createGroup(key, GROUP, "0");

            // control: a client whose command timeout is below the block duration aborts the read instead of waiting
            assertThrows(
                RedisCommandTimeoutException.class,
                () -> fixture.blockingRead(key, GROUP, "control_worker", Duration.ofSeconds(8))
            );

            // the task raises the command timeout above its block, so it returns an empty batch instead of failing
            var startedAt = System.nanoTime();
            var output = ReadGroup.builder()
                .url(Property.ofValue(shortTimeoutUri))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .consumer(Property.ofValue("worker_1"))
                .blockDuration(Property.ofValue(Duration.ofSeconds(8)))
                .build()
                .run(runContext);
            var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(output.getCount(), is(0));
            assertThat(elapsed.toMillis(), greaterThan(7_000L));

            fixture.delete(key);
        }
    }

    @Test
    void blockedReadDeliversAnEntryThatArrivesWhileItWaits() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_read_late_entry_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            var task = read(runContext, key)
                .blockDuration(Property.ofValue(Duration.ofSeconds(30)))
                .build();

            var reading = CompletableFuture.supplyAsync(() ->
            {
                try {
                    return task.run(runContext);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });

            Thread.sleep(2_000);
            var id = fixture.add(key, Map.of("order", "late"));

            var output = reading.get(30, TimeUnit.SECONDS);

            assertThat(output.getIds(), contains(id));
            assertThat(fixture.pendingIds(key, GROUP), contains(id));

            fixture.delete(key);
        }
    }

    @Test
    void rendersEveryPebbleInput() throws Exception {
        var runContext = runContextFactory.of(
            Map.of(
                "streamKey", "r236_read_rendered_" + IdUtils.create(),
                "groupName", GROUP,
                "consumerName", "worker_rendered",
                "readOffset", ">",
                "readCount", 7
            )
        );

        var key = (String) runContext.getVariables().get("streamKey");

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));

            var output = ReadGroup.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofExpression("{{ streamKey }}"))
                .group(Property.ofExpression("{{ groupName }}"))
                .consumer(Property.ofExpression("{{ consumerName }}"))
                .offset(Property.ofExpression("{{ readOffset }}"))
                .count(Property.ofExpression("{{ readCount }}"))
                .build()
                .run(runContext);

            assertThat(output.getStream(), is(key));
            assertThat(output.getGroup(), is(GROUP));
            assertThat(output.getConsumer(), is("worker_rendered"));
            assertThat(output.getIds(), contains(id));
            assertThat(fixture.pendingIds(key, GROUP, "worker_rendered"), contains(id));

            fixture.delete(key);
        }
    }

    private ReadGroup.ReadGroupBuilder<?, ?> read(RunContext runContext, String key) {
        return ReadGroup.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .consumer(Property.ofValue("worker_1"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readStored(RunContext runContext, ReadGroup.Output output) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();

        try (var stream = new BufferedInputStream(runContext.storage().getFile(output.getUri()), FileSerde.BUFFER_SIZE)) {
            FileSerde.read(stream, row -> rows.add((Map<String, Object>) row));
        }

        return rows;
    }

    private static Throwable getRootCause(Throwable throwable) {
        var cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }

        return cause;
    }
}
