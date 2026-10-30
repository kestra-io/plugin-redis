package io.kestra.plugin.redis.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClaimTest {
    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String GROUP = "billing";

    private static final Duration TEN_MINUTES = Duration.ofMinutes(10);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void aSecondConsumerTakesOverEntriesThatAFailedWorkerNeverAcknowledged() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_recover_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            var secondId = fixture.add(key, Map.of("order", "2"));

            // worker_1 reads both entries and dies before acknowledging them
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), firstId, secondId);
            assertThat(fixture.owners(key, GROUP), is(Map.of(firstId, "worker_1", secondId, "worker_1")));

            var output = claim(key).build().run(runContext);

            assertThat(output.getIds(), contains(firstId, secondId));
            assertThat(output.getCount(), is(2));
            assertThat(output.getMessages().getFirst().getFields(), is(Map.of("order", "1")));
            assertThat(output.getCompleted(), is(true));
            assertThat(output.getCursor(), is("0-0"));
            assertThat(output.getDeletedIds(), is(empty()));

            // verified on a separate connection: the entries now belong to the recovery consumer, still unacknowledged
            assertThat(fixture.owners(key, GROUP), is(Map.of(firstId, "recovery", secondId, "recovery")));

            // and they can be acknowledged with the same IDs, which finishes the recovery
            var ack = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .ids(Property.ofValue(output.getIds()))
                .build()
                .run(runContext);
            assertThat(ack.getCount(), is(2L));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void recentDeliveriesAreNotTakenBeforeTheIdleThreshold() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_recent_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var staleId = fixture.add(key, Map.of("order", "stale"));
            var recentId = fixture.add(key, Map.of("order", "recent"));

            fixture.deliver(key, GROUP, "worker_1", 10);
            // only the first entry has been idle long enough, the second was delivered just now
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), staleId);

            var output = claim(key).build().run(runContext);

            assertThat(output.getIds(), contains(staleId));
            assertThat(fixture.owners(key, GROUP), is(Map.of(staleId, "recovery", recentId, "worker_1")));

            fixture.delete(key);
        }
    }

    @Test
    void takingAnEntryCountsAsADeliveryAndResetsItsIdleTime() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_delivery_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), id);
            var before = fixture.deliveryCount(key, GROUP, id);

            claim(key).build().run(runContext);

            assertThat(fixture.deliveryCount(key, GROUP, id), is(before + 1));

            // the idle time was reset by the take-over, so an immediate second run takes nothing
            var again = claim(key).consumer(Property.ofValue("other_recovery")).build().run(runContext);
            assertThat(again.getCount(), is(0));
            assertThat(fixture.owners(key, GROUP), is(Map.of(id, "recovery")));

            fixture.delete(key);
        }
    }

    @Test
    void anEmptyPageKeepsTheCursorAndTheRunStopsWithinItsBudget() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_budget_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            for (int i = 0; i < 30; i++) {
                ids.add(fixture.add(key, Map.of("order", String.valueOf(i))));
            }
            fixture.deliver(key, GROUP, "worker_1", 100);

            // Redis inspects at most COUNT * 10 pending entries per call, so with COUNT 1 and nothing idle long
            // enough, every page is empty and the cursor advances by ten entries without reaching the end
            var first = claim(key).count(Property.ofValue(1)).maxPages(Property.ofValue(1)).build().run(runContext);
            assertThat(first.getCount(), is(0));
            assertThat(first.getPages(), is(1));
            assertThat(first.getCompleted(), is(false));
            assertThat(first.getCursor(), is(ids.get(10)));

            // a budget of two steps stops at the second cursor and keeps it, it does not loop on
            var second = claim(key).count(Property.ofValue(1)).maxPages(Property.ofValue(2)).build().run(runContext);
            assertThat(second.getCount(), is(0));
            assertThat(second.getPages(), is(2));
            assertThat(second.getCompleted(), is(false));
            assertThat(second.getCursor(), is(ids.get(20)));

            // a later run continues from the kept cursor and reaches the end of the list
            var rest = claim(key).count(Property.ofValue(1)).maxPages(Property.ofValue(5))
                .startId(Property.ofValue(second.getCursor())).build().run(runContext);
            assertThat(rest.getPages(), is(1));
            assertThat(rest.getCompleted(), is(true));
            assertThat(rest.getCursor(), is("0-0"));

            // nothing was idle enough, so nothing changed hands
            assertThat(fixture.owners(key, GROUP).values().stream().distinct().toList(), is(List.of("worker_1")));

            fixture.delete(key);
        }
    }

    @Test
    void theScanEndsWhenTheWholePendingListWasVisited() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_complete_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            for (int i = 0; i < 7; i++) {
                ids.add(fixture.add(key, Map.of("order", String.valueOf(i))));
            }
            fixture.deliver(key, GROUP, "worker_1", 100);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), ids.toArray(String[]::new));

            // small pages force several steps, and the entries of all of them are collected
            var output = claim(key).count(Property.ofValue(3)).maxPages(Property.ofValue(50)).build().run(runContext);

            assertThat(output.getIds(), is(ids));
            assertThat(output.getPages(), greaterThan(1));
            assertThat(output.getCompleted(), is(true));
            assertThat(output.getCursor(), is("0-0"));

            fixture.delete(key);
        }
    }

    @Test
    void maxRecordsCapsHowManyEntriesOneRunTakes() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_max_records_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            for (int i = 0; i < 6; i++) {
                ids.add(fixture.add(key, Map.of("order", String.valueOf(i))));
            }
            fixture.deliver(key, GROUP, "worker_1", 100);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), ids.toArray(String[]::new));

            var output = claim(key).count(Property.ofValue(10)).maxRecords(Property.ofValue(4)).build().run(runContext);

            assertThat(output.getIds(), is(ids.subList(0, 4)));
            assertThat(output.getCompleted(), is(false));
            // the entries beyond the budget were not touched, so they still belong to the failed worker
            var owners = fixture.owners(key, GROUP);
            assertThat(owners.get(ids.get(4)), is("worker_1"));
            assertThat(owners.get(ids.get(5)), is("worker_1"));
            assertThat(output.getCursor(), is(ids.get(4)));

            fixture.delete(key);
        }
    }

    @Test
    void entriesTrimmedWhilePendingAreReportedAndNotReturned() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_trimmed_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var keptId = fixture.add(key, Map.of("order", "kept"));
            var trimmedId = fixture.add(key, Map.of("order", "trimmed"));
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), keptId, trimmedId);
            // the entry is deleted from the stream while it is still in the pending list
            fixture.remove(key, trimmedId);

            var output = claim(key).build().run(runContext);

            assertThat(output.getIds(), contains(keptId));
            assertThat(output.getDeletedIds(), contains(trimmedId));
            assertThat(output.getDeletedCount(), is(1L));
            // Redis dropped the dangling entry from the pending list, so there is nothing left to acknowledge for it
            assertThat(fixture.owners(key, GROUP).keySet(), contains(keptId));

            fixture.delete(key);
        }
    }

    @Test
    void theListOfTrimmedIdsIsCappedButTheCountIsNot() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_cap_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            for (var i = 0; i < 1100; i++) {
                ids.add(fixture.add(key, Map.of("order", String.valueOf(i))));
            }
            fixture.deliver(key, GROUP, "worker_1", 1100);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), ids.toArray(String[]::new));
            fixture.remove(key, ids.toArray(String[]::new));

            var output = claim(key).count(Property.ofValue(1000)).maxRecords(Property.ofValue(10000)).maxPages(Property.ofValue(5)).fetchType(Property.ofValue(FetchType.NONE)).build().run(runContext);

            assertThat(output.getDeletedCount(), is(1100L));
            assertThat(output.getDeletedIds(), hasSize(1000));

            fixture.delete(key);
        }
    }

    @Test
    void anEmptyPendingListIsAValidResult() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_empty_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");

            var output = claim(key).build().run(runContext);

            assertThat(output.getCount(), is(0));
            assertThat(output.getIds(), is(empty()));
            assertThat(output.getMessages(), is(empty()));
            assertThat(output.getCompleted(), is(true));
            assertThat(output.getCursor(), is("0-0"));

            var one = claim(key).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build().run(runContext);
            assertThat(one.getMessage(), is(nullValue()));

            fixture.delete(key);
        }
    }

    @Test
    void fetchOneTakesASingleEntryAndLeavesTheOthersWithTheirOwner() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_fetch_one_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            var secondId = fixture.add(key, Map.of("order", "2"));
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), firstId, secondId);

            var output = claim(key).fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .maxRecords(Property.ofValue(50)).count(Property.ofValue(50)).build().run(runContext);

            assertThat(output.getCount(), is(1));
            assertThat(output.getMessage().getId(), is(firstId));
            assertThat(output.getMessages(), is(nullValue()));
            // no entry was taken without its ID reaching the flow
            assertThat(fixture.owners(key, GROUP), is(Map.of(firstId, "recovery", secondId, "worker_1")));

            fixture.delete(key);
        }
    }

    @Test
    void noneAndStoreKeepTheIdsAndTheCursor() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_modes_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1", "payload", "{\"a\": 1}"));
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), firstId);

            var stored = claim(key).fetchType(Property.ofValue(FetchType.STORE)).build().run(runContext);
            assertThat(stored.getUri(), is(notNullValue()));
            assertThat(stored.getIds(), contains(firstId));
            assertThat(stored.getMessages(), is(nullValue()));

            try (var reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(stored.getUri())))) {
                var rows = FileSerde.readAll(reader).collectList().block();
                assertThat(rows.size(), is(1));
                assertThat(((Map<?, ?>) rows.getFirst()).get("id"), is(firstId));
                assertThat(((Map<?, ?>) ((Map<?, ?>) rows.getFirst()).get("fields")).get("payload"), is("{\"a\": 1}"));
            }

            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), firstId);
            var none = claim(key).fetchType(Property.ofValue(FetchType.NONE)).build().run(runContext);
            assertThat(none.getMessages(), is(nullValue()));
            assertThat(none.getUri(), is(nullValue()));
            assertThat(none.getIds(), contains(firstId));
            assertThat(none.getCursor(), is("0-0"));

            fixture.delete(key);
        }
    }

    @Test
    void aMissingGroupFailsWithTheRedisError() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_nogroup_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.add(key, Map.of("order", "1"));

            var failure = assertThrows(Exception.class, () -> claim(key).build().run(runContext));
            assertThat(failure.getMessage(), containsString("NOGROUP"));

            fixture.delete(key);
        }
    }

    @Test
    void theIdleThresholdIsRequiredAndMustBePositive() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_idle_" + IdUtils.create();

        var missing = claim(key).minIdleTime(null).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> missing.run(runContext)).getMessage(),
            containsString("`minIdleTime` is required")
        );

        for (var invalid : new Duration[] { Duration.ZERO, Duration.ofSeconds(-5), Duration.ofNanos(500) }) {
            var task = claim(key).minIdleTime(Property.ofValue(invalid)).build();
            assertThat(
                assertThrows(IllegalArgumentException.class, () -> task.run(runContext)).getMessage(),
                containsString("minIdleTime must be at least 1 millisecond")
            );
        }
    }

    @Test
    void invalidInputsAreRejectedBeforeAnyCall() {
        var runContext = runContextFactory.of(Map.of());
        var key = "r236_claim_invalid_" + IdUtils.create();

        for (var invalid : new String[] { "abc", "1-", "-5", "1-2-3", "" }) {
            var task = claim(key).startId(Property.ofValue(invalid)).build();
            assertThat(
                assertThrows(IllegalArgumentException.class, () -> task.run(runContext)).getMessage(),
                containsString("startId must be `0-0` or a stream ID")
            );
        }

        var missingGroup = claim(key).group(null).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> missingGroup.run(runContext)).getMessage(),
            containsString("`group` rendered to an empty value")
        );
    }

    @Test
    void boundsAreEnforcedForLiteralsWrittenInFlowYaml() {
        // the annotation on the Property type argument does not bound a YAML literal, so the run has to
        var cases = Map.of(
            "count: 0", "`count` must be between 1 and 10000 but was 0",
            "count: 10001", "`count` must be between 1 and 10000 but was 10001",
            "maxRecords: 0", "`maxRecords` must be between 1 and 10000 but was 0",
            "maxRecords: 99999", "`maxRecords` must be between 1 and 10000 but was 99999",
            "maxPages: 0", "`maxPages` must be between 1 and 1000 but was 0",
            "maxPages: 1001", "`maxPages` must be between 1 and 1000 but was 1001"
        );

        cases.forEach((property, expected) ->
        {
            var flow = YamlParser.parse("""
                id: f
                namespace: n
                tasks:
                  - id: c
                    type: io.kestra.plugin.redis.stream.Claim
                    url: redis://:redis@localhost:6379/0
                    key: k
                    group: g
                    consumer: c
                    minIdleTime: PT10M
                    %s
                """.formatted(property), Flow.class);
            var task = (Claim) flow.getTasks().getFirst();

            var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of(Map.of())));
            assertThat(property, failure.getMessage(), is(expected));
        });
    }

    @Test
    void renderedPropertiesAreResolvedAtRuntime() throws Exception {
        var runContext = runContextFactory.of(Map.of(
            "streamKey", "r236_claim_rendered_" + IdUtils.create(),
            "groupName", GROUP,
            "consumerName", "recovery_rendered",
            "idle", "PT10M",
            "budget", 2
        ));

        try (var fixture = new StreamFixture(REDIS_URI)) {
            var key = (String) runContext.getVariables().get("streamKey");
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            for (int i = 0; i < 3; i++) {
                ids.add(fixture.add(key, Map.of("order", String.valueOf(i))));
            }
            fixture.deliver(key, GROUP, "worker_1", 10);
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), ids.toArray(String[]::new));

            var output = Claim.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofExpression("{{ streamKey }}"))
                .group(Property.ofExpression("{{ groupName }}"))
                .consumer(Property.ofExpression("{{ consumerName }}"))
                .minIdleTime(Property.ofExpression("{{ idle }}"))
                .maxRecords(Property.ofExpression("{{ budget }}"))
                .build()
                .run(runContext);

            assertThat(output.getStream(), is(key));
            assertThat(output.getConsumer(), is("recovery_rendered"));
            assertThat(output.getIds(), is(ids.subList(0, 2)));
            assertThat(fixture.owners(key, GROUP).get(ids.getFirst()), is("recovery_rendered"));

            fixture.delete(key);
        }
    }

    private Claim.ClaimBuilder<?, ?> claim(String key) {
        return Claim.builder()
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .consumer(Property.ofValue("recovery"))
            .minIdleTime(Property.ofValue(TEN_MINUTES));
    }
}
