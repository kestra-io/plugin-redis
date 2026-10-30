package io.kestra.plugin.redis.stream;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.plugin.core.debug.Return;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.RunContextInitializer;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(startRunner = true, startScheduler = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TriggerTest {
    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    private static final String GROUP = "trigger_group";

    private static final String CONSUMER = "trigger_consumer";

    private static final List<String> FLOW_KEYS = List.of(
        "stream_trigger_key", "stream_trigger_store_key", "stream_trigger_one_key", "stream_trigger_flow_key"
    );

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private RunContextInitializer runContextInitializer;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    private final List<String> entryIds = new ArrayList<>();

    @BeforeEach
    void setUp() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            // the flows of the annotated tests read fixed keys, so each starts from a clean stream with three entries
            entryIds.clear();
            for (var key : FLOW_KEYS.subList(0, 3)) {
                fixture.delete(key);
                fixture.createGroup(key, GROUP, "0");
                for (int i = 1; i <= 3; i++) {
                    var id = fixture.add(key, Map.of("order", String.valueOf(i), "payload", "{\"n\": " + i + "}"));
                    if (key.equals(FLOW_KEYS.getFirst())) {
                        entryIds.add(id);
                    }
                }
            }
        }
    }

    @AfterEach
    void tearDown() {
        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.delete(FLOW_KEYS.toArray(String[]::new));
        }
    }

    @Test
    void theConnectionUrlIsNotPrintedWithTheTrigger() {
        var trigger = Trigger.builder()
            .id("stream_trigger")
            .type(Trigger.class.getName())
            .url(Property.ofValue("redis://:topsecret@localhost:6379/0"))
            .build();

        assertThat(trigger.toString(), not(containsString("topsecret")));
    }

    @Test
    @EvaluateTrigger(flow = "flows/stream-trigger.yaml", triggerId = "watch")
    void startsAnExecutionWithTheExactIdsAndLeavesThemPending(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));

        var variables = optionalExecution.get().getTrigger().getVariables();
        assertThat(variables.get("count"), is(3));
        assertThat(variables.get("ids"), is(entryIds));
        assertThat(variables.get("stream"), is(FLOW_KEYS.getFirst()));
        assertThat(variables.get("consumer"), is(CONSUMER));

        @SuppressWarnings("unchecked")
        var messages = (List<Map<String, Object>>) variables.get("messages");
        assertThat(messages.size(), is(3));
        assertThat(messages.getFirst().get("id"), is(entryIds.getFirst()));
        assertThat(((Map<?, ?>) messages.getFirst().get("fields")).get("payload"), is("{\"n\": 1}"));

        // verified on a separate connection: the trigger acknowledged nothing, the entries wait for the flow's Ack
        try (var fixture = new StreamFixture(REDIS_URI)) {
            assertThat(fixture.owners(FLOW_KEYS.getFirst(), GROUP), is(Map.of(
                entryIds.get(0), CONSUMER, entryIds.get(1), CONSUMER, entryIds.get(2), CONSUMER
            )));
        }
    }

    @Test
    @EvaluateTrigger(flow = "flows/stream-trigger-store.yaml", triggerId = "watch")
    void storeKeepsThePayloadOutOfTheExecutionButNotTheIds(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));

        var variables = optionalExecution.get().getTrigger().getVariables();
        assertThat(variables.get("uri"), is(notNullValue()));
        assertThat(variables.get("messages"), is(nullValue()));
        assertThat(((List<?>) variables.get("ids")).size(), is(3));
        assertThat(variables.get("count"), is(3));
    }

    @Test
    @EvaluateTrigger(flow = "flows/stream-trigger-one.yaml", triggerId = "watch")
    void fetchOneDeliversASingleEntryAndLeavesTheOthersUntouched(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));

        var variables = optionalExecution.get().getTrigger().getVariables();
        assertThat(variables.get("count"), is(1));
        assertThat(variables.get("messages"), is(nullValue()));
        assertThat(((Map<?, ?>) variables.get("message")).get("id"), is(notNullValue()));

        // count 50 was lowered to 1, so the other two entries were never delivered and are not pending
        try (var fixture = new StreamFixture(REDIS_URI)) {
            assertThat(fixture.owners(FLOW_KEYS.get(2), GROUP).size(), is(1));
        }
    }

    @Test
    void anEmptyStreamStartsNothingAndTheNextPollDoesNotRedeliver() throws Exception {
        var key = "r236_trigger_poll_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var trigger = trigger(key).build();

            assertThat(evaluate(trigger).isPresent(), is(false));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            var id = fixture.add(key, Map.of("order", "1"));
            var first = evaluate(trigger);
            assertThat(first.isPresent(), is(true));
            assertThat(first.get().getTrigger().getVariables().get("ids"), is(List.of(id)));

            // the entry is pending now, and the trigger only asks for new entries, so a second poll finds nothing
            assertThat(evaluate(trigger).isPresent(), is(false));
            assertThat(fixture.pendingIds(key, GROUP), is(List.of(id)));

            fixture.delete(key);
        }
    }

    @Test
    void aFailedExecutionLeavesEntriesPendingAndAClaimHandsThemToASecondConsumer() throws Exception {
        var key = "r236_trigger_recover_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var firstId = fixture.add(key, Map.of("order", "1"));
            var secondId = fixture.add(key, Map.of("order", "2"));
            var trigger = trigger(key).build();

            // the trigger delivers the batch, then the execution it started fails before reaching Ack
            var started = evaluate(trigger);
            assertThat(started.isPresent(), is(true));
            assertThat(fixture.owners(key, GROUP), is(Map.of(firstId, CONSUMER, secondId, CONSUMER)));

            // nothing re-delivers them to the trigger, they only wait to be recovered
            assertThat(evaluate(trigger).isPresent(), is(false));
            fixture.makeIdle(key, GROUP, Duration.ofMinutes(30), firstId, secondId);

            var recovered = Claim.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .consumer(Property.ofValue("recovery"))
                .minIdleTime(Property.ofValue(Duration.ofMinutes(10)))
                .build()
                .run(runContextFactory.of(Map.of()));
            assertThat(recovered.getIds(), is(List.of(firstId, secondId)));
            assertThat(fixture.owners(key, GROUP), is(Map.of(firstId, "recovery", secondId, "recovery")));

            // the second consumer finishes the work
            var ack = Ack.builder()
                .url(Property.ofValue(REDIS_URI))
                .key(Property.ofValue(key))
                .group(Property.ofValue(GROUP))
                .ids(Property.ofValue(recovered.getIds()))
                .build()
                .run(runContextFactory.of(Map.of()));
            assertThat(ack.getCount(), is(2L));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            fixture.delete(key);
        }
    }

    @Test
    void aMissingGroupFailsTheEvaluationWithTheRedisError() throws Exception {
        var key = "r236_trigger_nogroup_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.add(key, Map.of("order", "1"));
            var trigger = trigger(key).build();

            var failure = assertThrows(Exception.class, () -> evaluate(trigger));
            assertThat(failure.getMessage(), containsString("NOGROUP"));

            fixture.delete(key);
        }
    }

    @Test
    void countOutsideTheRangeIsRejectedForALiteralInFlowYaml() {
        for (var invalid : new String[] { "0", "1001", "5000" }) {
            var flow = YamlParser.parse("""
                id: f
                namespace: n
                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.stream.Trigger
                    url: redis://:redis@localhost:6379/0
                    key: k
                    group: g
                    consumer: c
                    count: %s
                tasks:
                  - id: t
                    type: io.kestra.plugin.core.debug.Return
                    format: x
                """.formatted(invalid), Flow.class);
            var trigger = (Trigger) flow.getTriggers().getFirst();

            var failure = assertThrows(IllegalArgumentException.class, () -> evaluateOnly(trigger));
            assertThat(failure.getMessage(), is("`count` must be between 1 and 1000 but was " + invalid));
        }
    }

    @Test
    void killingTheTriggerClosesAReadInFlightAndStartsNothing() throws Exception {
        var key = "r236_trigger_kill_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI); var proxy = StallingProxy.start("localhost", 6379, "XREADGROUP")) {
            fixture.createGroup(key, GROUP, "0");
            fixture.add(key, Map.of("order", "1"));

            var trigger = trigger(key).url(Property.ofValue("redis://:redis@localhost:" + proxy.getPort() + "/0")).build();

            var completed = new CountDownLatch(1);
            var thrown = new AtomicReference<Throwable>();
            var result = new AtomicReference<Optional<Execution>>();
            var runner = new Thread(() ->
            {
                try {
                    result.set(evaluate(trigger));
                } catch (Throwable t) {
                    thrown.set(t);
                } finally {
                    completed.countDown();
                }
            }, "stream-trigger-evaluate-kill-test");
            runner.setDaemon(true);

            try {
                runner.start();

                // evaluate() is genuinely blocked on the read, so only kill() can end it
                assertThat("evaluate() must reach the stalled read", proxy.awaitStalled(Duration.ofSeconds(10)), is(true));
                assertThat(completed.await(500, TimeUnit.MILLISECONDS), is(false));

                var killStart = System.currentTimeMillis();
                trigger.kill();
                var killElapsedMs = System.currentTimeMillis() - killStart;

                assertThat("kill() must not wait for the connection shutdown", killElapsedMs, lessThan(1000L));
                assertThat("evaluate() must return promptly after kill()", completed.await(10, TimeUnit.SECONDS), is(true));
                assertThat("a killed evaluation is not a trigger error", thrown.get(), is(nullValue()));
                assertThat("a killed evaluation starts no execution", result.get().isPresent(), is(false));

                // the stalled read never reached Redis, so nothing was delivered and nothing was acknowledged
                assertThat(fixture.pendingIds(key, GROUP), is(empty()));
            } finally {
                trigger.kill();
                runner.join(Duration.ofSeconds(10).toMillis());
                if (runner.isAlive()) {
                    runner.interrupt();
                }
            }

            fixture.delete(key);
        }
    }

    @Test
    void aKilledTriggerDeliversNothingAndLeavesTheEntriesForTheNextReader() throws Exception {
        var key = "r236_trigger_killed_" + IdUtils.create();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.createGroup(key, GROUP, "0");
            var id = fixture.add(key, Map.of("order", "1"));
            var trigger = trigger(key).build();

            trigger.kill();

            // there is an entry to deliver, and a killed trigger must neither start an execution nor mark it as delivered
            assertThat(evaluate(trigger).isPresent(), is(false));
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            // and the entry is still deliverable to a trigger that was not killed
            var live = evaluate(trigger(key).build());
            assertThat(live.isPresent(), is(true));
            assertThat(live.get().getTrigger().getVariables().get("ids"), is(List.of(id)));

            fixture.delete(key);
        }
    }

    @Test
    void anUnreachableRedisFailsTheEvaluationOfALiveTrigger() {
        // the control for the test above: the same trigger, not killed, does try to connect and fails
        var trigger = trigger("r236_trigger_unreachable_" + IdUtils.create())
            .url(Property.ofValue("redis://:redis@localhost:1/0"))
            .build();

        assertThrows(Exception.class, () -> evaluate(trigger));
    }

    @Test
    void theSchedulerRunsTheFlowAndItsAckLeavesNothingPending() throws Exception {
        var key = FLOW_KEYS.get(3);
        var done = new CountDownLatch(1);
        var finished = new AtomicReference<Execution>();

        try (var fixture = new StreamFixture(REDIS_URI)) {
            fixture.delete(key);
            fixture.createGroup(key, GROUP, "0");
            var ids = new ArrayList<String>();
            ids.add(fixture.add(key, Map.of("order", "1")));
            ids.add(fixture.add(key, Map.of("order", "2")));

            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution ->
            {
                if (execution.getLeft().getFlowId().equals("stream-trigger-flow") && execution.getLeft().getState().isTerminated()) {
                    finished.set(execution.getLeft());
                    done.countDown();
                }
            });

            repositoryLoader.load(
                MAIN_TENANT,
                Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/stream-trigger-flow.yaml"))
            );

            assertThat("the trigger started and finished an execution", done.await(1, TimeUnit.MINUTES), is(true));
            assertThat(finished.get().getState().getCurrent(), is(State.Type.SUCCESS));
            assertThat(finished.get().getTrigger().getVariables().get("ids"), is(ids));

            // read -> process -> acknowledge went through the scheduler, and Redis agrees nothing is left pending
            assertThat(fixture.pendingIds(key, GROUP), is(empty()));

            receive.blockLast(Duration.ofMillis(100));
        } catch (IllegalStateException ignored) {
            // blockLast on a flux that was not completed yet: the assertions above are what matters
        }
    }

    private Trigger.TriggerBuilder<?, ?> trigger(String key) {
        return Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(REDIS_URI))
            .key(Property.ofValue(key))
            .group(Property.ofValue(GROUP))
            .consumer(Property.ofValue(CONSUMER));
    }

    // a trigger evaluation the way the scheduler runs it: the run context is derived from the trigger context
    private Optional<Execution> evaluate(Trigger trigger) throws Exception {
        var triggerContext = io.kestra.core.models.triggers.Trigger.builder()
            .tenantId(MAIN_TENANT)
            .namespace("io.kestra.tests")
            .flowId("stream-trigger")
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .executionId(IdUtils.create())
            .build();

        var flow = Flow.builder()
            .tenantId(MAIN_TENANT)
            .namespace("io.kestra.tests")
            .id("stream-trigger")
            .revision(1)
            .tasks(List.of(Return.builder().id("end").type(Return.class.getName()).format(Property.ofValue("x")).build()))
            .triggers(List.of(trigger))
            .build();

        var runContext = runContextInitializer.forScheduler((DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger);

        return trigger.evaluate(ConditionContext.builder().flow(flow).runContext(runContext).build(), triggerContext);
    }

    // the count check happens before anything is built from the flow, so a plain context is enough
    private Optional<Execution> evaluateOnly(Trigger trigger) throws Exception {
        return trigger.evaluate(
            ConditionContext.builder().runContext(runContextFactory.of(Map.of())).build(),
            TriggerContext.builder().tenantId(MAIN_TENANT).namespace("io.kestra.tests").flowId("stream-trigger").triggerId(trigger.getId()).date(ZonedDateTime.now()).build()
        );
    }
}
