package io.kestra.plugin.redis.list;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.RedisConnectionInterface;
import io.kestra.plugin.redis.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Batch trigger from a Redis list",
    description = "Periodically pops list items in batches using `LPOP` (default batch size 100) until `maxRecords` or `maxDuration` is reached, then starts one Execution. Use [RealtimeTrigger](https://kestra.io/plugins/plugin-redis/triggers/io.kestra.plugin.redis.list.realtimetrigger) instead for per-message executions."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: list_listen
                namespace: company.team

                tasks:
                  - id: echo
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uri }} containing {{ trigger.count }} lines"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.list.Trigger
                    url: redis://localhost:6379/0
                    key: mytriggerkey
                    maxRecords: 2
                """
        )
    },
    aliases = "io.kestra.plugin.redis.TriggerList"
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ListPop.Output>, ListPopInterface, RedisConnectionInterface {
    private Property<String> url;

    private Property<String> key;

    @Schema(
        title = "Batch size per evaluation",
        description = "Defaults to 100."
    )
    @Builder.Default
    private Property<Integer> count = Property.ofValue(100);

    @Schema(
        title = "Format of the data contained in Redis"
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    // Holds the latch for the currently in-flight evaluate() call. Unlike RealtimeTrigger (whose
    // loop runs once for the trigger's whole lifetime), evaluate() here runs once per poll cycle,
    // so a single fixed CountDownLatch would be exhausted after the first cycle and no longer
    // synchronize kill() with a later in-flight call; a fresh latch is published on every call.
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<CountDownLatch> waitForTermination = new AtomicReference<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<AbstractRedisConnection.RedisFactory> factoryRef = new AtomicReference<>();

    // Published at the start of every evaluate() cycle so kill() can log to the trigger's own
    // console instead of only the worker's static log, and so it has a logger to use even when
    // called from outside evaluate()'s thread. Stays null until the first cycle runs; callers must
    // tolerate that.
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<Logger> loggerRef = new AtomicReference<>();

    // Lettuce's own shutdown() blocks for up to 2s and close() joins on closeAsync(): give kill()'s
    // await a margin above that instead of blocking indefinitely if the connection close hangs.
    private static final Duration KILL_AWAIT_TIMEOUT = Duration.ofSeconds(5);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();
        loggerRef.set(logger);

        // Publish the fresh per-cycle latch before anything else, including the isActive check
        // below: otherwise a kill() landing between that check and the publish could still observe
        // the previous (already-completed) cycle's latch and return without truly waiting on this
        // one. The outer finally always counts it down exactly once, on every return/throw path.
        var latch = new CountDownLatch(1);
        waitForTermination.set(latch);

        try {
            if (!isActive.get()) {
                return Optional.empty();
            }

            var task = ListPop.builder()
                .url(this.url)
                .key(this.key)
                .count(this.count)
                .maxRecords(this.maxRecords)
                .maxDuration(this.maxDuration)
                .serdeType(this.serdeType)
                .build();

            ListPop.Output run;
            try {
                var factory = task.redisFactory(runContext);
                factoryRef.set(factory);
                // Re-check right after publishing: closes the gap between opening the connection and
                // storing it, where a kill() landing in between would otherwise leave the fresh
                // connection running until this cycle finishes instead of being torn down immediately.
                if (!isActive.get()) {
                    return Optional.empty();
                }

                try {
                    run = task.run(runContext, factory);
                } catch (Exception e) {
                    if (!isActive.get()) {
                        // The connection was closed by kill() to unblock the in-flight command: this
                        // is an expected shutdown, not a trigger error.
                        return Optional.empty();
                    }
                    throw e;
                }
            } finally {
                // Close on every path (happy, killed, or failed): releaseFactory()'s atomic hand-off
                // with kill() guarantees exactly one side ever closes the connection, so the
                // normal (non-killed) path never leaks a Lettuce client/connection either.
                releaseFactory();
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Found '{}' data.", run.getCount());
            }

            if (run.getCount() == 0) {
                return Optional.empty();
            }

            var execution = TriggerService.generateExecution(this, conditionContext, context, run);

            return Optional.of(execution);
        } finally {
            latch.countDown();
        }
    }

    /**
     * Atomically hands off the live factory so exactly one of {@code evaluate()}'s {@code finally}
     * and {@code kill()} closes it, whichever reaches it first; the other finds {@code factoryRef}
     * already cleared and does nothing.
     */
    private void releaseFactory() {
        var factory = factoryRef.getAndSet(null);
        if (factory == null) {
            return;
        }
        try {
            factory.close();
        } catch (Exception e) {
            var logger = loggerRef.get();
            if (logger != null) {
                logger.warn("Failed to close Redis connection for trigger id={} during kill()", this.id, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        releaseFactory();

        var latch = this.waitForTermination.get();
        if (latch == null) {
            return;
        }

        try {
            if (!latch.await(KILL_AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                var logger = loggerRef.get();
                if (logger != null) {
                    logger.warn("Trigger id={} kill() timed out after {} waiting for the in-flight evaluate() to terminate; proceeding.", this.id, KILL_AWAIT_TIMEOUT);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

}
