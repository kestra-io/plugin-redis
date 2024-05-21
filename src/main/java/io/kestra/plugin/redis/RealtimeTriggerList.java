package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React to and consume key of type list from a Redis database creating one executions for each key."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "id: list-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: echo",
                "    type: io.kestra.core.tasks.log.Log",
                "    message: \"Received '{{ trigger.value }}'\" ",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.redis.RealtimeTriggerList",
                "    url: redis://localhost:6379/0",
                "    key: mytriggerkey",
            },
            full = true
        )
    },
    beta = true
)
public class RealtimeTriggerList extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<RealtimeTriggerList.Output>, ListPopBaseInterface, RedisConnectionInterface {
    private String url;

    private String key;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        ListPop task = ListPop.builder()
            .url(this.url)
            .key(this.key)
            .count(1)
            .serdeType(this.serdeType)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map((record) -> TriggerService.generateRealtimeExecution(this, context, Output.of(record)));
    }

    public Publisher<Object> publisher(final ListPop task,
                                       final RunContext runContext) {
        return Flux.create(
            fluxSink -> {
                try (AbstractRedisConnection.RedisFactory factory = task.redisFactory(runContext)) {
                    String key = runContext.render(this.key);

                    while (isActive.get()) {
                        factory.listPop(key, 1)
                            .forEach(throwConsumer(s -> fluxSink.next(this.serdeType.deserialize(s))));
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            isActive.set(false); // proactively stop polling
                        }
                    }
                } catch (Throwable e) {
                    fluxSink.error(e);
                } finally {
                    fluxSink.complete();
                    this.waitForTermination.countDown();
                }
            });
    }


    @Builder
    @Getter
    @AllArgsConstructor(staticName = "of")
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The value."
        )
        private Object value;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
