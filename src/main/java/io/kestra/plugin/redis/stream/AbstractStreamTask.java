package io.kestra.plugin.redis.stream;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.redis.AbstractRedisConnection;

import io.lettuce.core.StreamMessage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

/** Holds the Redis connection while a stream task runs, so a kill can close it without waiting. */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractStreamTask extends AbstractRedisConnection {
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<RedisFactory> factoryRef = new AtomicReference<>();

    @FunctionalInterface
    protected interface StreamAction<T> {
        T run(RedisFactory factory) throws Exception;
    }

    // what names the operation in the cancellation messages, for example "Read on a Redis stream consumer group"
    protected <T> T cancellable(RunContext runContext, String what, StreamAction<T> action) throws Exception {
        var logger = runContext.logger();

        if (!isActive.get()) {
            throw new CancellationException(what + " was cancelled before it started.");
        }

        var factory = this.redisFactory(runContext);
        factoryRef.set(factory);

        try {
            // kill() may have landed while the connection was opening
            if (!isActive.get()) {
                throw new CancellationException(what + " was cancelled before it started.");
            }

            return action.run(factory);
        } catch (Exception e) {
            if (!isActive.get()) {
                logger.info("{} was cancelled; no entry was acknowledged.", what);

                throw new CancellationException(what + " was cancelled while it was running.");
            }

            throw e;
        } finally {
            this.releaseFactory(logger);
        }
    }

    protected boolean isCancelled() {
        return !isActive.get();
    }

    // the atomic hand-off makes sure the connection is closed once, whether by the run or by an earlier path
    private void releaseFactory(Logger logger) {
        var factory = factoryRef.getAndSet(null);
        if (factory == null) {
            return;
        }

        try {
            factory.close();
        } catch (Exception e) {
            logger.warn("Failed to close the Redis connection of task id={}", this.id, e);
        }
    }

    // closing the connection fails a command in flight without waiting; the run does the full cleanup
    public void kill() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        var factory = factoryRef.get();
        if (factory != null) {
            factory.closeAsync();
        }
    }

    // @Min and @Max on a Property type argument do not bound a literal written in flow YAML, so the run checks too
    protected static int requireBetween(String name, int value, int min, int max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException("`" + name + "` must be between " + min + " and " + max + " but was " + value);
        }

        return value;
    }

    // Redis field names and values are kept exactly as stored, and a nil reply becomes an empty batch
    protected static List<ReadGroup.Message> toMessages(List<StreamMessage<String, String>> entries) {
        if (entries == null) {
            return List.of();
        }

        return entries.stream()
            .map(
                entry -> ReadGroup.Message.builder()
                    .stream(entry.getStream())
                    .id(entry.getId())
                    .fields(entry.getBody() == null ? Map.<String, String> of() : Collections.unmodifiableMap(new LinkedHashMap<>(entry.getBody())))
                    .build()
            )
            .toList();
    }

    protected static URI store(RunContext runContext, List<ReadGroup.Message> messages) throws Exception {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var stream = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            FileSerde.writeAll(stream, Flux.fromIterable(messages)).block();
            stream.flush();
        }

        return runContext.storage().putFile(tempFile);
    }
}
