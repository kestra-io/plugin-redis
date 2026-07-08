package io.kestra.plugin.redis.vector;

/**
 * Mirrors {@link io.lettuce.core.vector.QuantizationType}'s constants so that {@link Add#quantization}
 * doesn't bind the task's public property surface directly to a third-party SDK type.
 */
public enum QuantizationType {
    NO_QUANTIZATION,
    BINARY,
    Q8
}
