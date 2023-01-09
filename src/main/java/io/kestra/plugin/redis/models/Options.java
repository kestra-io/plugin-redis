package io.kestra.plugin.redis.models;

import io.lettuce.core.SetArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
public class Options {
    @Schema(
        title = "Set the specified expire time, in seconds."
    )
    private Long expireTimeSeconds;

    @Schema(
        title = "Set the specified Unix time at which the key will expire, in seconds."
    )
    private Long expireTimeSecondsTimestamp;

    @Schema(
        title = "Set the specified expire time, in milliseconds."
    )
    private Long expireTimeMillis;

    @Schema(
        title = "Set the specified Unix time at which the key will expire, in milliseconds."
    )
    private Long expireTimeMillisTimestamp;

    @Schema(
        title = "Only set the key if it does not already exist."
    )
    @Builder.Default
    private boolean notExist = false;

    @Schema(
        title = "Only set the key if it already exist."
    )
    @Builder.Default
    private boolean mustExist = false;

    @Schema(
        title = " Retain the time to live associated with the key."
    )
    @Builder.Default
    private boolean keepttl = false;

    public SetArgs asRedisSet() {
        SetArgs setArgs = new SetArgs();
        if (expireTimeSeconds != null) {
            setArgs.ex(expireTimeSeconds);
        }
        if (expireTimeSecondsTimestamp != null) {
            setArgs.exAt(expireTimeSecondsTimestamp);
        }
        if (expireTimeMillis != null) {
            setArgs.px(expireTimeMillis);
        }
        if (expireTimeMillisTimestamp != null) {
            setArgs.pxAt(expireTimeMillisTimestamp);
        }
        if (notExist) {
            setArgs.nx();
        }
        if (mustExist) {
            setArgs.xx();
        }
        if (keepttl) {
            setArgs.keepttl();
        }
        return setArgs;
    }
}
