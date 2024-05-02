package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public interface ListPopBaseInterface {
    @Schema(
        title = "The redis key for the list."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    String getKey();

    @Schema(
        title = "Format of the data contained in Redis"
    )
    SerdeType getSerdeType();
}
