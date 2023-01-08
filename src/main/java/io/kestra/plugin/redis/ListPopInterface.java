package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;
import java.time.Duration;

public interface ListPopInterface {

    @Schema(
        title = "Redis key",
        description = "The redis key you want to set"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    String getKey();

    @Schema(
        title = "Deserialization type",
        description = "Format of the data contained in Redis"
    )
    SerdeType getSerdeType();

    @Schema(
        title = "The max number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Integer getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Duration getMaxDuration();

    @Schema(
        title = "Number of elements that should be pop at once"
    )
    Integer getCount();
}
