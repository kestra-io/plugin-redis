package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ListPopInterface extends ListPopBaseInterface {
    @Schema(
        title = "The max number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty
    Integer getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty
    Duration getMaxDuration();

    @Schema(
        title = "Number of elements that should be pop at once"
    )
    @PluginProperty
    Integer getCount();
}
