package io.kestra.plugin.redis.list;

import java.time.Duration;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;

public interface ListPopInterface extends ListPopBaseInterface {
    @PluginProperty(group = "advanced")
    @Schema(
        title = "Maximum rows to fetch",
        description = "Soft cap evaluated each loop; required when maxDuration is not set."
    )
    Property<Integer> getMaxRecords();

    @PluginProperty(group = "execution")
    @Schema(
        title = "Maximum duration to poll",
        description = "Soft cap evaluated each loop; required when maxRecords is not set."
    )
    Property<Duration> getMaxDuration();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Batch size per pop",
        description = "Defaults to 100."
    )
    Property<Integer> getCount();
}
