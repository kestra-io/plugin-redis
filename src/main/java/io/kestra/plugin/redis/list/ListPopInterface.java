package io.kestra.plugin.redis.list;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ListPopInterface extends ListPopBaseInterface {
    @Schema(
        title = "Maximum rows to fetch",
        description = "Soft cap evaluated each loop; required when maxDuration is not set."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration to poll",
        description = "Soft cap evaluated each loop; required when maxRecords is not set."
    )
    Property<Duration> getMaxDuration();

    @Schema(
        title = "Batch size per pop",
        description = "Defaults to 100."
    )
    Property<Integer> getCount();
}
