package io.kestra.plugin.redis.list;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ListPopInterface extends ListPopBaseInterface {
    @Schema(
        title = "The max number of rows to fetch before stopping",
        description = "It's not a hard limit and is evaluated every second."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows",
        description = "It's not a hard limit and is evaluated every second."
    )
    Property<Duration> getMaxDuration();

    @Schema(
        title = "Number of elements that should pop at once"
    )
    Property<Integer> getCount();
}
