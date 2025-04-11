package io.kestra.plugin.redis.list;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ListPopBaseInterface {
    @Schema(
        title = "The Redis key for the list"
    )
    @NotNull
    Property<String> getKey();

    @Schema(
        title = "Format of the data contained in Redis"
    )
    Property<SerdeType> getSerdeType();
}
