package io.kestra.plugin.redis.list;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.redis.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ListPopBaseInterface {
    @Schema(
        title = "Redis list key",
        description = "Rendered key passed to `LPOP`."
    )
    @NotNull
    Property<String> getKey();

    @Schema(
        title = "Serialization format",
        description = "Defaults to STRING; controls how list elements are decoded."
    )
    Property<SerdeType> getSerdeType();
}
