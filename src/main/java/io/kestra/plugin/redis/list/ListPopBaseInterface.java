package io.kestra.plugin.redis.list;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.redis.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ListPopBaseInterface {
    String SERDE_TYPE_DESCRIPTION = """
        Defaults to STRING. Controls how list elements are decoded.
        With JSON, an element that is not valid JSON is kept as a string and a warning is logged.
        """;

    @PluginProperty(group = "main")
    @Schema(
        title = "Redis list key",
        description = "Rendered key passed to `LPOP`."
    )
    @NotNull
    Property<String> getKey();

    @PluginProperty(group = "main")
    @Schema(
        title = "Serialization format",
        description = SERDE_TYPE_DESCRIPTION
    )
    Property<SerdeType> getSerdeType();
}
