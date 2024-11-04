package io.kestra.plugin.redis;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface RedisConnectionInterface {

    @NotNull
    @Schema(
        title = "The connection string."
    )
    Property<String> getUrl();
}
