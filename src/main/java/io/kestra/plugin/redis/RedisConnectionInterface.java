package io.kestra.plugin.redis;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface RedisConnectionInterface {

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The connection string."
    )
    String getUrl();
}
