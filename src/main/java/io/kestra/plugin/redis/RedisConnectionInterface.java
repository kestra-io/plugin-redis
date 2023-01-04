package io.kestra.plugin.redis;


import io.swagger.v3.oas.annotations.media.Schema;

public interface RedisConnectionInterface {

    @Schema(
            title = "Redis protocol",
            description = "The protocol used for the connection"
    )
    String getUri();

}
