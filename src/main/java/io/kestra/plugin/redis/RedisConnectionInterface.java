package io.kestra.plugin.redis;


import io.swagger.v3.oas.annotations.media.Schema;

public interface RedisConnectionInterface {

    @Schema(
        title = "Redis URI",
        description = "The URI to connect to the Redis Database."
    )
    String getUri();

}
