package io.kestra.plugin.redis.services;

import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.nio.charset.Charset;

@io.swagger.v3.oas.annotations.media.Schema(
    title = "Serializer / Deserializer use for the value",
    description = "List are not handled."
)
public enum SerdeType {
    STRING,
    JSON;

    public Object deserialize(String payload) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).readValue(payload, Object.class);
        } else {
            return payload;
        }
    }
}
