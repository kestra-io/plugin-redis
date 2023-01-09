package io.kestra.plugin.redis.models;

import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;

@Schema(
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

    public String serialize(Object message) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).writeValueAsString(message);
        } else {
            return (String) message;
        }
    }
}
