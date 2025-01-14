package io.kestra.plugin.redis.models;

import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;

@Schema(
    title = "Serializer / Deserializer use for the value"
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
            if (message instanceof String messageString) {
                // will raise an exception if invalid json
                JacksonMapper.toObject(messageString);

                return messageString;
            } else {
                return JacksonMapper.ofJson(false).writeValueAsString(message);
            }
        } else {
            return String.valueOf(message);
        }
    }
}
