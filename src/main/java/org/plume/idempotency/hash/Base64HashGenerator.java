package org.plume.idempotency.hash;

import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.plume.event.Event;
import tools.jackson.core.JacksonException;
import java.util.Base64.Encoder;

import static java.util.Base64.getEncoder;
import static org.plume.serialization.Mapper.OBJECT_MAPPER;

/**
 * Default hash generator implementation using Base64 encoding.
 */
@NoArgsConstructor
public class Base64HashGenerator implements HashGenerator {

    private final Encoder ENCODER = getEncoder();

    @Override
    public String hash(Event event) {
        try {
            byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(event.payload());
            return ENCODER.encodeToString(bytes);

        } catch (JacksonException e) {
            throw new SerializationException("Error generating Base64 hash", e);
        }
    }
}
