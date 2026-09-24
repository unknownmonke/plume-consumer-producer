package org.plume.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.plume.event.Event;
import org.plume.event.EventFactory;

import java.io.IOException;

import static org.plume.serialization.SchemaUtils.DATA_INSTANCE;

/**
 * Custom Avro Event deserializer class.
 *
 * <p> Does not rely on {@code KafkaAvroDeserializer} and implements its own logic
 * as Java record SerDes are not supported natively.
 */
@Slf4j
public class EventAvroDeserializer implements Deserializer<Event> {

    @Override
    public Event deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public Event deserialize(String topic, Headers headers, byte[] data) {
        Schema schema = SchemaUtils.getEventSchema();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        RecordDatumReader<Event> reader = new RecordDatumReader<>(schema, DATA_INSTANCE);

        try {
            return reader.read(null, decoder);

        } catch (IOException e) {
            throw new RuntimeException(e);

        } catch (Exception e) {
            log.error("Event deserialization failed for topic: {} - {}", topic, e.getMessage());

            return EventFactory.buildErrorEvent(
                String.format("Event deserialization failure: %s", e.getMessage()), e, data);
        }
    }
}
