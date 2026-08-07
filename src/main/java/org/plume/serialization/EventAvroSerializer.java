package org.plume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.plume.event.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.plume.serialization.SchemaUtils.DATA_INSTANCE;

/**
 * Custom Avro Event serializer class.
 *
 * <p> Does not rely on {@code KafkaAvroSerializer} and implements its own logic
 * as Java record SerDes are not supported natively.
 */
public class EventAvroSerializer implements Serializer<Event> {

    @Override
    public byte[] serialize(String topic, Event data) {
        return serialize(topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Event data) {
        if (data == null) return null;

        Schema schema = SchemaUtils.getEventSchema();
        GenericDatumWriter<Event> writer = new GenericDatumWriter<>(schema, DATA_INSTANCE);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(data, encoder);
            encoder.flush();
            return out.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
