package org.plume.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.plume.event.Event;
import org.plume.event.EventFactory;
import tools.jackson.core.JacksonException;

import static org.plume.serialization.Mapper.OBJECT_MAPPER;

@Slf4j
public class EventDeserializer implements Deserializer<Event> {

    @Override
    public Event deserialize(String topic, byte[] data) {
        try {
            return OBJECT_MAPPER.readValue(data, Event.class);

        } catch (JacksonException e) {
            log.error("Event deserialization failed for topic: {} - {}", topic, e.getMessage());

            return EventFactory.buildErrorEvent(
                String.format("Event deserialization failure: %s", e.getMessage()), e, data);
        }
    }
}
