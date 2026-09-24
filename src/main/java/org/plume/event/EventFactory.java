package org.plume.event;

import org.plume.serialization.EventDeserializationException;

import java.time.Instant;
import java.util.UUID;

public class EventFactory {

    public static Event buildIgnoreEvent(Event origin, String message) {
        return new Event(
            message,
            Type.IGNORED,
            origin.metadata().source(),
            origin.metadata().identity(),
            Exposure.IGNORED,
            Instant.now(),
            origin.metadata().additionalProperties(),
            origin
        );
    }

    public static Event buildErrorEvent(String message, Exception e, byte[] originalEvent) {
        return new Event(
            new EventDeserializationException(message, e, new String(originalEvent)),
            Type.ERROR,
            new Source("system", "plume"),
            new Identity("External deserialization error"),
            Exposure.ERROR,
            new UUID(8L, 4L).toString(),
            Instant.now(),
            null
        );
    }
}
