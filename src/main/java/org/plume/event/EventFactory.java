package org.plume.event;

import java.time.Instant;

import static org.plume.event.Type.IGNORED;

public class EventFactory {

    public static Event buildIgnoreEvent(Event origin, String message) {
        return new Event(
            message,
            IGNORED,
            origin.metadata().source(),
            origin.metadata().identity(),
            Exposure.IGNORED,
            Instant.now(),
            origin.metadata().additionalProperties(),
            origin
        );
    }
}
