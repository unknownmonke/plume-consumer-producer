package org.plume.serialization;

import lombok.Getter;

/**
 * Represents an error when deserializing an event.
 * Wraps the original event bytes to maybe produce or store for manual review.
 */
@Getter
public class EventDeserializationException extends Exception {

    private final String originEvent;

    public EventDeserializationException(String message, Throwable cause, String originEvent) {
        super(message, cause);
        this.originEvent = originEvent;
    }
}
