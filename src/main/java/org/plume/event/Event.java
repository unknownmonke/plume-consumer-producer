package org.plume.event;

import lombok.NonNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

import static java.util.UUID.randomUUID;

/**
 * Common structure for events.
 *
 * <li> Payload : actual data of event.
 * <li> Metadata : metadata of event.
 * <li> Origin : origin event if this event is a child of another event (replay, error...).
 */
public record Event(
    Object payload,
    Metadata metadata,
    Event origin
) implements Serializable {

    // Excludes origin of event.
    public Event(@NonNull Object payload,
                 @NonNull Type type,
                 @NonNull Source source,
                 @NonNull Identity identity,
                 @NonNull Exposure exposure,
                 String correlationId,
                 Instant timestamp,
                 Map<?, ?> additionalProperties) {

        Metadata metadata = Metadata.with(
                randomUUID().toString(),
                correlationId)
            .timestamp(timestamp == null ? Instant.now() : timestamp)
            .type(type)
            .source(source)
            .identity(identity)
            .exposure(exposure)
            .additionalProperties(additionalProperties)
            .build();

        this(payload, metadata, null);
    }

    // Includes origin of event.
    public Event(@NonNull Object payload,
                 @NonNull Type type,
                 @NonNull Source source,
                 @NonNull Identity identity,
                 @NonNull Exposure exposure,
                 Instant timestamp,
                 Map<?, ?> additionalProperties,
                 Event origin) {

        Metadata metadata = Metadata.with(
                randomUUID().toString(),
                origin.metadata().correlationId())
            .parentId(origin.metadata().uuid())
            .timestamp(timestamp == null ? Instant.now() : timestamp)
            .type(type)
            .source(source)
            .identity(identity)
            .exposure(exposure)
            .additionalProperties(additionalProperties)
            .build();

        this(payload, metadata, origin);
    }
}
