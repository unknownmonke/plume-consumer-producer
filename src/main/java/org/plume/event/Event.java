package org.plume.event;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@Getter
@EqualsAndHashCode
@NoArgsConstructor
public class Event implements Serializable {

    private Object payload;
    private Metadata metadata;
    private Event origin;


    // Excludes origin of event.
    public Event(@NonNull Object payload,
                 @NonNull Type type,
                 @NonNull Source source,
                 @NonNull Identity identity,
                 @NonNull Exposure exposure,
                 String correlationId,
                 Instant timestamp,
                 Map<?, ?> additionalProperties) {

        this.payload = payload;
        this.metadata = Metadata.with(
                randomUUID().toString(),
                correlationId)
            .timestamp(timestamp == null ? Instant.now() : timestamp)
            .type(type)
            .source(source)
            .identity(identity)
            .exposure(exposure)
            .additionalProperties(additionalProperties)
            .build();
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

        this.payload = payload;
        this.metadata = Metadata.with(
                randomUUID().toString(),
                origin.getMetadata().correlationId())
            .parentId(origin.getMetadata().uuid())
            .timestamp(timestamp == null ? Instant.now() : timestamp)
            .type(type)
            .source(source)
            .identity(identity)
            .exposure(exposure)
            .additionalProperties(additionalProperties)
            .build();
        this.origin = origin;
    }

    public Event(@NonNull Metadata metadata, @NonNull Object payload, Event origin) {
        this.payload = payload;
        this.origin = origin;
        this.metadata = metadata;
    }
}
