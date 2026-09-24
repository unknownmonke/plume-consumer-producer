package org.plume.consumer.processing;

import org.plume.event.Event;

/**
 * Contains information about an event in error during deserialization / validation for additional processing.
 */
public record ErrorOnFallback(
    Event value,
    String key,
    Long offset,
    int partition,
    Throwable exception
) {}
