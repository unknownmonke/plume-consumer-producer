package org.plume.event;

import lombok.NonNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Event metadata. Each event must provide a UUID and correlationId for exactly-once semantics.
 *
 * <p> A timestamp is required and provided by default if not specified in builder.
 */
public record Metadata(
    @NonNull String uuid,
    @NonNull String correlationId,
    @NonNull Instant timestamp,
    String parentId,
    Type type,
    Source source,
    Identity identity,
    Exposure exposure,
    Map<?, ?> additionalProperties
) implements Serializable {

    public static MetadataBuilder with(String uuid, String correlationId) {
        return new MetadataBuilder(uuid, correlationId);
    }

    public static class MetadataBuilder {

        private final String uuid;
        private final String correlationId;
        private Instant timestamp;
        private String parentId;
        private Type type;
        private Source source;
        private Identity identity;
        private Exposure exposure;
        private Map<?, ?> additionalProperties;


        MetadataBuilder(String uuid, String correlationId) {
            this.uuid = uuid;
            this.correlationId = correlationId;
            this.timestamp = Instant.now();
        }


        public MetadataBuilder parentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public MetadataBuilder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public MetadataBuilder type(Type type) {
            this.type = type;
            return this;
        }

        public MetadataBuilder source(Source source) {
            this.source = source;
            return this;
        }

        public MetadataBuilder identity(Identity identity) {
            this.identity = identity;
            return this;
        }

        public MetadataBuilder exposure(Exposure exposure) {
            this.exposure = exposure;
            return this;
        }

        public MetadataBuilder additionalProperties(Map<?, ?> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        public Metadata build() {
            return new Metadata(uuid, correlationId, timestamp, parentId,
                type, source, identity, exposure, additionalProperties
            );
        }
    }
}
