package org.plume.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.plume.event.Event;
import org.plume.event.Metadata;
import org.plume.idempotency.hash.HashGenerator;

import java.util.List;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static org.plume.event.EventHeaders.*;

@RequiredArgsConstructor
public class ProducerRecordBuilder {

    private final HashGenerator hashGenerator;

    public ProducerRecord<String, Event> buildRecord(String topic, String key, Event event) {
        ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(topic, key, event);
        addHeaders(event.metadata(), producerRecord);
        return producerRecord;
    }

    public ProducerRecord<String, Event> buildRecord(String topic, String key, Event event,
                                                     List<? extends Header> headers) {
        ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(topic, key, event);
        addHeaders(event.metadata(), producerRecord);
        headers.forEach(header -> producerRecord.headers().add(header));
        return producerRecord;
    }

    public ProducerRecord<String, Event> buildRecord(String topic, String key, Event event,
                                                     Integer partition, List<? extends Header> headers) {
        Metadata metadata = event.metadata();
        ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(topic, partition, key, event);
        addHeaders(metadata, producerRecord);
        headers.forEach(header -> producerRecord.headers().add(header));
        return producerRecord;
    }

    private void addHeaders(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        addMetadataHeaders(metadata, producerRecord);
        addOptionalHeaders(metadata, producerRecord);

        producerRecord.headers().add(KEY, producerRecord.key() == null ? null : producerRecord.key().getBytes());
        producerRecord.headers().add(PARTITION, producerRecord.partition() == null ? null : producerRecord.partition().toString().getBytes());
        producerRecord.headers().add(TOPIC, producerRecord.topic().getBytes());
    }

    private void addMetadataHeaders(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        producerRecord.headers().add(UUID, metadata.uuid().getBytes());
        producerRecord.headers().add(CORRELATION_ID, metadata.correlationId().getBytes());
        producerRecord.headers().add(TYPE, metadata.type().toString().getBytes());
        producerRecord.headers().add(TIMESTAMP, metadata.timestamp().toString().getBytes());
        producerRecord.headers().add(SOURCE_APP, metadata.source().app().getBytes());
        producerRecord.headers().add(SOURCE_DOMAIN, metadata.source().domain().getBytes());
        producerRecord.headers().add(IDENTITY_IDENTIFIER, metadata.identity().identifier().getBytes());
        producerRecord.headers().add(IDEMPOTENCY_KEY, hashGenerator.hash(producerRecord.value()).getBytes());
    }

    private void addOptionalHeaders(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        addParentIdHeader(metadata, producerRecord);
        addIdentityAuthenticationHeader(metadata, producerRecord);
        addSourceComponentHeader(metadata, producerRecord);
    }

    private void addParentIdHeader(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        Optional<String> parentId = ofNullable(metadata.parentId());
        parentId.ifPresent(p -> producerRecord.headers().add(PARENT_ID, p.getBytes()));
    }

    private void addIdentityAuthenticationHeader(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        Optional<String> authentication = ofNullable(metadata.identity().authentication());
        authentication.ifPresent(a -> producerRecord.headers().add(IDENTITY_AUTHENTICATION, a.getBytes()));
    }

    private void addSourceComponentHeader(Metadata metadata, ProducerRecord<String, Event> producerRecord) {
        Optional<String> component = ofNullable(metadata.source().component());
        component.ifPresent(a -> producerRecord.headers().add(SOURCE_COMPONENT, a.getBytes()));
    }
}
