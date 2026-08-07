package org.plume.producer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.plume.event.Event;
import org.plume.idempotency.hash.Base64HashGenerator;
import org.plume.idempotency.hash.HashGenerator;

import java.util.concurrent.Future;

/**
 * Internal event producer to publish events as part of DLQ, error and replay cycles.
 * Should not be accessible to clients.
 *
 * <p> Groups common code accessed in both {@code EventProducer} and {@code EventConsumer} in a single class to avoid duplication,
 * with simplified logic (idempotency...).
 *
 * <p> Uses parent security, shutdown hook configuration and hash generator function from producer.
 */
@Slf4j
public class InternalEventProducer {

    private final ProducerBootstrap producerBootstrap;
    private final HashGenerator hashGenerator;

    private KafkaProducer<String, Event> kafkaProducer;
    private ProducerRecordBuilder producerRecordBuilder;


    public InternalEventProducer(@NonNull ProducerBootstrap producerBootstrap) {
        this.producerBootstrap = producerBootstrap;
        this.hashGenerator = new Base64HashGenerator();

        setupProducer();
    }


    private void setupProducer() {
        this.producerRecordBuilder = new ProducerRecordBuilder(hashGenerator);
        this.kafkaProducer = new KafkaProducer<>(producerBootstrap.properties());
        addShutdownHook();
    }

    /**
     * Adds provided hook or registers a default one that will stop the producer.
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaProducer.close()));
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event);

        return kafkaProducer.send(producerRecord, (metadata, exception) -> {
            // On success.
            if (exception == null) {
                log.debug("Acknowledged record: \n Topic: {}\n Partition: {}\n Offset: {}\n Timestamp: {}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp());
            }
            // On error.
            else {
                log.error("Error trying to publish: correlationId={}, topic={}",
                    event.metadata().correlationId(), producerRecord.topic(), exception);
            }
        });
    }
}
