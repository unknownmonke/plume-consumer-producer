package org.plume.producer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.plume.event.Event;
import org.plume.event.EventFactory;
import org.plume.idempotency.IdempotencyKeyStore;
import org.plume.idempotency.hash.Base64HashGenerator;
import org.plume.idempotency.hash.HashGenerator;
import org.plume.lifecycle.ShutdownManager;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.plume.common.Constants.getDlqTopic;

@Slf4j
public class EventProducer {

    private final ProducerBootstrap producerBootstrap;
    private final ShutdownManager shutdownManager;
    private final Map<?, ?> customProperties;
    private final boolean enableIdempotencyCheck;
    private final HashGenerator hashGenerator;
    private final IdempotencyKeyStore idempotencyKeyStore;
    private final String dlqTopic;

    private ProducerRecordBuilder producerRecordBuilder;
    private KafkaProducer<String, Event> kafkaProducer;
    private InternalEventProducer internalProducer;
    
    
    public EventProducer(@NonNull ProducerBootstrap producerBootstrap,
                         ShutdownManager shutdownManager,
                         Map<?, ?> customProperties,
                         boolean enableIdempotencyCheck,
                         HashGenerator hashGenerator,
                         IdempotencyKeyStore idempotencyKeyStore,
                         String dlqTopic) {
        log.info("Initializing producer...");

        this.producerBootstrap = producerBootstrap;
        this.shutdownManager = shutdownManager;
        this.customProperties = customProperties;
        this.enableIdempotencyCheck = enableIdempotencyCheck;
        this.hashGenerator = hashGenerator != null ? hashGenerator : new Base64HashGenerator();
        this.idempotencyKeyStore = maybeSetIdempotencyStore(idempotencyKeyStore);
        this.dlqTopic = dlqTopic;

        setupProducer();
    }


    public static ProducerBuilder with(ProducerBootstrap producerBootstrap) {
        return new ProducerBuilder(producerBootstrap);
    }

    private void setupProducer() {
        this.producerRecordBuilder = new ProducerRecordBuilder(hashGenerator);
        this.kafkaProducer = new KafkaProducer<>(buildConfig());
        this.internalProducer = buildInternalProducer();
        addShutdownHook();
    }

    private Properties buildConfig() {
        Properties config = new Properties();

        // Applies common configuration.
        config.putAll(producerBootstrap.properties());

        // Registers custom properties if any.
        if (customProperties != null) {
            config.putAll(customProperties);
        }
        return config;
    }

    private InternalEventProducer buildInternalProducer() {
        ProducerBootstrap internalProducerBootstrap = ProducerBootstrap.with(
                producerBootstrap.getBootstrapServers(),
                producerBootstrap.getClientId() + "-internal-producer",
                producerBootstrap.getSecurity())
            .build();
        
        return new InternalEventProducer(internalProducerBootstrap);
    }

    /**
     * Adds provided hook or registers a default one that will stop the producer.
     */
    private void addShutdownHook() {
        if (shutdownManager == null) {
            // Registers default shutdown hook.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaProducer.close()));
        } else {
            // Registers provided hook
            shutdownManager.registerShutdownHook(() -> kafkaProducer.close());
        }
    }

    private IdempotencyKeyStore maybeSetIdempotencyStore(IdempotencyKeyStore idempotencyKeyStore) {
        if (idempotencyKeyStore == null) {
            if (enableIdempotencyCheck) {
                throw new IllegalStateException(
                    "Idempotency check is active without an IdempotencyKeyStore implementation. " +
                        "Provide one via idempotencyKeyStore(...) " +
                        "or remove enableIdempotencyCheck() for default behavior.");
            }
            log.warn("Producer-side IdempotencyKeyStore feature is explicitly disabled. "
                + "Duplicate events will not be detected and may be published more than once.");
        }
        return idempotencyKeyStore;
    }

    /* ------------------------------------------------------------ */
    /* Async publish with optional headers, partition and callback. */
    /* ------------------------------------------------------------ */

    public Future<RecordMetadata> publish(String topic, String key, Event event) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event);
        return publishAndMaybeHandleDuplicate(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          List<? extends Header> headers) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event, headers);

        return publishAndMaybeHandleDuplicate(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          Integer partition, List<? extends Header> headers) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event,partition, headers);

        return publishAndMaybeHandleDuplicate(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event, Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event);

        return publishAndMaybeHandleDuplicate(producerRecord, callback);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          List<? extends Header> headers, Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event, headers);

        return publishAndMaybeHandleDuplicate(producerRecord, callback);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          Integer partition, List<? extends Header> headers, Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event, partition, headers);

        return publishAndMaybeHandleDuplicate(producerRecord, callback);
    }

    private Future<RecordMetadata> publishAndMaybeHandleDuplicate(ProducerRecord<String, Event> producerRecord) {
        return publishAndMaybeHandleDuplicate(producerRecord, null);
    }

    private Future<RecordMetadata> publishAndMaybeHandleDuplicate(ProducerRecord<String, Event> producerRecord, Callback callback) {
        if (enableIdempotencyCheck) {
            if (isDuplicate(producerRecord)) {
                return publishToDlq(producerRecord);
            }
            idempotencyKeyStore.save(producerRecord);
        }
        return publishRecord(producerRecord, callback);
    }

    private Future<RecordMetadata> publishRecord(ProducerRecord<String, Event> producerRecord, Callback callback) {
        Event event = producerRecord.value();

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
                    event.getMetadata().correlationId(), producerRecord.topic(), exception);
            }
            // Custom provided callback.
            if (callback != null) {
                callback.onCompletion(metadata, exception);
            }
        });
    }

    private boolean isDuplicate(ProducerRecord<String, Event> producerRecord) {
        if (idempotencyKeyStore.exists(producerRecord).isPresent()) {
            log.info("⚠ Duplicate event detected: key={}, topic={}", producerRecord.key(), producerRecord.topic());
            return true;
        }
        return false;
    }

    private Future<RecordMetadata> publishToDlq(ProducerRecord<String, Event> originalRecord) {
        String topic = getDlqTopic(dlqTopic, originalRecord.topic());
        String key = originalRecord.key();
        Event event = originalRecord.value();

        Event ignoredEvent = EventFactory.buildIgnoreEvent(event, "Duplicate event detected by producer-side idempotency");

        log.info("(Publishing duplicate to DLQ: key={}, topic={})", key, topic);
        return internalProducer.publish(topic, key, ignoredEvent);
    }

    /* ------------------------------------------------------------------------ */

    /**
     * Restricted builder for optional arguments only.
     */
    @RequiredArgsConstructor
    public static class ProducerBuilder {

        private final ProducerBootstrap producerBootstrap;
        private ShutdownManager shutdownManager;
        private Map<?, ?> customProperties;
        private boolean enableIdempotencyCheck = false;
        private HashGenerator hashGenerator;
        private IdempotencyKeyStore idempotencyKeyStore;
        private String dlqTopic;


        public ProducerBuilder shutdownManager(ShutdownManager shutdownManager) {
            this.shutdownManager = shutdownManager;
            return this;
        }

        public ProducerBuilder customProperties(Map<?, ?> customProperties) {
            this.customProperties = customProperties;
            return this;
        }

        public ProducerBuilder enableIdempotencyCheck() {
            this.enableIdempotencyCheck = true;
            return this;
        }

        public ProducerBuilder hashGenerator(HashGenerator hashGenerator) {
            this.hashGenerator = hashGenerator;
            return this;
        }

        public ProducerBuilder idempotencyKeyStore(IdempotencyKeyStore idempotencyKeyStore) {
            this.idempotencyKeyStore = idempotencyKeyStore;
            return this;
        }

        public ProducerBuilder dlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
            return this;
        }

        public EventProducer build() {
            return new EventProducer(
                producerBootstrap, shutdownManager, customProperties, enableIdempotencyCheck,
                hashGenerator, idempotencyKeyStore, dlqTopic
            );
        }
    }
}