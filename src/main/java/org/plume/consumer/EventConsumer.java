package org.plume.consumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.plume.consumer.processing.ErrorOnFallback;
import org.plume.event.Event;
import org.plume.event.EventFactory;
import org.plume.idempotency.IdempotencyKeyStore;
import org.plume.lifecycle.ShutdownManager;
import org.plume.lifecycle.StartupManager;
import org.plume.producer.InternalEventProducer;
import org.plume.producer.ProducerBootstrap;
import org.plume.serialization.EventDeserializationException;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.plume.common.Constants.getOrInferDlqTopic;
import static org.plume.common.Constants.getOrInferErrorTopic;

@Slf4j
public class EventConsumer implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConsumerBootstrap consumerBootstrap;
    private final BiConsumer<Headers, Event> consumerFunction;
    private final Consumer<ErrorOnFallback> errorCallback;
    private final StartupManager startupManager;
    private final ShutdownManager shutdownManager;
    private final Map<?, ?> customProperties;
    private final boolean enableIdempotencyCheck;
    private final IdempotencyKeyStore idempotencyKeyStore;
    private final String dlqTopic;
    private final String errorTopic;

    private KafkaConsumer<String, Event> kafkaConsumer;
    private InternalEventProducer internalProducer;


    private EventConsumer(@NonNull ConsumerBootstrap consumerBootstrap,
                         @NonNull BiConsumer<Headers, Event> consumerFunction,
                         Consumer<ErrorOnFallback> errorCallback,
                         StartupManager startupManager,
                         ShutdownManager shutdownManager,
                         Map<?, ?> customProperties,
                         boolean enableIdempotencyCheck,
                         IdempotencyKeyStore idempotencyKeyStore,
                         String dlqTopic, String errorTopic
    ) {
        log.info("Initializing consumer...");

        this.consumerBootstrap = consumerBootstrap;
        this.consumerFunction = consumerFunction;
        this.errorCallback = errorCallback;
        this.startupManager = startupManager;
        this.shutdownManager = shutdownManager;
        this.customProperties = customProperties;
        this.enableIdempotencyCheck = enableIdempotencyCheck;
        this.idempotencyKeyStore = maybeSetIdempotencyStore(idempotencyKeyStore);
        this.dlqTopic = dlqTopic;
        this.errorTopic = errorTopic;

        setupConsumer();
    }


    public static ConsumerBuilder with(ConsumerBootstrap consumerBootstrap,
                                       BiConsumer<Headers, Event> consumerFunction) {
        return new ConsumerBuilder(consumerBootstrap, consumerFunction);
    }

    private void setupConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(buildConfig());
        this.internalProducer = buildInternalProducer();
        addStartupHook();
        addShutdownHook();
    }

    private Properties buildConfig() {
        Properties config = new Properties();

        // Applies common configuration.
        config.putAll(consumerBootstrap.properties());

        // Registers supplied custom properties.
        if (customProperties != null) {
            config.putAll(customProperties);
        }
        return config;
    }

    // Internal events use the same schema when validation is enabled.
    private InternalEventProducer buildInternalProducer() {
        ProducerBootstrap internalProducerBootstrap = ProducerBootstrap.with(
                consumerBootstrap.getBootstrapServers(),
                consumerBootstrap.getClientId() + "-internal-producer",
                consumerBootstrap.getSecurity())
            .withSchemaValidation(consumerBootstrap.getSchemaRegistryConfig())
            .build();

        return new InternalEventProducer(internalProducerBootstrap);
    }

    /**
     * Adds delaying startup hook if any.
     */
    private void addStartupHook() {
        if (startupManager != null) {
            startupManager.registerStartupHook(this);
        }
    }

    /**
     * Adds provided hook or registers a default one that will stop the consumer.
     */
    private void addShutdownHook() {
        if (shutdownManager == null) {
            // Registers default shutdown hook.
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        } else {
            // Registers provided hook.
            shutdownManager.registerShutdownHook(this);
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
            log.warn("Consumer-side IdempotencyKeyStore feature is explicitly disabled. "
                + "Duplicate events will not be detected and may be processed more than once.");
        }
        return idempotencyKeyStore;
    }

    /**
     * Polls into a separate thread to not block main thread on infinite loop.
     */
    @Override
    public void run() {
        new Thread(() -> {
            try {
                kafkaConsumer.subscribe(consumerBootstrap.getTopics());
                log.info("Subscribed to topics: {}", kafkaConsumer.listTopics().keySet());

                while (!closed.get()) {
                    ConsumerRecords<String, Event> records = kafkaConsumer.poll(Duration.ofSeconds(5));

                    for (ConsumerRecord<String, Event> record : records) {
                        log.info("-------- Processing record: key={}, topic={} at offset={} --------", record.key(), record.topic(), record.offset());
                        maybeProcessRecord(record);
                    }
                }
            } catch (WakeupException e) {
                // Ignores exception if closing.
                if (!closed.get()) throw e;

            } catch (Exception e) {
                log.error("Error in consumer thread: ", e);

            } finally {
                kafkaConsumer.close();
                log.info("Consumer stopped.");
            }
        }).start();
    }

    public void stop() {
        closed.set(true);
        kafkaConsumer.wakeup();
    }

    private void maybeProcessRecord(ConsumerRecord<String, Event> consumerRecord) {

        if (consumerRecord.value().payload() instanceof EventDeserializationException) {
            handleDeserializationException(consumerRecord);
            return;
        }

        if (enableIdempotencyCheck) {
            if (isDuplicate(consumerRecord)) {
                publishToDlq(consumerRecord);
                return;
            }
            idempotencyKeyStore.save(consumerRecord, consumerBootstrap.getGroupId());
        }
        consumerFunction.accept(consumerRecord.headers(), consumerRecord.value());
    }

    private boolean isDuplicate(ConsumerRecord<String, Event> consumerRecord) {
        if (idempotencyKeyStore.exists(consumerRecord, consumerBootstrap.getGroupId()).isPresent()) {

            log.info("⚠ Duplicate event detected: key={}, topic={} at offset={}",
                consumerRecord.key(), consumerRecord.topic(), consumerRecord.offset());
            return true;
        }
        return false;
    }

    private void publishToDlq(ConsumerRecord<String, Event> originalRecord) {
        String topic = getOrInferDlqTopic(dlqTopic, originalRecord.topic());
        String key = originalRecord.key();
        Event event = originalRecord.value();

        Event ignoredEvent = EventFactory.buildIgnoreEvent(event, "Duplicate event detected by consumer-side idempotency");

        log.info("(Publishing duplicate to DLQ: key={}, topic={})", key, topic);
        internalProducer.publish(topic, key, ignoredEvent);
    }

    private void handleDeserializationException(ConsumerRecord<String, Event> consumerRecord) {
        String topic = getOrInferErrorTopic(errorTopic, consumerRecord.topic());

        log.info("(Publishing error event to error topic: key={}, topic={})", consumerRecord.key(), consumerRecord.topic());

        internalProducer.publish(topic, consumerRecord.key(), consumerRecord.value());
        maybeApplyFallbackOnError(consumerRecord);
    }

    /**
     * Applies user-provided callback if present.
     */
    private void maybeApplyFallbackOnError(ConsumerRecord<String, Event> consumerRecord) {
        if (this.errorCallback != null) {
            Event errorEvent = consumerRecord.value();
            EventDeserializationException exception = (EventDeserializationException) errorEvent.payload();

            this.errorCallback.accept(
                new ErrorOnFallback(errorEvent, consumerRecord.key(), consumerRecord.offset(), consumerRecord.partition(), exception));
        }
    }

    /* ------------------------------------------------------------------------ */

    /**
     * Restricted builder for optional arguments only.
     */
    @RequiredArgsConstructor
    public static class ConsumerBuilder {

        private final ConsumerBootstrap consumerBootstrap;
        private final BiConsumer<Headers, Event> consumerFunction;
        private Consumer<ErrorOnFallback> errorCallback;
        private StartupManager startupManager;
        private ShutdownManager shutdownManager;
        private Map<?, ?> customProperties;
        private boolean enableIdempotencyCheck = false;
        private IdempotencyKeyStore idempotencyKeyStore;
        private String dlqTopic;
        private String errorTopic;

        public ConsumerBuilder errorCallback(Consumer<ErrorOnFallback> errorCallback) {
            this.errorCallback = errorCallback;
            return this;
        }

        public ConsumerBuilder startupManager(StartupManager startupManager) {
            this.startupManager = startupManager;
            return this;
        }

        public ConsumerBuilder shutdownManager(ShutdownManager shutdownManager) {
            this.shutdownManager = shutdownManager;
            return this;
        }

        public ConsumerBuilder customProperties(Map<?, ?> customProperties) {
            this.customProperties = customProperties;
            return this;
        }

        public ConsumerBuilder enableIdempotencyCheck() {
            this.enableIdempotencyCheck = true;
            return this;
        }

        public ConsumerBuilder idempotencyKeyStore(IdempotencyKeyStore idempotencyKeyStore) {
            this.idempotencyKeyStore = idempotencyKeyStore;
            return this;
        }

        public ConsumerBuilder dlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
            return this;
        }

        public ConsumerBuilder errorTopic(String errorTopic) {
            this.errorTopic = errorTopic;
            return this;
        }

        public EventConsumer build() {
            return new EventConsumer(
                consumerBootstrap, consumerFunction, errorCallback, startupManager, shutdownManager,
                customProperties, enableIdempotencyCheck, idempotencyKeyStore, dlqTopic,
                errorTopic
            );
        }
    }
}
