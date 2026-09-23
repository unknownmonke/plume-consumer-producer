package org.plume.integration.serialization;

import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.plume.consumer.ConsumerBootstrap;
import org.plume.consumer.EventConsumer;
import org.plume.event.Event;
import org.plume.event.Type;
import org.plume.idempotency.InMemoryIdempotencyKeyStore;
import org.plume.idempotency.hash.Base64HashGenerator;
import org.plume.integration.common.AbstractIT;
import org.plume.producer.EventProducer;
import org.plume.producer.ProducerBootstrap;
import org.plume.security.PlainTextSecurity;
import org.plume.serialization.SchemaRegistryConfig;
import org.plume.serialization.SchemaType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.plume.common.Constants.getDlqTopic;
import static org.plume.event.TestEventFactory.buildTestEvent;

/**
 * Encapsulates tests for schema validation and SerDes.
 */
public class SerializationTest extends AbstractIT {

    @Test
    void producer_and_consumer_should_validate_schema() throws ExecutionException, InterruptedException {
        List<Event> values = new ArrayList<>();
        Event event = buildTestEvent();

        // Registers schema beforehand.
        registerAvroEventSchema();

        SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(
            getSchemaRegistryUrl(),
            "user",
            "pwd",
            SchemaType.AVRO
        );

        // Creates producer with schema validation.
        ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-producer",
                new PlainTextSecurity())
            .withSchemaValidation(schemaRegistryConfig)
            .build();

        EventProducer eventProducer = EventProducer.with(producerBootstrap)
            .customProperties(Map.of(VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName()))
            .build();

        // Creates consumer with schema validation.
        ConsumerBootstrap consumerBootstrap = ConsumerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-consumer",
                new PlainTextSecurity(),
                "test-eventconsumer-group")
            .withSchemaValidation(schemaRegistryConfig)
            .forTopic(TOPIC)
            .build();

        EventConsumer eventConsumer = EventConsumer.with(
                consumerBootstrap,
                (_, value) -> values.add(value))
            .customProperties(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
            .build();

        eventConsumer.run();

        // Publishes valid event and asserts event has been published.
        eventProducer.publish(TOPIC, "key", event, (RecordMetadata metadata, Exception _) -> {
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(TOPIC);
            assertThat(metadata.hasOffset()).isTrue();
        }).get();

        // Assert event has been deserialized and consumed.
        Awaitility
            .await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(values.size()).isEqualTo(1);
                assertThat(values.getFirst()).isEqualTo(event);

                eventConsumer.stop(); // Closes polling thread.
            });
    }

    @Test
    void internal_events_should_be_serialized() throws ExecutionException, InterruptedException {
        List<Event> values = new ArrayList<>();
        Event event = buildTestEvent();

        String dlqTopic = getDlqTopic(null, TOPIC);
        createTopic(dlqTopic, 1, (short) 1);

        // Registers schema beforehand.
        registerAvroEventSchema();

        SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(
            getSchemaRegistryUrl(),
            "user",
            "pwd",
            SchemaType.AVRO
        );

        // Creates producer with schema validation and DLQ.
        ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-producer",
                new PlainTextSecurity())
            .withSchemaValidation(schemaRegistryConfig)
            .build();

        EventProducer eventProducer = EventProducer.with(producerBootstrap)
            .hashGenerator(new Base64HashGenerator())
            .enableIdempotencyCheck()
            .idempotencyKeyStore(new InMemoryIdempotencyKeyStore())
            .dlqTopic(dlqTopic)
            .customProperties(Map.of(VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName()))
            .build();

        // Creates consumer with schema validation for DQL topic.
        ConsumerBootstrap consumerBootstrap = ConsumerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-consumer",
                new PlainTextSecurity(),
                "test-eventconsumer-group")
            .withSchemaValidation(schemaRegistryConfig)
            .forTopic(dlqTopic)
            .build();

        EventConsumer eventConsumer = EventConsumer.with(
                consumerBootstrap,
                (_, value) -> values.add(value))
            .customProperties(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
            .build();

        eventConsumer.run();

        // Publishes event twice.
        eventProducer.publish(TOPIC, "key", event, (RecordMetadata metadata, Exception _) -> {
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(TOPIC);
            assertThat(metadata.hasOffset()).isTrue();
        }).get();

        eventProducer.publish(TOPIC, "key", event, (RecordMetadata metadata, Exception _) -> {
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(dlqTopic);
            assertThat(metadata.hasOffset()).isTrue();
        }).get();

        // Asserts event has been serialized successfully to DLQ.
        Awaitility
            .await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(values.size()).isEqualTo(1);
                assertThat(values.getFirst().metadata().type()).isEqualTo(Type.IGNORED);
                assertThat(values.getFirst().origin()).isNotNull();
                assertThat(values.getFirst().origin()).isEqualTo(event);

                eventConsumer.stop(); // Closes polling thread.
            });
    }
}
