package org.plume.integration.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.plume.consumer.ConsumerBootstrap;
import org.plume.consumer.EventConsumer;
import org.plume.event.Event;
import org.plume.idempotency.InMemoryIdempotencyKeyStore;
import org.plume.integration.common.AbstractIT;
import org.plume.producer.EventProducer;
import org.plume.producer.ProducerBootstrap;
import org.plume.security.PlainTextSecurity;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.plume.common.Constants.getDlqTopic;
import static org.plume.event.TestEventFactory.buildTestEvent;

public class EventConsumerTest extends AbstractIT {

    @Test
    void consumer_should_process_events() throws ExecutionException, InterruptedException {

        List<Event> values = new ArrayList<>();

        ConsumerBootstrap consumerBootstrap = ConsumerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-consumer",
                new PlainTextSecurity(),
                "test-eventconsumer-group")
            .forTopic(TOPIC)
            .build();

        EventConsumer eventConsumer = EventConsumer.with(
                consumerBootstrap,
                (_, value) -> values.add(value))
            .customProperties(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
            .build();

        producer.send(new ProducerRecord<>(TOPIC, "key", buildTestEvent())).get();

        eventConsumer.run();

        Awaitility
            .await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(values.isEmpty()).isFalse();
                eventConsumer.stop(); // Closes polling thread.
            });
    }

    @Test
    void consumer_should_handle_duplicates() {

        List<Event> values = new ArrayList<>();

        // Creates event, producer, consumer and DLQ topic.
        ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-producer",
                new PlainTextSecurity())
            .build();

        String dlqTopic = getDlqTopic(null, TOPIC);

        // Event must be created once to share the same timestamp.
        Event event = buildTestEvent();

        createTopic(dlqTopic, 1, (short) 1);

        // Uses an EventProducer to produce with correct headers.
        EventProducer eventProducer = EventProducer.with(producerBootstrap).build();

        ConsumerBootstrap consumerBootstrap = ConsumerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-consumer",
                new PlainTextSecurity(),
                "test-eventconsumer-group")
            .forTopic(TOPIC)
            .build();

        EventConsumer eventConsumer = EventConsumer.with(
                consumerBootstrap,
                (_, value) -> values.add(value))
            .customProperties(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
            .enableIdempotencyCheck()
            .idempotencyKeyStore(new InMemoryIdempotencyKeyStore())
            .dlqTopic(dlqTopic)
            .build();

        // Subscribes to DLQ topic.
        consumer.subscribe(List.of(dlqTopic));

        // Publishes same event twice.
        eventProducer.publish(TOPIC, "key", event);
        eventProducer.publish(TOPIC, "key", event);

        eventConsumer.run();

        // Asserts duplicate has been consumed and published to DLQ.
        Awaitility
            .await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(values.size()).isEqualTo(2);

                ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));
                assertThat(records.records(dlqTopic).iterator().hasNext()).isTrue();

                eventConsumer.stop(); // Closes polling thread.
            });
    }
}
