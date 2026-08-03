package org.plume.integration.producer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.plume.event.Event;
import org.plume.idempotency.InMemoryIdempotencyKeyStore;
import org.plume.idempotency.hash.Base64HashGenerator;
import org.plume.integration.common.AbstractIT;
import org.plume.producer.EventProducer;
import org.plume.producer.ProducerBootstrap;
import org.plume.security.PlainTextSecurity;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.plume.common.Constants.getDlqTopic;
import static org.plume.event.TestEventFactory.buildTestEvent;

public class EventProducerTest extends AbstractIT {

    @Test
    void producer_should_publish_event() throws ExecutionException, InterruptedException {

        ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-producer",
                new PlainTextSecurity()
            ).build();

        EventProducer eventProducer = EventProducer.with(producerBootstrap).build();

        eventProducer.publish(TOPIC, "key", buildTestEvent()).get();

        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));
        assert records.count() > 0;
    }

    @Test
    void producer_should_handle_duplicates() throws ExecutionException, InterruptedException {

        // Creates event, producer and DLQ topic.
        ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
                KAFKA_CONTAINER.getBootstrapServers(),
                "client-producer",
                new PlainTextSecurity())
            .build();

        String dlqTopic = getDlqTopic(null, TOPIC);

        // Event must be created once to share the same timestamp.
        Event event = buildTestEvent();

        createTopic(dlqTopic, 1, (short) 1);

        EventProducer eventProducer = EventProducer.with(producerBootstrap)
            .hashGenerator(new Base64HashGenerator())
            .enableIdempotencyCheck()
            .idempotencyKeyStore(new InMemoryIdempotencyKeyStore())
            .dlqTopic(dlqTopic)
            .build();

        // Subscribes to all topics.
        consumer.subscribe(List.of(TOPIC, dlqTopic));

        // Publishes same event twice.
        eventProducer.publish(TOPIC, "key", event).get();
        eventProducer.publish(TOPIC, "key", event).get();

        // Asserts published duplicate has been published to DLQ.
        ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));

        assertThat(records.records(TOPIC).iterator().hasNext()).isTrue();
        assertThat(records.records(dlqTopic).iterator().hasNext()).isTrue();
    }
}
