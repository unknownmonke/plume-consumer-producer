package org.plume.integration.common;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.plume.event.Event;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.plume.event.TestEventFactory.buildTestEvent;

public class SetupHealthTest extends AbstractIT {

    @Test
    void should_init_cluster() throws ExecutionException, InterruptedException {

        // Verifies container is running.
        assert KAFKA_CONTAINER.isRunning();

        // Verifies producer is up.
        producer.send(new ProducerRecord<>(TOPIC, "key", buildTestEvent())).get();

        // Verifies consumer is up.
        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));
        assert records.count() > 0;

        // Verifies admin client is up.
        Set<String> topics = adminClient.listTopics().names().get();
        assert topics.contains(TOPIC);
    }

    @Test
    void should_init_schema_registry() {
        assert SCHEMA_REGISTRY_CONTAINER.isRunning();
    }

    @Test
    void should_register_schema() {
        try {
            RegisterSchemaResponse response = registerAvroEventSchemaWithResponse();
            ParsedSchema parsedSchema = schemaRegistryClient.getSchemaBySubjectAndId(EVENT_SUBJECT_NAME, response.getId());

            assertThat(parsedSchema).isNotNull();

        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Error registering Avro Event schema: ", e);
        }
    }
}
