package org.plume.integration.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.plume.event.Event;
import org.plume.integration.config.TestClusterConfig;
import org.plume.integration.config.TestConsumerProperties;
import org.plume.integration.config.TestProducerProperties;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Testcontainers
public abstract class AbstractIT {

    protected static final String TOPIC = "test-topic";

    protected KafkaProducer<String, Event> producer;
    protected KafkaConsumer<String, Event> consumer;
    protected AdminClient adminClient;

    @Container
    protected final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(TestClusterConfig.KAFKA_IMAGE_NAME)
        .withNetwork(Network.newNetwork())                  // Each test suite runs in its own Docker network.
        .withStartupTimeout(Duration.ofSeconds(10))         // Ensures Kafka is ready before tests run.
        .withExposedPorts(TestClusterConfig.CLUSTER_PORT);


    @BeforeEach
    void setup() {
        setupClients();
        createTestTopic();
    }

    @AfterEach
    void teardown() {
        if (producer != null) producer.close();
        if (consumer != null) consumer.close();
    }

    private void setupClients() {
        producer = new KafkaProducer<>(TestProducerProperties.getProperties(KAFKA_CONTAINER.getBootstrapServers()));
        consumer = new KafkaConsumer<>(TestConsumerProperties.getProperties(KAFKA_CONTAINER.getBootstrapServers()));

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);
    }

    private void createTestTopic() {
        createTopic(TOPIC, 1, (short) 1);
    }

    public void createTopic(String name, int partitions, short replicationFactor) {
        try {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor);
            adminClient.createTopics(List.of(topic)).all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error creating topics", e);
        }
    }
}
