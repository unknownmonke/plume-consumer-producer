package org.plume.integration.common;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.plume.event.Event;
import org.plume.integration.config.TestConsumerProperties;
import org.plume.integration.config.TestProducerProperties;
import org.plume.serialization.SchemaUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.plume.integration.config.TestClusterConfig.*;

@Testcontainers
public abstract class AbstractIT {

    protected static final String TOPIC = "test-topic";
    protected static final String EVENT_SUBJECT_NAME = TOPIC + "-value";
    private static final Network NETWORK = Network.newNetwork();

    protected KafkaProducer<String, Event> producer;
    protected KafkaConsumer<String, Event> consumer;
    protected AdminClient adminClient;
    protected HttpClient httpClient;
    protected SchemaRegistryClient schemaRegistryClient;


    @Container
    protected static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE_NAME)
        .withStartupTimeout(Duration.ofSeconds(10))         // Ensures Kafka is ready before tests run.
        .withNetwork(NETWORK)                               // Each test suite runs in its own Docker network.
        .withListener("kafka-container:" + CLUSTER_PORT_INTERNAL)
        .withExposedPorts(CLUSTER_PORT);

    @Container
    protected static final GenericContainer<?> SCHEMA_REGISTRY_CONTAINER =
        new GenericContainer<>(SCHEMA_REGISTRY_IMAGE_NAME)
            .withStartupTimeout(Duration.ofSeconds(10))
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(SCHEMA_REGISTRY_PORT)
            .dependsOn(KAFKA_CONTAINER)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://kafka-container:" + CLUSTER_PORT_INTERNAL)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));


    @BeforeEach
    void setup() {
        setupClients();
        setupSchemaRegistryClient();
        createTestTopic();
    }

    @AfterEach
    void teardown() {
        if (producer != null) producer.close();
        if (consumer != null) consumer.close();
        resetTopics();
    }

    private void setupClients() {
        producer = new KafkaProducer<>(TestProducerProperties.getProperties(KAFKA_CONTAINER.getBootstrapServers()));
        consumer = new KafkaConsumer<>(TestConsumerProperties.getProperties(KAFKA_CONTAINER.getBootstrapServers()));

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);

        httpClient = HttpClient.newHttpClient();
    }

    /* ------------------------------------------------------------------------ */
    /*                              SCHEMA REGISTRY                             */
    /* ------------------------------------------------------------------------ */

    protected String getSchemaRegistryUrl() {
        return "http://localhost:" + SCHEMA_REGISTRY_CONTAINER.getMappedPort(SCHEMA_REGISTRY_PORT);
    }

    protected void setupSchemaRegistryClient() {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 10);
    }

    /**
     * Registers a client and uploads event schema before tests.
     */
    protected RegisterSchemaResponse registerAvroEventSchemaWithResponse() {
        try {
            ParsedSchema schema = SchemaUtils.getParsedSchema();
            return schemaRegistryClient.registerWithResponse(EVENT_SUBJECT_NAME, schema, false);

        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Error registering Avro Event schema: ", e);
        }
    }

    protected void registerAvroEventSchema() {
        try {
            ParsedSchema schema = SchemaUtils.getParsedSchema();
            schemaRegistryClient.register(EVENT_SUBJECT_NAME, schema);

        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Error registering Avro Event schema: ", e);
        }
    }

    /* ------------------------------------------------------------------------ */
    /*                                  TOPICS                                  */
    /* ------------------------------------------------------------------------ */

    private void createTestTopic() {
        createTopic(TOPIC, 1, (short) 1);
    }

    protected void createTopic(String name, int partitions, short replicationFactor) {
        try {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor);
            adminClient.createTopics(List.of(topic)).all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error creating topics", e);
        }
    }

    protected void resetTopics() {
        try {
            List<String> topics = adminClient.listTopics().names().get().stream().toList();
            adminClient.deleteTopics(topics).all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error deleting topics", e);
        }
    }
}
