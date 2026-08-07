package org.plume.integration.config;

import org.testcontainers.utility.DockerImageName;

/**
 * Basic configuration for integration test cluster.
 */
public final class TestClusterConfig {

    private static final String KAFKA_VERSION = "4.2.0";
    private static final String SCHEMA_REGISTRY_VERSION = "8.2.2";

    public static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName
        .parse("apache/kafka")
        .withTag(KAFKA_VERSION);

    public static final DockerImageName SCHEMA_REGISTRY_IMAGE_NAME = DockerImageName
        .parse("confluentinc/cp-schema-registry")
        .withTag(SCHEMA_REGISTRY_VERSION);

    public static final Integer CLUSTER_PORT = 9092;
    public static final Integer CLUSTER_PORT_INTERNAL = 19092;
    public static final Integer SCHEMA_REGISTRY_PORT = 8081;
}
