<img src="assets/plume_logo-large.webp" alt="drawing"/>

<br>

# <img src="assets/plume_logo.webp" alt="drawing" width="20"/> Plume Consumers and Producers

## Summary

- [Overview](#overview-)
    - [Features](#features-)
- [Get Started](#get-started-)
    - [Create a producer](#create-a-producer-)
    - [Create a consumer](#create-a-consumer-)
    - [Enable schema validation](#enable-schema-validation-)
- [Producer Lifecycle](#producer-lifecycle-)
- [Consumer Lifecycle](#consumer-lifecycle-)

<br>

## Overview [🔼](#summary)

- Wrapper library for generic Kafka producers and consumers in java.

- Provides **opinionated handling** for recurring concerns, such as schema validation, duplicates, security.

- Hides boilerplate code and simplifies creation of consumers and producers : Shutdown hooks, processing logic...

#
### Features [🔼](#summary)

- Simplified creation for consumers and producers :

    - **Startup and shutdown hooks** : Built-in support for default and user provided.

    - **Default configuration** :

        - Additional custom configuration can be supplied, but default configuration **cannot be overriden**.

    - **Security** : Simple configuration for different protocols.

    - **Builders** : Mandatory and optional parameters.

- Common `Event` class wraps message payload with *standard metadata*.

- <ins>Optional built-in features</ins> :

    - **Exactly-once processing** : Built-in **idempotency** mechanism to detect and handle duplicates.

    - **Avro schema validation** : Custom serdes.

    - **Error, DLQ and replay** handling.

    - **Client-Side Field Level Encryption** (**CSFLE**) : Support for Hashicorp Vault.

<br>

## Get Started [🔼](#summary)

### Create a producer [🔼](#summary)

``` java
// 1. Create a bootstrap with broker addresses, client id and security config.
ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
        bootstrapServers,
        "client-producer",
        new PlainTextSecurity()
    ).build();

// 2. Create the producer with bootstrap.
EventProducer eventProducer = EventProducer.with(producerBootstrap).build();

// 3. Publish.
eventProducer.publish(TOPIC, "key", event).get();
```
#
### Create a consumer [🔼](#summary)

``` java
// 1. Create a bootstrap with broker addresses, client id, security config, consumer group and topics.
ConsumerBootstrap consumerBootstrap = ConsumerBootstrap.with(
        bootstrapServers,
        "client-consumer",
        new PlainTextSecurity(),
        "eventconsumer-group")
    .forTopic(TOPIC)
    .build();

// 2. Create consumer by providing the processing logic.
EventConsumer eventConsumer = EventConsumer.with(
        consumerBootstrap,
        (_, record) -> processRecord(record)) // User-specific process logic.
    .customProperties(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
    .build();

// 3. Launch consumer polling thread.
eventConsumer.run();
```
#
### Enable schema validation [🔼](#summary)

``` java
// 1. Specify registry credentials and schema type.
SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(
    getSchemaRegistryUrl(),
    "user",
    "pwd",
    SchemaType.AVRO
);

// 2. Specify consumers & producers bootstraps with registry configuration.
ProducerBootstrap producerBootstrap = ProducerBootstrap.with(
        bootstrapServers,
        "client-producer",
        new PlainTextSecurity())
    .withSchemaValidation(schemaRegistryConfig)
    .build();
```
<br>

## Producer Lifecycle [🔼](#summary)

``` mermaid
---
config:
  layout: dagre
---
flowchart TD
    A[Emits] --> B[Builds record]
    B --> ID{Idempotency enabled ?}
    ID -- Yes --> ID1[Builds idempotency hash]
    ID1 --> ID2{Duplicate in store ?}
    ID -- No --> D[Serialization]
    ID2 -- Yes --> DLQ[DLQ topic]
    ID2 -- No --> D
    D --> SCH{Entity check active ?}
    SCH -- Yes --> SCH1[Schema validation]
    SCH -- No --> F[Publishes]
    SCH1 --> F
    F --> FA{{On success}}
    FA --> ID3{Idempotency enabled ?}
    ID3 -- Yes --> ID34[Persists hash in IdempotencyKeyStore]

    style D fill:#fbc,color:#212
    style DLQ fill:#f90,color:#fff
    style F fill:#6c6,color:#fff
    style FA fill:#fcf9a4,color:#212
```
<br>

## Consumer Lifecycle [🔼](#summary)

``` mermaid
---
config:
  layout: dagre
---
flowchart TD
    A[Receives record] --> SCH{Entity check active ?}
    SCH -- Yes --> SCH1[Schema validation]
    SCH -- No --> B[Deserialization]
    SCH1 --> SCH2{Error ?}
    SCH2 -- Yes --> SCH3[Error topic]
    SCH2 -- No --> B
    B --> BC{Error ?}
    BC -- Yes --> E[Error topic]
    BC -- No --> ID{Idempotency enabled ?}
    ID -- Yes --> ID1[Builds idempotency hash]
    ID1 --> ID2{Duplicate in store ?}
    ID -- No --> D[Processing]
    ID2 -- Yes --> DLQ[DLQ topic]
    ID2 -- No --> D
    D --> ID3{Idempotency enabled ?}
    ID3 -- Yes --> ID34[Persists hash in IdempotencyKeyStore]

    style DLQ fill:#f90,color:#fff
    style B fill:#fbc,color:#212
    style D fill:#6c6,color:#fff
    style E fill:#f20,color:#fff
    style SCH3 fill:#f20,color:#fff
```
<br>