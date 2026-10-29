package org.plume.idempotency;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.plume.event.Event;

import java.util.List;
import java.util.Optional;

/**
 * Interface for storing and retrieving idempotency keys.
 *
 * <p> This allows to check whether a specific event has been seen and processed before,
 * ensuring that duplicate events are not processed multiple times.
 *
 * <p> Clients can implement this interface to provide their own storage mechanism.
 */
public interface IdempotencyKeyStore {

    boolean exists(ConsumerRecord<String, Event> consumerRecord, String groupId);

    boolean exists(ProducerRecord<String, Event> producerRecord);

    Optional<IdempotencyKey> find(ConsumerRecord<String, Event> consumerRecord, String groupId);

    Optional<IdempotencyKey> find(ProducerRecord<String, Event> producerRecord);

    List<IdempotencyKey> findAll(String key, String topic, String groupId);

    void save(ConsumerRecord<String, Event> consumerRecord, String groupId);

    void save(ProducerRecord<String, Event> producerRecord);
}
