package org.plume.idempotency;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.plume.event.Event;

import java.time.Instant;
import java.util.*;

import static org.plume.event.EventHeaders.IDEMPOTENCY_KEY;
import static org.plume.event.EventHeaders.TIMESTAMP;

/**
 * Default implementation of an idempotency key store using an {@code HashSet} to store and retrieve keys.
 */
public class InMemoryIdempotencyKeyStore implements IdempotencyKeyStore {

    private final Set<IdempotencyKey> store = new HashSet<>();


    @Override
    public Optional<IdempotencyKey> exists(ConsumerRecord<String, Event> consumerRecord, String groupId) {
        IdempotencyKey key = createIdempotencyKey(consumerRecord, groupId);
        return store.stream().filter(key::equals).findFirst();
    }

    @Override
    public Optional<IdempotencyKey> exists(ProducerRecord<String, Event> producerRecord) {
        IdempotencyKey key = createIdempotencyKey(producerRecord);
        return store.stream().filter(key::equals).findFirst();
    }

    @Override
    public void save(ConsumerRecord<String, Event> consumerRecord, String groupId) {
        IdempotencyKey key = createIdempotencyKey(consumerRecord, groupId);
        store.add(key);
    }

    @Override
    public void save(ProducerRecord<String, Event> producerRecord) {
        IdempotencyKey key = createIdempotencyKey(producerRecord);
        store.add(key);
    }

    @Override
    public List<IdempotencyKey> search(String key, String topic, String groupId) {
        return store.stream()
                .filter(k -> k.key().equals(key)
                    && k.topic().equals(topic)
                    && k.groupId().equals(groupId))
                .toList();
    }

    private IdempotencyKey createIdempotencyKey(ConsumerRecord<String, Event> consumerRecord, String groupId) {
        return new IdempotencyKey(
            consumerRecord.key(),
            consumerRecord.topic(),
            new String(consumerRecord.headers().lastHeader(IDEMPOTENCY_KEY).value()),
            Instant.parse(new String(consumerRecord.headers().lastHeader(TIMESTAMP).value())),
            groupId
        );
    }

    private IdempotencyKey createIdempotencyKey(ProducerRecord<String, Event> producerRecord) {
        return new IdempotencyKey(
            producerRecord.key(),
            producerRecord.topic(),
            new String(producerRecord.headers().lastHeader(IDEMPOTENCY_KEY).value()),
            Instant.parse(new String(producerRecord.headers().lastHeader(TIMESTAMP).value()))
        );
    }
}
