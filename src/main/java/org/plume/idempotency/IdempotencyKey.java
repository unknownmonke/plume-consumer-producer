package org.plume.idempotency;

import java.time.Instant;

/**
 * Represents event unicity. Used to ensure that a message is processed only once.
 */
public record IdempotencyKey(String key, String topic, String groupId, String hash, Instant createdAt) {

    // For producers.
    public IdempotencyKey(String key, String topic, String hash, Instant createdAt) {
        this(key, topic, null, hash, createdAt);
    }
}
