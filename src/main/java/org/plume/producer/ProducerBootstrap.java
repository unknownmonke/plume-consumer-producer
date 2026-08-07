package org.plume.producer;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.StringSerializer;
import org.plume.common.AbstractBootstrap;
import org.plume.security.Security;
import org.plume.serialization.EventAvroSerializer;
import org.plume.serialization.EventSerializer;
import org.plume.serialization.SchemaRegistryConfig;
import org.plume.serialization.SchemaType;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.plume.common.Constants.ACKS;

/**
 * Encapsulates required and common configuration for producers.
 *
 * <p> Just extends {@code AbstractConfig} without adding anything for now.
 */
@ToString(callSuper = true)
public class ProducerBootstrap extends AbstractBootstrap {

    private ProducerBootstrap(String bootstrapServers, String clientId,
                              Security security, SchemaRegistryConfig schemaRegistryConfig) {
        super(bootstrapServers, clientId, security, schemaRegistryConfig);
    }


    public static ProducerBootstrapBuilder with(String bootstrapServers, String clientId, Security security) {
        return new ProducerBootstrapBuilder(bootstrapServers, clientId, security);
    }

    @Override
    public Properties properties() {
        Properties properties = super.properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ACKS_CONFIG, ACKS);

        // Only Avro supported for now.
        if (super.getSchemaRegistryConfig() != null && super.getSchemaRegistryConfig().schemaType() == SchemaType.AVRO) {
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, EventAvroSerializer.class);
        } else {
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class);
        }
        return properties;
    }

    /* ------------------------------------------------------------------------ */

    /**
     * Restricted builder for optional arguments only.
     */
    @RequiredArgsConstructor
    public static class ProducerBootstrapBuilder {

        private final String bootstrapServers;
        private final String clientId;
        private final Security security;

        private SchemaRegistryConfig schemaRegistryConfig;


        public ProducerBootstrapBuilder withSchemaValidation(SchemaRegistryConfig schemaRegistryConfig) {
            this.schemaRegistryConfig = schemaRegistryConfig;
            return this;
        }

        public ProducerBootstrap build() {
            return new ProducerBootstrap(bootstrapServers, clientId, security, schemaRegistryConfig);
        }
    }
}
