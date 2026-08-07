package org.plume.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.plume.security.Security;
import org.plume.serialization.SchemaRegistryConfig;
import org.plume.serialization.SchemaType;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

/**
 * Base class for configuration required by both producers & consumers :
 *
 * <li> Bootstrap servers.
 * <li> Client ID.
 * <li> Security.
 * <li> Schema registry configuration.
 */
@ToString
@Getter
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractBootstrap {

    final String bootstrapServers;
    final String clientId;
    final Security security;
    final SchemaRegistryConfig schemaRegistryConfig;


    public Properties properties() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        properties.put(CLIENT_ID_CONFIG, this.clientId + "-" + randomUUID());
        properties.putAll(security.securityConfig());

        if (schemaRegistryConfig != null && schemaRegistryConfig.schemaType() == SchemaType.AVRO) {
            // Registry url & auth.
            properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryConfig.url());
            properties.put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            properties.put(USER_INFO_CONFIG, schemaRegistryConfig.getUserInfo());

            // Schema must be registered at topic creation.
            properties.put(AUTO_REGISTER_SCHEMAS, false);
            properties.put(USE_LATEST_VERSION, true);
        }
        return properties;
    }
}
