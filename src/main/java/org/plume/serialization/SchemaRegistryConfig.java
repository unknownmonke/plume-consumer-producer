package org.plume.serialization;

public record SchemaRegistryConfig(String url, String username, String password, SchemaType schemaType) {

    public String getUserInfo() {
        return username + ":" + password;
    }
}