package org.plume.serialization;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.plume.event.Event;
import org.plume.serialization.avro.RecordData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@Slf4j
public final class SchemaUtils {

    public final static RecordData DATA_INSTANCE = new RecordData();

    static {
        // Matches defined logical type.
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.TimestampNanosConversion());
    }

    /**
     * Gets schema from static schema definition.
     */
    public static Schema getEventSchema() {
        String schemaString = readEventSchema();
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    public static ParsedSchema getParsedSchema() {
        return new AvroSchema(getEventSchema());
    }

    private static String readEventSchema() {
        try (InputStream inputStream = Event.class.getResourceAsStream("/event.avsc")) {
            String result = null;

            if (inputStream != null) {
                result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .collect(Collectors.joining("\n"));
            }
            return result;

        } catch (IOException e) {
            log.error("Error reading event schema file: ", e);
            return null;
        }
    }
}
