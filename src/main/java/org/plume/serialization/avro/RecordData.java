package org.plume.serialization.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.plume.event.Event;
import org.plume.serialization.SchemaUtils;

import java.lang.reflect.Type;

public class RecordData extends ReflectData {

    /**
     * Overrides {@link ReflectData#newRecord} to force return of a generic record.
     */
    @Override
    public Object newRecord(Object old, Schema schema) {
        return new GenericData.Record(schema);
    }

    /**
     * Overrides {@link org.apache.avro.specific.SpecificData#getSchema SpecificData.getSchema}
     * method to return {@link Event} schema explicitly.
     *
     * <p> Otherwise, Avro code will attempt to build schema for {@link Event#origin()} field,which is a record itself, and will fail.
     */
    @Override
    public Schema getSchema(Type type) {
        if (type.equals(Event.class)) {
            return SchemaUtils.getEventSchema();
        }
        return super.getSchema(type);
    }
}