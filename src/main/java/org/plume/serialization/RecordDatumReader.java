package org.plume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;

/**
 * Allows to serialize and deserialize Java records with Avro, maintaining immutability and simplicity.
 * Extends existing reflection reader implementation.
 */
public class RecordDatumReader<T extends Record> extends ReflectDatumReader<T> {

    public RecordDatumReader(Schema schema, RecordData data) {
        super(schema, schema, data);
    }

    protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
        Object o = super.readRecord(old, expected, in);

        if (o instanceof GenericData.Record record) {
            return toJavaRecord(record);
        }
        return o;
    }

    private T toJavaRecord(GenericData.Record record) {
        Class<?> recordClass = data().getClass(record.getSchema());

        try {
            // Fields of record class.
            RecordComponent[] recordComponents = recordClass.getRecordComponents();

            // Will contain actual field values.
            Object[] components = new Object[recordComponents.length];
            Class<?>[] constructorTypes = new Class[recordComponents.length];

            for (int i = 0; i < recordComponents.length; i++) {
                components[i] = record.get(recordComponents[i].getName());
                constructorTypes[i] = recordComponents[i].getType();
            }
            Constructor<?> constructor = recordClass.getConstructor(constructorTypes);
            return (T) constructor.newInstance(components);

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private RecordData data() {
        return (RecordData) getData();
    }
}
