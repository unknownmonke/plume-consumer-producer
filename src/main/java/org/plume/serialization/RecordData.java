package org.plume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;

public class RecordData extends ReflectData {

    @Override
    public Object newRecord(Object old, Schema schema) {
        return new GenericData.Record(schema);
    }
}