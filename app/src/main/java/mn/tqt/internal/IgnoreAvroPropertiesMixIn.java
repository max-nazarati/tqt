package mn.tqt.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;

abstract class IgnoreAvroPropertiesMixIn {
    @JsonIgnore
    public abstract org.apache.avro.Schema getSchema();

    @JsonIgnore
    public abstract org.apache.avro.specific.SpecificData getSpecificData();

    @JsonIgnore
    public abstract org.apache.avro.generic.GenericData getGenericData();
}
