import org.apache.nifi.serialization.record.*;

import java.util.*;

public class NiFiRecord implements Record {

    Map<String, Object> recordValues = new HashMap<>();

    @Override
    public RecordSchema getSchema() {
        return new RecordSchema() {
            @Override
            public List<RecordField> getFields() {
                return null;
            }

            @Override
            public int getFieldCount() {
                return 0;
            }

            @Override
            public RecordField getField(int index) {
                return null;
            }

            @Override
            public List<DataType> getDataTypes() {
                return null;
            }

            @Override
            public List<String> getFieldNames() {
                return new ArrayList<>(recordValues.keySet());
//                List<String> fields = new ArrayList<>();
//                fields.add("individual_id");
//                fields.add("weight");
//                fields.add("sector");
//                fields.add("viewer_type");
//                fields.add("sample_type");
//                fields.add("demographic");
//                return fields;
            }

            @Override
            public Optional<DataType> getDataType(String fieldName) {
                return Optional.empty();
            }

            @Override
            public Optional<String> getSchemaText() {
                return Optional.empty();
            }

            @Override
            public Optional<String> getSchemaFormat() {
                return Optional.empty();
            }

            @Override
            public Optional<RecordField> getField(String fieldName) {
                return Optional.empty();
            }

            @Override
            public SchemaIdentifier getIdentifier() {
                return null;
            }
        };
    }

    @Override
    public boolean isTypeChecked() {
        return false;
    }

    @Override
    public boolean isDropUnknownFields() {
        return false;
    }

    @Override
    public void incorporateSchema(RecordSchema other) {

    }

    @Override
    public Object[] getValues() {
        return recordValues.values().toArray();
    }

    @Override
    public Object getValue(String fieldName) {
        return recordValues.get(fieldName);
    }

    @Override
    public Object getValue(RecordField field) {
        return null;
    }

    @Override
    public String getAsString(String fieldName) {
        return null;
    }

    @Override
    public String getAsString(String fieldName, String format) {
        return null;
    }

    @Override
    public String getAsString(RecordField field, String format) {
        return null;
    }

    @Override
    public Long getAsLong(String fieldName) {
        return null;
    }

    @Override
    public Integer getAsInt(String fieldName) {
        return null;
    }

    @Override
    public Double getAsDouble(String fieldName) {
        return null;
    }

    @Override
    public Float getAsFloat(String fieldName) {
        return null;
    }

    @Override
    public Record getAsRecord(String fieldName, RecordSchema schema) {
        return null;
    }

    @Override
    public Boolean getAsBoolean(String fieldName) {
        return null;
    }

    @Override
    public Date getAsDate(String fieldName, String format) {
        return null;
    }

    @Override
    public Object[] getAsArray(String fieldName) {
        return new Object[0];
    }

    @Override
    public Optional<SerializedForm> getSerializedForm() {
        return Optional.empty();
    }

    @Override
    public void setValue(String fieldName, Object value) {
        recordValues.put(fieldName, value);
    }

    @Override
    public void setArrayValue(String fieldName, int arrayIndex, Object value) {

    }

    @Override
    public void setMapValue(String fieldName, String mapKey, Object value) {

    }

    @Override
    public Set<String> getRawFieldNames() {
        return null;
    }
}
