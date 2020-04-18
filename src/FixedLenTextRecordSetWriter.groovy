//@Grapes(value = [
//        @Grab(group = 'commons-io', module = 'commons-io', version = '2.6'),
//        @Grab(group = 'charset', module = 'charset', version = '1.2.1'),
//        @Grab(group = 'org.apache.nifi', module = 'nifi-api', version = '1.8.0'),
//        @Grab(group = 'org.apache.nifi', module = 'nifi-record', version = '1.8.0'),
//        @Grab(group = 'org.apache.nifi', module = 'nifi-utils', version = '1.8.0'),
//        @Grab(group = 'org.apache.nifi', module = 'nifi-record-serialization-service-api', version = '1.8.0'),
//])

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.RecordSetWriter
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.WriteResult
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.RecordSet

class FixedLenTextRecordSetWriter implements RecordSetWriter {
    private int recordCount = 0
    private final OutputStream out
    private final String outputFormat
    private final Set<String> outputColumns

    FixedLenTextRecordSetWriter(final OutputStream out, Map<String, String> variables) {
        this.out = out
        this.outputFormat = variables.get("output.format")
        this.outputColumns = variables.get("output.columns").split(',')
    }

    @Override
    WriteResult write(Record record) throws IOException {
        Set<String> outputFieldNames = record.schema.fieldNames.intersect(outputColumns)
        printRow(out, record, outputFieldNames, outputFormat)
        recordCount++
        WriteResult.of(1, [:])
    }

    @Override
    String getMimeType() { 'application/octet-stream' }

    @Override
    WriteResult write(final RecordSet recordSet) throws IOException {
        int count = 0

        Record record
        while (record = recordSet.next()) {
            count++
            Set<String> outputFieldNames = record.schema.fieldNames.intersect(outputColumns)
            printRow(out, record, outputFieldNames, outputFormat)
        }

        WriteResult.of(count, [:])
    }

    private static void printRow(OutputStream out, Record r, Set<String> fieldNames, String outputFormat) {
        List<Object> values = new ArrayList<>()
        fieldNames.each { fieldName -> values.add(r.getValue(fieldName)) }
        Object[] vs = values.toArray()
        byte[] row = String.format(Locale.US, outputFormat, vs).bytes
        String rowString = new String(row)

        int len = rowString.length()
        for (int i = 0 ; i < len ; i++) {
            out.write((byte)rowString.charAt(i))
        }

        out.write("\r\n".getBytes())
    }

    void beginRecordSet() throws IOException {}

    @Override
    WriteResult finishRecordSet() throws IOException {
        WriteResult.of(recordCount, [:])
    }

    @Override
    void close() throws IOException {}

    @Override
    void flush() throws IOException { out.flush() }
}

class GroovyRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
    Map<String, String> variables

    @Override
    RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        this.variables = variables
        null
    }

    @Override
    RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out) throws SchemaNotFoundException, IOException {
        new FixedLenTextRecordSetWriter(out, variables)
    }
}
