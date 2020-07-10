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
    private final String headerFirstRow;
    private final String headerSecondRow;
    private final String headerRespCount;
    private final String headerVariableList;
    private final String headerAllWeights;
    private final Locale locale;

    FixedLenTextRecordSetWriter(final OutputStream out, Map<String, String> variables) {
        this.out = out
        this.outputFormat = variables.get("output.format")
        String[] locales = variables.get("output.locale").split("_")
        locale = new Locale(locales[0].toString(), locales[1].toString())

        this.outputColumns = variables.get("output.columns").split(',')

        this.headerFirstRow = variables.get("header.first.row")
        this.headerSecondRow = variables.get("header.second.row")
        this.headerRespCount = variables.get("header.resp.count")
        this.headerVariableList = variables.get("header.variable.list")
        this.headerAllWeights = variables.get("header.all.weight")
    }

    @Override
    WriteResult write(Record record) throws IOException {
        Set<String> outputFieldNames = record.schema.fieldNames.intersect(outputColumns)
        printRow(out, record, outputFieldNames, outputFormat, locale)
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
            printRow(out, record, outputFieldNames, outputFormat, locale)
        }

        WriteResult.of(count, [:])
    }

    private void printHeader(String... headerRows) {
        for (String headerRow : headerRows) {
            printRow(this.out, headerRow)
        }
    }

    private static void printRow(OutputStream out, String row) {
        int len = row.length();
        for (int i = 0; i < len; i++) {
            out.write((byte) row.charAt(i));
        }

        out.write("\r\n".getBytes())
    }

    private static void printRow(OutputStream out, Record r, Set<String> fieldNames, String outputFormat, Locale locale) {
        List<Object> values = new ArrayList<>()
        fieldNames.each { fieldName -> values.add(r.getValue(fieldName)) }
        Object[] vs = values.toArray()
        String[] formats = outputFormat.split("%")
        for (int i = 0; i < formats.length; i++) {
            if (formats[i].contains('c')) {
                Object charValue = vs[i - 1]
                if (charValue instanceof Integer) {
                    int value = (int) charValue
                    vs[i - 1] = value + 48
                }
            }
        }
        byte[] row = String.format(locale, outputFormat, vs).bytes
        String rowString = new String(row);

        int len = rowString.length();
        for (int i = 0; i < len; i++) {
            out.write((byte) rowString.charAt(i));
        }

        out.write("\r\n".getBytes())
    }

    public void beginRecordSet() throws IOException {
        printHeader(headerFirstRow, headerSecondRow, headerRespCount, headerVariableList, headerAllWeights)
    }

    @Override
    public WriteResult finishRecordSet() throws IOException {
        WriteResult.of(recordCount, [:])
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void flush() throws IOException { out.flush(); }
}

class GroovyRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
    Map<String, String> variables

    @Override
    RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        this.variables = variables;
        null
    }

    @Override
    RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out) throws SchemaNotFoundException, IOException {
        new FixedLenTextRecordSetWriter(out, variables)
    }
}

//writer = new GroovyRecordSetWriterFactory()
