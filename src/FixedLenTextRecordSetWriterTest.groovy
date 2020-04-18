class FixedLenTextRecordSetWriterTest extends GroovyTestCase {
    void testWrite() {

        Map<String, String> variables = new HashMap<>()
        variables.put("output.format", "%09d%08.4f%1d%1d%1d%s")
        variables.put("output.columns", "individual_id,weight,sector,viewer_type,sample_type,demographic")

        //ByteArrayOutputStream out = new ByteArrayOutputStream()
        OutputStream out = new FileOutputStream("C:\\temp\\BDP\\test.dem")
        FixedLenTextRecordSetWriter fixedLenTextRecordSetWriter =  new FixedLenTextRecordSetWriter(out, variables)

        // 000538601009.98151111ѓ312
        NiFiRecord record = new NiFiRecord();
        record.setValue("individual_id", 538601)
        record.setValue("weight", 9.9815)
        record.setValue("sector", 1)
        record.setValue("viewer_type", 1)
        record.setValue("sample_type", 1)
        // char - 2 байта
        char[] demo = new char[5]
        demo[0] = (48+1) // код символа в десятичной системе
        demo[1] = (48+83)
        demo[2] = (48+3)
        demo[3] = (48+1)
        demo[4] = (48+2)
        String str = new String(demo)

        record.setValue("demographic", str)
        //record.setValue("demographic", "1ѓ312")
        fixedLenTextRecordSetWriter.write(record)

    }
}
