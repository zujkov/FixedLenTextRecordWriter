@GrabConfig( systemClassLoader=true )

class TestFixedWriter {
    static void main(String[] args) {
//        GroovyRecordSetWriterFactory writer = new GroovyRecordSetWriterFactory()
        Map<String, String> variables = new HashMap<>()
        variables.put("output.format", "%09d%08.4f%1c%1d%1d%s");
        variables.put("output.locale", "ru_RU")
        variables.put("output.columns", "individual_id,weight,sector,viewer_type,sample_type,demographic")

        variables.put("header.first.row", "INFOSYS")
        variables.put("header.second.row", "TV")
        variables.put("header.resp.count", "11021")
        String vars = "sex_zdk,rage18,educ_zdk,hhold_zdk,work_zdk,ort_amount,rtr_amount,ntv_amount,sts_amount,tnt_amount,culture_amount,rentv_amount,tvc_amount,sport_amount,tv3_amount,domashii_amount,muztv_amount,mtv_amount,dtv_viasat_amount,tv7_amount,star_amount,pit5_amount,tv_wd_hml_prime,tv_wd_hml_nonprime,tv_we_hml_amount"
        variables.put("header.variable.list", vars)
        variables.put("header.all.weight", "68169439,158618")

//        variables.put("output.format", "%1d")
//        variables.put("output.columns", "sector")

        //ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream out = new FileOutputStream("C:\\temp\\BDP\\test123.dem")
        FixedLenTextRecordSetWriter fixedLenTextRecordSetWriter =  new FixedLenTextRecordSetWriter(out, variables)
        fixedLenTextRecordSetWriter.beginRecordSet()

        // 000538601009.98151111ѓ312
        NiFiRecord record = new NiFiRecord()
        record.setValue("individual_id", 538601)
        record.setValue("weight", 9.9815)
        record.setValue("sector", 37)
        record.setValue("viewer_type", 1)
        record.setValue("sample_type", 1)
        // char - 2 байта
        char[] demo = new char[5]
        demo[0] = (48+1); // код символа в десятичной системе
        demo[1] = (48+83);
        demo[2] = (48+3);
        demo[3] = (48+1);
        demo[4] = (48+2);
        String str = new String(demo);

        record.setValue("demographic", str)
        //record.setValue("demographic", "1ѓ312")

//        int sector = 1;
//        record.setValue("sector", sector)
        fixedLenTextRecordSetWriter.write(record)

//        String resultString = new String(out.toByteArray(), "windows-1251");
//        System.out.println(resultString);

    }
}
