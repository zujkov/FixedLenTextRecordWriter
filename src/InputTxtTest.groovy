import java.time.LocalDateTime

@GrabConfig(systemClassLoader = true)

class InputTxtTest {
    static void main(String[] args) {

        Map<String, String> variables = new HashMap<>()
        //variables.put("output.format", "%d; %f; %s; %s")
        variables.put("output.format", "%d; %f; %s; %s; %s; %s");
        //variables.put("output.columns", "resp_id,weight,sex_zdk,rage18")
        //variables.put("output.columns", "resp_id, weight, sex_zdk, rage18, hhold_zdk, educ_zdk")
        variables.put("output.columns", "resp_id,weight,sex_zdk,rage18,hhold_zdk,educ_zdk")

        variables.put("header.first.row", "INFOSYS")
        variables.put("header.second.row", "TV")
        variables.put("header.resp.count", "11021")
        String vars = "sex_zdk, rage18, hhold_zdk, educ_zdk"
        variables.put("header.variable.list", vars)
        variables.put("header.all.weight", "68169439,158618")
        variables.put("output.locale", "ru_RU")

        OutputStream out = new FileOutputStream("C:\\temp\\BDP\\input"+ LocalDateTime.now().format("yyyy-MM-dd-hh-mm-ss") +".txt")
        FixedLenTextRecordSetWriter fixedLenTextRecordSetWriter = new FixedLenTextRecordSetWriter(out, variables)
        fixedLenTextRecordSetWriter.beginRecordSet()

        BufferedReader br = new BufferedReader(new FileReader("C:\\temp\\BDP\\source_for_info.csv"))

        String[] headers
        Integer numLine = 1
        String line
        while ((line = br.readLine()) != null) {
            if (numLine == 1) {
                headers = line.split(";")
                numLine++
            } else {
                NiFiRecord record = new NiFiRecord()
                String[] values = line.split(";")
                for (int i = 0; i < values.length; i++) {
                    String header = headers[i]
                    if (header.equals("weight")) {
                        record.setValue(header, Double.parseDouble(values[i]))
                    } else if (header.equals("resp_id")) {
                        record.setValue(header, Integer.parseInt(values[i]))
                    } else {
                        record.setValue(header, values[i])
                    }
                }
                fixedLenTextRecordSetWriter.write(record)
            }
        }

        br.close()

//        NiFiRecord record = new NiFiRecord()
//        record.setValue("resp_id", 1541)
//        record.setValue("weight", 5190.199852)
//        record.setValue("sex_zdk", 2)
//        record.setValue("rage18", 5)
//
//        fixedLenTextRecordSetWriter.write(record)
//
//        record = new NiFiRecord()
//        record.setValue("resp_id", 1542)
//        record.setValue("weight", 5303.899765)
//        record.setValue("sex_zdk", 1)
//        record.setValue("rage18", 5)
//
//        fixedLenTextRecordSetWriter.write(record)

    }
}
