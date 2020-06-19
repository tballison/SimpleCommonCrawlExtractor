package org.tallison.cc.index;

import org.junit.Test;

import java.util.List;

public class TestCCIndexRecord {
    @Test
    public void testRecordParser() throws Exception {
        List<CCIndexRecord> records =
                CCIndexRecord.parseRecords("com,uk,bug)/download.php?id=bugbaun2017/" +
                        "j%20taylor.pdf 20200220040220 {\"url\":" +
                        " \"https://www.bug.uk.com/download.php?id=bugbaun2017/J%20Taylor.pdf\", " +
                        "\"mime\": \"application/{$ext[1]}\", \"mime-detected\": " +
                        "\"application/pdf\", \"status\": \"200\", \"digest\": " +
                        "\"QVSYUFIAO5R3MEI4HO3CUFK4EU6VLJUD\", \"length\": \"678822\"," +
                        " \"offset\": \"687777403\", \"filename\":" +
                        " \"crawl-data/CC-MAIN-2020-10/segments/1581875144637.88/warc/CC-MAIN-20200220035657-20200220065657-00105.warc.gz\"," +
                        " \"truncated\": \"length\"}");
        System.out.println(records);
    }
}
