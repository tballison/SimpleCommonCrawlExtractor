package org.tallison.cc.index.mappers;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tallison.cc.index.CCIndexRecord;
import org.tallison.utils.MapUtil;

public class CountMimeByExt extends AbstractRecordProcessor {

    private Map<String, ExtCounts> map = new HashMap<>();
    private Writer writer;

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        Path targFile = Paths.get(args[0]).resolve("mime_counts_"+getThreadNumber()+".txt");
        Files.createDirectories(targFile.getParent());
        writer = Files.newBufferedWriter(targFile,
                StandardCharsets.UTF_8);

    }

    @Override
    public void usage() {
        System.out.println("CountMimeByExt <output_directory>");
    }

    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = parseRecords(row);

        for (CCIndexRecord r : records) {
            String u = r.getUrl();
            if (u == null)
                continue;
            String ext = getExtension(u);
            ext = (ext == null) ? "NULL" : ext;

            String mime = CCIndexRecord.normalizeMime(r.getMime());
            mime = (mime == null) ? "NULL" : mime;

            ExtCounts extCounts = map.get(mime);
            if (extCounts == null) {
                extCounts = new ExtCounts();
            }
            extCounts.increment(ext);
            map.put(mime, extCounts);

        }
    }



    @Override
    public void close() throws IOException {
        map = MapUtil.sortByValueDesc(map);
        for (Map.Entry<String, ExtCounts> e : map.entrySet()) {
            String ext = e.getKey();
            ExtCounts extCounts = e.getValue();
            extCounts.m = MapUtil.sortByValueDesc(extCounts.m);
            for (Map.Entry<String, Integer> e2 : extCounts.m.entrySet()) {
                writer.write(ext + "\t" + e2.getKey() + "\t" + e2.getValue()+"\n");
            }
        }
        writer.flush();
        writer.close();
    }

    private class ExtCounts implements Comparable<ExtCounts> {
        private int total = 0;
        Map<String, Integer> m = new HashMap<>();
        void increment(String mime) {
            Integer cnt = m.get(mime);
            if (cnt == null) {
                cnt = new Integer(1);
            } else {
                cnt++;
            }
            m.put(mime, cnt);
            total++;
        }
        int getTotal() {
            return total;
        }


        @Override
        public int compareTo(ExtCounts o) {
            return new Integer(total).compareTo(new Integer(o.total));
        }
    }
}
