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

public class CountExtByMime extends AbstractRecordProcessor {

    private Map<String, MimeCounts> map = new HashMap<>();
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
        System.out.println("CountExtByMime <output_directory>");
    }


    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = CCIndexRecord.parseRecords(row);

        for (CCIndexRecord r : records) {
            String u = r.getUrl();
            if (u == null)
                continue;
            String ext = getExtension(u);
            ext = (ext == null) ? "NULL" : ext;

            String mime = CCIndexRecord.normalizeMime(r.getMime());
            mime = (mime == null) ? "NULL" : mime;

            MimeCounts mimeCounts = map.get(ext);
            if (mimeCounts == null) {
                mimeCounts = new MimeCounts();
            }
            mimeCounts.increment(mime);
            map.put(ext, mimeCounts);

        }
    }



    @Override
    public void close() throws IOException {
        map = MapUtil.sortByValueDesc(map);
        for (Map.Entry<String, MimeCounts> e : map.entrySet()) {
            String ext = e.getKey();
            MimeCounts mimeCounts = e.getValue();
            mimeCounts.m = MapUtil.sortByValueDesc(mimeCounts.m);
            for (Map.Entry<String, Integer> e2 : mimeCounts.m.entrySet()) {
                writer.write(clean(ext) + "\t" + clean(e2.getKey()) + "\t" + e2.getValue()+"\n");
            }
        }
        writer.flush();
        writer.close();
    }

}
