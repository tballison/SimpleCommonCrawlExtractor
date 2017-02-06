/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.regex.Pattern;

import org.tallison.cc.index.CCIndexRecord;
import org.tallison.utils.MapUtil;

public class CountMimesByTopLevelDomains extends CountTopLevelDomains {

    private static Pattern INT_PATTERN = Pattern.compile("^\\d+$");
    private Map<String, MimeCounts> map = new HashMap<>();
    private Writer writer;
    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        Path targFile = Paths.get(args[0]).resolve("domain_counts_"+getThreadNumber()+".txt");
        Files.createDirectories(targFile.getParent());
        writer = Files.newBufferedWriter(targFile,
                StandardCharsets.UTF_8);

    }


    @Override
    public void usage() {
        System.out.println("CountTopLevelDomains <output_directory>");
    }

    @Override
    public void process(String row) throws IOException {
        List<CCIndexRecord> records = parseRecords(row);
        for (CCIndexRecord r : records) {
            if (! r.getStatus().equals("200")) {
                continue;
            }
            String u = r.getUrl();

            if (u.endsWith("robots.txt")) {
                continue;
            }
            String tld = getTLD(u);
            String mime = CCIndexRecord.normalizeMime(r.getMime());
            mime = (mime == null) ? "NULL" : mime;
            MimeCounts mimeCounts = map.get(tld);
            if (mimeCounts == null) {
                mimeCounts = new MimeCounts();
            }
            mimeCounts.increment(mime);
            map.put(tld, mimeCounts);
        }
    }

    @Override
    public void close() throws IOException {
        map = MapUtil.sortByValueDesc(map);
        for (Map.Entry<String, MimeCounts> e : map.entrySet()) {

            MimeCounts mimeCounts = e.getValue();
            mimeCounts.m = MapUtil.sortByValueDesc(mimeCounts.m);

            for (Map.Entry<String, Integer> mc : mimeCounts.m.entrySet()) {
                writer.write(clean(e.getKey()) + "\t" + clean(mc.getKey()) + "\t"+mc.getValue()+"\n");
            }
        }
        writer.flush();
        writer.close();
    }


}
