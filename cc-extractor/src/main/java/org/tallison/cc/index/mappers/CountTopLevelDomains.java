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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tallison.cc.index.CCIndexRecord;
import org.tallison.utils.MapUtil;

public class CountTopLevelDomains extends AbstractRecordProcessor {

    private static Pattern INT_PATTERN = Pattern.compile("^\\d+$");
    private Map<String, Integer> map = new HashMap<>();
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
            String tld = getTLD(r.getUrl());
            Integer c = map.get(tld);
            if (c == null) {
                c = new Integer(1);
            } else {
                c++;
            }
            map.put(tld, c);

        }
    }

    @Override
    public void close() throws IOException {
        map = MapUtil.sortByValueDesc(map);
        for (Map.Entry<String, Integer> e : map.entrySet()) {
            writer.write(e.getKey() + "\t" + e.getValue()+"\n");
        }
        writer.flush();
        writer.close();
    }

    /**
     *
     * @param url
     * @return "" if no tld could be extracted
     */
    protected static String getTLD(String url) {
        if (url == null) {
            return "";
        }
        Matcher intMatcher = INT_PATTERN.matcher("");

        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            int i = host.lastIndexOf(".");
            String tld = "";
            if (i > -1 && i+1 < host.length()) {
                tld = host.substring(i+1);
            } else {
                //bad host...or do we want to handle xyz.com. ?
                return tld;
            }
            if (intMatcher.reset(tld).find()) {
                return "";
            }
            return tld;

        } catch (URISyntaxException e) {
            //swallow
        }
        return "";
    }

}
