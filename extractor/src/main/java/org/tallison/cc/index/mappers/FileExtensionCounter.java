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
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tallison.cc.index.CCIndexRecord;
import org.tallison.utils.MapUtil;

public class FileExtensionCounter extends AbstractRecordProcessor {

    private Map<String, Integer> extensions = new HashMap<>();
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
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = parseRecords(row);

        for (CCIndexRecord r : records) {
            String u = r.getUrl();
            if (u == null)
                continue;
            String ext = getExtension(u);
            ext = (ext == null) ? "NULL" : ext;
            Integer c = extensions.get(ext);
            if (c == null) {
                c = new Integer(1);
            } else {
                c++;
            }
            extensions.put(ext, c);

        }
    }

    private String getExtension(String u) {
        if (u == null || u.length() == 0) {
            return null;
        }
        int i = u.lastIndexOf('.');
        if (i < 0 || i+6 < u.length()) {
            return null;
        }
        String ext = u.substring(i+1);
        ext = ext.trim();
        Matcher m = Pattern.compile("^\\d+$").matcher(ext);
        if (m.find()) {
            return null;
        }
        ext = ext.toLowerCase(Locale.ENGLISH);
        ext = ext.replaceAll("\\/$", "");
        return ext;
    }

    @Override
    public void close() throws IOException {
        extensions = MapUtil.sortByValueDesc(extensions);
        for (Map.Entry<String, Integer> e : extensions.entrySet()) {
            writer.write(e.getKey() + "\t" + e.getValue()+"\n");
        }
        writer.flush();
        writer.close();
    }

}
