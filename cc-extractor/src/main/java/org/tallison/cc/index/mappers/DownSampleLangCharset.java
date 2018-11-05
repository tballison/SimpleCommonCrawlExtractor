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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.tallison.cc.index.CCIndexRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class loads a tab-delimited file of mime\t<code>float</code>.
 * It selects a record if a random float is at/below that threshold.
 * <p>
 * This allows the user to set different sampling rates per mime
 * type to generate a new sub-index for downloading, e.g. I only
 * want .001% of "text/html" but I want 50% of "application/pdf"
 * <p>
 * If a mime type does not exist in the sampling weights file, the index
 * record is selected (or threshold value = 1.0f).
 * <p>
 * Alternatively, the tab delimited file can contain topleveldomain\tmime\t<code>float</code>
 */

public class DownSampleLangCharset extends AbstractRecordProcessor {

    private static final String CHARSET_COL_HEADER = "charset";
    private static Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES)
            .create();

    private final Random random = new Random();
    private Writer writer;
    private long selected = 0;
    private long total = 0;

    private int i;
    private final Map<String, Float> sampleRates = new HashMap<>();

    public DownSampleLangCharset() {
    }

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        sampleRates.clear();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]))) {
            String line = reader.readLine();
            int lastNumCols = -1;
            while (line != null) {
                String[] cols = line.split("\t");
                if (cols.length == 3) {
                    if (lastNumCols > -1 && lastNumCols != 3) {
                        throw new IllegalArgumentException("Last row had" + lastNumCols +
                                "columns, but this row has 2.  Every row must have the same number of columns");
                    }
                    lastNumCols = 3;
                    String lang = cols[0].trim();
                    String charset = cols[1].trim();
                    String key = lang+"\t"+charset;
                    if (charset.equalsIgnoreCase(CHARSET_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[2]);
                        sampleRates.put(key, f);
                    } catch (NumberFormatException e) {
                        System.err.println("couldn't parse " + cols[2] + " for: " + key);
                    }
                } else {
                    System.err.println("Expected 3 columns!!!!: "+line);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Path targFile = Paths.get(args[1]).resolve("downsampled_rows_" + getThreadNumber() + ".txt");
            Files.createDirectories(targFile.getParent());
            writer = Files.newBufferedWriter(targFile,
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void usage() {
        System.out.println("DownSample <sample_rates_file> <output_directory> <optional>detected_only|header_only</optional>");
        System.out.println("Sample rates file should be a UTF-8 tab delimited file (with no escaped tabs)");
        System.out.println("and it should have at least 2 columns: mime\tfloat");
        System.out.println("alternatively, it can have 3 columns: topleveldomain\tmime\tfloat");
    }

    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = CCIndexRecord.parseRecords(row);

        for (CCIndexRecord r : records) {
            if (! r.getStatus().equals("200")) {
                continue;
            }
            String u = r.getUrl();

            if (u.endsWith("robots.txt")) {
                continue;
            }
            String mime = CCIndexRecord.normalizeMime(r.getMimeDetected());
            mime = (mime == null) ? "NULL" : mime;
            if (!mime.contains("html") && !mime.contains("text")) {
                continue;
            }
            String charset = r.getCharset();
            charset = (StringUtils.isEmpty(charset)) ? "UNK" : charset;
            String lang = getFirstLang(r.getLanguages());
            String key = lang+"\t"+charset;
            Float sampleRate = sampleRates.get(key);
            boolean select = false;
            if (sampleRate != null) {
                if (sampleRate > 0.99999 || random.nextFloat() <= sampleRate) {
                    select = true;
                }
            }

            if (select == true) {
                selected++;
                gson.toJson(r, writer);
                writer.write("\n");
            } else {
                //System.out.println("IGNORE: "+m);
            }
            total++;
        }
    }

    private String getFirstLang(String languages) {
        if (StringUtils.isBlank(languages)) {
            return "NULL";
        }
        String[] langs = languages.split(",");
        if (langs.length > 0) {
            return langs[0];
        }
        return "NULL";
    }

    @Override
    public void close() throws IOException {
        System.out.println(selected + " out of "+total);
        writer.flush();
        writer.close();
    }
}
