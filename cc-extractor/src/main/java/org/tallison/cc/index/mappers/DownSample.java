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
import java.util.List;
import java.util.Map;
import java.util.Random;
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

public class DownSample extends AbstractRecordProcessor {
    private static final String ANY_TLD = "ANY_TLD";
    private static final String MIME_COL_HEADER = "mime";
    private static Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES)
            .create();

    private final Random random = new Random();
    private Writer writer;

    private int i;
    private final Map<String, Map<Matcher, Float>> tldMimes = new HashMap<>();
    int multiline = 0;

    public DownSample() {
    }

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        tldMimes.clear();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]))) {
            String line = reader.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                if (cols.length == 2) {
                    String mime = cols[0].trim();
                    if (mime.equalsIgnoreCase(MIME_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[1]);
                        Map<Matcher, Float> mimes = tldMimes.get(ANY_TLD);
                        if (mimes == null) {
                            mimes = new HashMap<>();
                            tldMimes.put(ANY_TLD, mimes);
                        }
                        if (mime.startsWith("/") && mime.endsWith("/")) {
                            mime = mime.substring(1, mime.length() - 1);
                        } else {
                            mime = "(?i)\\A" + mime + "\\Z";
                        }
                        Matcher mimeMatcher = Pattern.compile(mime).matcher("");
                        mimes.put(mimeMatcher, f);
                    } catch (NumberFormatException e) {
                        System.err.println("couldn't parse " + cols[1] + " for: " + mime);
                    }
                } else if (cols.length == 3) {
                    String tld = cols[0].trim();
                    String mime = cols[1].trim();
                    if (mime.equalsIgnoreCase(MIME_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[2]);
                        Map<Matcher, Float> mimes = tldMimes.get(tld);
                        if (mimes == null) {
                            mimes = new HashMap<>();
                            tldMimes.put(tld, mimes);
                        }
                        if (mime.startsWith("/") && mime.endsWith("/")) {
                            mime = mime.substring(1, mime.length() - 1);
                        } else {
                            mime = "(?i)\\A" + mime + "\\Z";
                        }
                        Matcher mimeMatcher = Pattern.compile(mime).matcher("");
                        mimes.put(mimeMatcher, f);
                    } catch (NumberFormatException e) {
                        System.err.println("couldn't parse " + cols[1] + " for: " + mime);
                    }
                } else {
                    System.err.println("row too short: " + line);
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
        System.out.println("DownSample <sample_rates_file> <output_directory>");
        System.out.println("Sample rates file should be a UTF-8 tab delimited file (with no escaped tabs)");
        System.out.println("and it should have at least 2 columns: mime\tfloat");
        System.out.println("alternatively, it can have 3 columns: topleveldomain\tmime\tfloat");
    }

    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = CCIndexRecord.parseRecords(row);

        for (CCIndexRecord r : records) {
            if (!r.getStatus().equals("200")) {
                continue;
            } else if (r.getUrl().endsWith("robots.txt")) {
                continue;
            }
            String mime = CCIndexRecord.normalizeMime(r.getMime());
            String detectedMime = CCIndexRecord.normalizeMime(r.getMimeDetected());
            String tld = CCIndexRecord.getTLD(r.getUrl());

            boolean select = shouldSelect(tld, mime, detectedMime);

            if (select == true) {
                gson.toJson(r, writer);
                writer.write("\n");
            } else {
                //System.out.println("IGNORE: "+m);
            }
        }
    }

    private boolean shouldSelect(String tld, String mime, String detectedMime) {
        //try to find the sampling % for the actual tld
        if (!StringUtils.isBlank(tld) && tldMimes.containsKey(tld)) {
            Map<Matcher, Float> mimes = tldMimes.get(tld);
            for (Map.Entry<Matcher, Float> e : mimes.entrySet()) {
                Matcher mimeMatcher = e.getKey();
                if (mimeMatcher.reset(mime).find() || mimeMatcher.reset(detectedMime).find()) {
                    float rf = random.nextFloat();
                    if (e.getValue() >= 1.0 || rf <= e.getValue()) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
        //if there was no tld, or no specific mime pattern
        //had a hit, apply the any_tld sampling rules
        Map<Matcher, Float> mimes = tldMimes.get(ANY_TLD);
        if (mimes != null) {
            for (Map.Entry<Matcher, Float> e : mimes.entrySet()) {
                Matcher mimeMatcher = e.getKey();
                if (mimeMatcher.reset(mime).find() || mimeMatcher.reset(detectedMime).find()) {
                    float rf = random.nextFloat();
                    if (e.getValue() >= 1.0 || rf <= e.getValue()) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }

        return false;
    }


    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
