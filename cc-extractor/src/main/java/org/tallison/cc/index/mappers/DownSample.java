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

public class DownSample extends AbstractRecordProcessor {
    private enum WHICH_MIME {
        HEADER_ONLY,
        DETECTED_ONLY,
        HEADER_OR_DETECTED
    }
    private static final String ANY_TLD = "ANY_TLD";
    private static final String MIME_COL_HEADER = "mime";
    private static Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES)
            .create();

    private final Random random = new Random();
    private Writer writer;
    private boolean includesTLD = false;
    private long selected = 0;
    private long total = 0;
    private WHICH_MIME headerOrDetected = WHICH_MIME.HEADER_OR_DETECTED;

    private int i;
    private final Map<String, MimeMatcher> tldMimes = new HashMap<>();
    int multiline = 0;

    public DownSample() {
    }

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        tldMimes.clear();
        if (args.length > 2) {
            if (args[2].contains("detected_only")) {
                headerOrDetected = WHICH_MIME.DETECTED_ONLY;
            } else if (args[2].contains("header_only")) {
                headerOrDetected = WHICH_MIME.HEADER_ONLY;
            } else {
                throw new IllegalArgumentException("Expected 'detected_only' or 'header_only'."+
                        " I regret I don't understand: "+args[2]);
            }
        }
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]))) {
            String line = reader.readLine();
            int lastNumCols = -1;
            while (line != null) {
                String[] cols = line.split("\t");
                if (cols.length == 2) {
                    if (lastNumCols > -1 && lastNumCols != 2) {
                        throw new IllegalArgumentException("Last row had" + lastNumCols +
                                "columns, but this row has 2.  Every row must have the same number of columns");
                    }
                    lastNumCols = 2;
                    String mime = cols[0].trim();
                    if (mime.equalsIgnoreCase(MIME_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[1]);
                        MimeMatcher mimeMatcher = tldMimes.get(ANY_TLD);
                        if (mimeMatcher == null) {
                            mimeMatcher = new MimeMatcher();
                            tldMimes.put(ANY_TLD, mimeMatcher);
                        }
                        mimeMatcher.addMime(mime, f);
                    } catch (NumberFormatException e) {
                        System.err.println("couldn't parse " + cols[1] + " for: " + mime);
                    }
                } else if (cols.length == 3) {
                    if (lastNumCols > -1 && lastNumCols != 2) {
                        throw new IllegalArgumentException("Last row had" + lastNumCols +
                                "columns, but this row has 3.  Every row must have the same number of columns");
                    }
                    lastNumCols = 3;
                    includesTLD = true;
                    String tld = cols[0].trim();
                    String mime = cols[1].trim();
                    if (mime.equalsIgnoreCase(MIME_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[2]);
                        MimeMatcher mimeMatcher = tldMimes.get(tld);
                        if (mimeMatcher == null) {
                            mimeMatcher = new MimeMatcher();
                            tldMimes.put(tld, mimeMatcher);
                        }
                        mimeMatcher.addMime(mime, f);
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
        System.out.println("DownSample <sample_rates_file> <output_directory> <optional>detected_only|header_only</optional>");
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
            String headerMime = CCIndexRecord.normalizeMime(r.getMime());
            String detectedMime = CCIndexRecord.normalizeMime(r.getMimeDetected());
            String tld = CCIndexRecord.getTLD(r.getUrl());

            boolean select = shouldSelect(tld, headerMime, detectedMime);

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

    private boolean shouldSelect(String tld, String headerMime, String detectedMime) {
        //try to find the sampling % for the actual tld
        if (!StringUtils.isBlank(tld) && tldMimes.containsKey(tld)) {
            MimeMatcher mimeMatcher = tldMimes.get(tld);
            if (mimeMatcher != null) {
                return mimeMatcher.matches(headerMime, detectedMime);
            }
        }

        //if there was no tld, or no specific mime pattern
        //had a hit, apply the any_tld sampling rules
        MimeMatcher mimeMatcher = tldMimes.get(ANY_TLD);
        if (mimeMatcher != null) {
            return mimeMatcher.matches(headerMime, detectedMime);
        }
        return false;
    }


    @Override
    public void close() throws IOException {
        System.out.println(selected + " out of "+total);
        writer.flush();
        writer.close();
    }

    private class MimeMatcher {
        private final Random random = new Random();
        Map<String, Float> exactMatches = new HashMap<>();
        Map<Matcher, Float> regexMatches = new HashMap<>();
        Set<String> shouldIgnore = new HashSet<>();

        void addMime(String mime, float samplingRate) {
            if (mime.startsWith("/") && mime.endsWith("/")) {
                mime = mime.substring(1, mime.length() - 1);
                Matcher mimeMatcher = Pattern.compile(mime).matcher("");
                regexMatches.put(mimeMatcher, samplingRate);

            } else {
                exactMatches.put(mime, samplingRate);
            }
        }

        boolean matches(String headerMime, String detectedMime) {
            switch (headerOrDetected) {
                case HEADER_ONLY:
                    return matchesSingle(headerMime);
                case DETECTED_ONLY:
                    return matchesSingle(detectedMime);
                case HEADER_OR_DETECTED:
                    return matchesSingle(headerMime) || matchesSingle(detectedMime);
            }
            return false;
        }

        private boolean headerOrDetected(String headerMime, String detectedMime) {
            return matchesSingle(headerMime) || matchesSingle(detectedMime);
        }

        private boolean matchesSingle(String mime) {
            if (shouldIgnore.contains(mime)) {
                return false;
            }
            boolean found = false;
            if (exactMatches.containsKey(mime)) {
                float p = exactMatches.get(mime);
                if (p >= 1.0f || random.nextFloat() < p) {
                    return true;
                }
                return false;
            }

            for (Map.Entry<Matcher, Float> e : regexMatches.entrySet()) {
                if (e.getKey().reset(mime).find()) {
                    found = true;
                    float p = exactMatches.get(mime);
                    if (p >= 1.0f || random.nextFloat() < p) {
                        return true;
                    }
                    break;
                }
            }
            if (!found) {
                shouldIgnore.add(mime);
            }
            return false;
        }
    }
}
