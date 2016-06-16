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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.tallison.cc.index.CCIndexRecord;

/**
 * Class loads a tab-delimited file of mime\t<code>float</code>.
 * It selects a record if a random float is at/below that threshold.
 *
 * This allows the user to set different sampling rates per mime
 * type to generate a new sub-index for downloading, e.g. I only
 * want .001% of "text/html" but I want 50% of "application/pdf"
 *
 * If a mime type does not exist in the sampling weights file, the index
 * record is selected (or threshold value = 1.0f).
 */

public class DownSample extends AbstractRecordProcessor {
    private static final String MIME_COL_HEADER = "mime";
    private static Gson gson = new GsonBuilder().create();

    private final Random random = new Random();
    private Writer writer;

    private int i;
    private final Map<String, Float> mimes = new HashMap<>();
    int multiline = 0;

    public DownSample() {}

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        mimes.clear();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]))) {
            String line = reader.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                if (cols.length > 1) {
                    String mime = cols[0].trim();
                    if (mime.equalsIgnoreCase(MIME_COL_HEADER)) {
                        line = reader.readLine();
                        continue;
                    }
                    float f = -1.0f;
                    try {
                        f = Float.parseFloat(cols[1]);
                        mimes.put(mime, f);
                    } catch (NumberFormatException e) {
                        System.err.println("couldn't parse "+cols[1] +" for: " +mime);
                    }
                } else {
                    System.err.println("row too short: "+line);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Path targFile = Paths.get(args[1]).resolve("downsampled_rows_"+getThreadNumber()+".txt");
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
    }

    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = parseRecords(row);

        for (CCIndexRecord r : records) {
            String m = CCIndexRecord.normalizeMime(r.getMime());
            boolean select = false;
            if (mimes.containsKey(m)) {
                float rf = random.nextFloat();
                if (rf <= mimes.get(m)) {
                    select = true;
                }
            } else {
                select = true;
            }
            if (select == true) {
                gson.toJson(r, writer);
                writer.write("\n");
            }
        }
    }


    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
