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

import org.tallison.cc.index.CCIndexRecord;

/**
 * If you have a list of cc mimes and you want to look the original urls,
 * use this.
 * <p>
 * This is useful if you have a truncated/corrupt file and you want to repull it.
 */
public class FindURLsFromDigests extends AbstractRecordProcessor {

    private final Map digests = new HashMap<>();
    private Writer writer;

    private int i;
    Map<String, Integer> mimes = new HashMap<>();
    int multiline = 0;


    @Override
    public void usage() {
        System.out.println("FindURLsFromDigests <list_of_digests> <output_directory>");
    }

    @Override
    public void init(String[] args) throws Exception {
        super.init(args);
        if (args.length != 2) {
            throw new IllegalArgumentException("must have 2 arguments: digest file and output file");
        }
        digests.clear();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]))) {
            String line = reader.readLine();
            while (line != null) {
                digests.put(line.trim(), 1);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path targFile = Paths.get(args[1]).resolve("urls_"+getThreadNumber()+".txt");
        Files.createDirectories(targFile.getParent());
        writer = Files.newBufferedWriter(targFile,
                StandardCharsets.UTF_8);

    }

    @Override
    public void process(String row) throws IOException {

        List<CCIndexRecord> records = CCIndexRecord.parseRecords(row);

        for (CCIndexRecord r : records) {
            String digest = r.getDigest();
            if (digests.containsKey(digest)) {
                digest = digest.replaceAll("[\t\r\n]", " ");
                writer.write(clean(digest)+"\t"+
                        clean(r.getUrl())+"\n");
                writer.flush();
            }
        }
    }


    @Override
    public void close() throws IOException {
        System.err.println(getThreadNumber() + " is closing");
        writer.flush();
        writer.close();

    }
}
