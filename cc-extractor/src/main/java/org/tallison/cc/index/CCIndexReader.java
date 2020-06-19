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

package org.tallison.cc.index;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

public class CCIndexReader {
    private int count = 0;

    public void process(Path p, IndexRecordProcessor processor) {

        System.err.println("processing "+p.toString() + " :"+count);
        try (InputStream is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(p)))) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                int lines = 0;
                while (line != null) {
                    try {
                        processor.process(line);
                        if (++count % 100000 == 0) {
                            System.err.println(p.getFileName().toString() + ": "+count);
                        }
                    } catch (IOException e) {
                        //bad row
                        e.printStackTrace();
                    }
                    line = reader.readLine();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("finished processing "+p.toString() + " :"+count);

    }

    public static void main(String[] args) throws Exception {

        Path indexDir = Paths.get(args[0]);
        String pClass = args[1];

        String[] newArgs = Arrays.copyOfRange(args, 2, args.length);
        IndexRecordProcessor p = (IndexRecordProcessor)Class.forName(pClass).newInstance();
        p.init(newArgs);

        CCIndexReader ccIndexReader = new CCIndexReader();
        File[] gzs = indexDir.toFile().listFiles();
        Arrays.sort(gzs);
        for (File f : gzs) {
            ccIndexReader.process(f.toPath(), p);
        }
        p.close();
    }

}
