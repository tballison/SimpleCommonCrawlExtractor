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
package org.tallison.cc.index.reducers;

import java.io.BufferedReader;
import java.io.File;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.tallison.utils.MapUtil;

public class DoubleKeyReducer {

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        Writer w = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8);
        Map<String, Map<String, Integer>> m = new HashMap<>();

        for (File f :dir.toFile().listFiles()) {
            BufferedReader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
            String line = r.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                String k1 = cols[0];
                String k2 = cols[1];
                Integer v = Integer.parseInt(cols[2]);
                Map<String, Integer> m2 = m.get(k1);
                if (m2 == null) {
                    m2 = new HashMap<>();
                }

                Integer currVal = m2.get(k2);
                if (currVal == null) {
                    currVal = v;
                } else {
                    currVal += v;
                }
                m2.put(k2, currVal);
                m.put(k1, m2);
                line = r.readLine();
            }
        }
        Set<String> keys = new TreeSet<>(m.keySet());
        for (String k1 : keys) {
            Map<String, Integer> m2 = m.get(k1);
            m2 = MapUtil.sortByValueDesc(m2);
            for (Map.Entry<String, Integer> e: m2.entrySet()) {
                w.write(k1 + "\t" + e.getKey() + "\t" + e.getValue() + "\n");
            }
        }
        w.flush();
        w.close();
    }
}
