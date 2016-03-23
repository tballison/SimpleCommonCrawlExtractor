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

import org.tallison.utils.MapUtil;

/**
 * Created by TALLISON on 3/17/2016.
 */
public class SingleKeyReducer {

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        Writer w = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8);
        Map<String, Integer> m = new HashMap<>();

        for (File f :dir.toFile().listFiles()) {
            BufferedReader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
            String line = r.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                String mime = cols[0];
                Integer v = Integer.parseInt(cols[1]);
                Integer currVal = m.get(mime);
                if (currVal == null) {
                    currVal = v;
                } else {
                    currVal += v;
                }
                m.put(mime, currVal);
                line = r.readLine();
            }
        }
        m = MapUtil.sortByValueDesc(m);
        for (Map.Entry<String, Integer> e : m.entrySet()) {
            w.write(e.getKey()+"\t"+e.getValue()+"\n");
        }
        w.flush();
        w.close();
    }
}
