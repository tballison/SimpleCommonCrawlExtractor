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


public class ConcatReducer {

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        Writer w = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8);

        for (File f :dir.toFile().listFiles()) {
            BufferedReader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
            String line = r.readLine();
            while (line != null) {
                w.write(line);
                w.write("\n");
                line = r.readLine();
            }
        }
        w.flush();
        w.close();
    }
}
