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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This is the main driver for the mappers.
 *
 * Usage: java -cp xxx.jar CCIndexBatchReader &lt;numThreads&gt; &lt;cc_index_directory&gt; &lt;mapper_class&gt;
 * &lt;mapper_class_args ...&gt;
 */
public class CCIndexBatchReader {

    private final static String[] REDUCERS = {
            "CountExt",
            "CountExtByMime",
            "CountMimeByExt",
            "CountMimes",
            "DownSample",
            "FindURLsFromDigests",
            "CountTopLevelDomains"
    };

    private final static String PACKAGE_NAME = "org.tallison.cc.index.mappers";

    public void execute(String[] args) throws Exception {

        if (args[0].equals("-h") || args[0].equals("--help")) {
            usage();
            System.exit(1);
        }


        int numThreads = Integer.parseInt(args[0]);
        Path indexDir = Paths.get(args[1]);
        String pClass = args[2];
        //load index files into memory...there should only be 300 for now
        File[] gzs = indexDir.toFile().listFiles();
        numThreads = (gzs.length < numThreads) ? gzs.length : numThreads;

        ArrayBlockingQueue<Path> paths = new ArrayBlockingQueue<>(gzs.length+numThreads);
        Arrays.sort(gzs);
        for (File f : gzs) {
            paths.add(f.toPath());
        }
        for (int i = 0; i < numThreads; i++) {
            paths.add(CCIndexReaderWrapper.POISON);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executorService);

        String[] newArgs = Arrays.copyOfRange(args, 3, args.length);
        List<IndexRecordProcessor> procs = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            IndexRecordProcessor p = (IndexRecordProcessor) Class.forName(PACKAGE_NAME+"."+pClass).newInstance();
            p.init(newArgs);
            procs.add(p);
            completionService.submit(new CCIndexReaderWrapper(paths, p));
        }
        int completed = 0;
        while (completed < numThreads) {
            Future<Integer> result = completionService.poll(1, TimeUnit.SECONDS);
            if (result != null) {
                completed++;
            } else {
                System.out.println("In completion loop: "+completed);
            }
        }
        executorService.shutdown();
        executorService.shutdownNow();
        System.exit(1);
    }

    private static void usage() {
        System.out.println("java -jar cc-extractor.jar <number of reducers> <directory_of_index.gzs> <reducer_name> arguments for reducers....");
        System.out.println("Available reducers include:");
        for (String s : REDUCERS) {
            System.out.println(s);
        }
    }

    public static void main(String[] args) throws Exception {
        CCIndexBatchReader batchReader = new CCIndexBatchReader();
        batchReader.execute(args);
    }
}
