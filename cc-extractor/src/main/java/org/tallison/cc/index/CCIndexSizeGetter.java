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

import org.apache.commons.lang3.StringUtils;
import org.tallison.utils.StringStreamGobbler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CCIndexSizeGetter {
    private static final int POISON = -1;
    private static String AWS_URL_BASE = "https://commoncrawl.s3.amazonaws.com/cc-index/collections/";
    private static String AWS_URL_INDICES = "/indexes/cdx-";

    private static final int DEFAULT_NUM_THREADS = 1;
    public static void main(String[] args) {
        String index = args[0];//"CC-MAIN-2020-10";
        int numThreads = DEFAULT_NUM_THREADS;
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        CCIndexSizeGetter wgetter = new CCIndexSizeGetter();
        wgetter.execute(index, numThreads);
    }

    private void execute(String collection, int numThreads) {
        System.out.println("going to get "+collection +" with " + numThreads + " threads");
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(es);
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(300+numThreads);

        for (int i = 0; i < 300; i++) {
            queue.add(i);
        }
        for (int i = 0; i < numThreads; i++) {
            queue.add(POISON);
        }

        for (int i = 0; i < numThreads; i++) {
            completionService.submit(new Getter(collection, queue));
        }

        int completed = 0;
        while (completed < numThreads) {
            Future<Integer> fut = null;
            try {
                fut = completionService.take();
                fut.get();
                completed++;
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
                break;
            }
        }
        es.shutdownNow();

    }

    private class Getter implements Callable<Integer> {

        private final ArrayBlockingQueue<Integer> queue;
        private final String collection;

        Getter(String collection, ArrayBlockingQueue<Integer> queue) {
            this.collection = collection;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                int indexNum = queue.take();
                if (indexNum == POISON) {
                    return 1;
                }
                String num = StringUtils.leftPad(Integer.toString(indexNum), 5, "0");
                String url = AWS_URL_BASE + collection + AWS_URL_INDICES + num + ".gz";
                //System.out.println("about to get "+url);
                String output = collection+"-cdx-"+num+".gz";
                ProcessBuilder pb = new ProcessBuilder("curl",
                        "-sI", url);

                Process process = pb.start();
                StringStreamGobbler errorGobbler = new StringStreamGobbler(process.getErrorStream());
                StringStreamGobbler outGobbler = new StringStreamGobbler(process.getInputStream());
                Thread errorThread = new Thread(errorGobbler);
                Thread outThread = new Thread(outGobbler);
                errorThread.start();
                outThread.start();

                process.waitFor(360, TimeUnit.SECONDS);
                try {
                    if (process.exitValue() != 0) {
                        System.err.println("failed to get: "+url);
                    }
                } catch (IllegalThreadStateException e) {
                    e.printStackTrace();
                    process.destroyForcibly();
                    return -1;
                }
                String outString = outGobbler.toString();
                Matcher m = Pattern.compile("(?s)Content-Length:\\s+(\\d+)").matcher(outString);
                if (m.find()) {
                    System.out.println(output+ ": "+m.group(1));
                }
            }
        }
    }
}
