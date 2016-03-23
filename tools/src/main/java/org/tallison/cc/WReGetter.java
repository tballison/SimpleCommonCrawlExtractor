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
package org.tallison.cc;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * wrapper around wget to run it multi-threaded/process and output
 * the file by mime name
 */
public class WReGetter {
    private Path rootDir;
    public static void main(String[] args) throws Exception {
        WReGetter getter = new WReGetter();
        getter.execute(args);
    }

    private static void usage() {
        System.out.println("java -jar *.jar org.mitre.commoncrawl.WReGetter <numThreads> <digest_url_file> <outputdir>");
        System.out.println("The <digest_url_file> is a tab-delimited UTF-8 file with no escaped tabs");
        System.out.println("It has two columns: digest\\turl");
    }

    private void execute(String[] args) throws IOException {
        if (args.length != 3) {
            usage();
            System.exit(1);
        }
        if (args[0].contains("-h")) {
            usage();
            System.exit(0);
        }
        int numThreads = Integer.parseInt(args[0]);
        BufferedReader r = Files.newBufferedReader(Paths.get(args[1]));
        ArrayBlockingQueue<DigestURLPair> queue = new ArrayBlockingQueue<DigestURLPair>(1000);
        QueueFiller filler = new QueueFiller(r, queue, numThreads);
        filler.run();
        rootDir = Paths.get(args[2]);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<Integer>(executorService);

        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new WGetter(queue));
        }

        int completed = 0;
        while (completed < numThreads) {
            try {
                Future<Integer> future = executorCompletionService.poll(1, TimeUnit.SECONDS);
                if (future != null) {
                    completed++;
                }
            } catch (InterruptedException e) {

            }
        }
        executorService.shutdown();
        executorService.shutdownNow();
        System.exit(0);


    }

    private class QueueFiller implements Runnable {
        private final BufferedReader reader;
        private final ArrayBlockingQueue queue;
        private final int numThreads;

        private QueueFiller(BufferedReader reader,
                            ArrayBlockingQueue<DigestURLPair> q, int numThreads) {
            this.reader = reader;
            this.queue = q;
            this.numThreads = numThreads;
        }

        @Override
        public void run() {

            try{
                String line = reader.readLine();
                while (line != null) {
                    String[] cols = line.split("\t");
                    String digest = cols[0];
                    String url = cols[1];
                    DigestURLPair p = new DigestURLPair(digest, url);
                    //hang forever
                    queue.put(p);
                    /*boolean added = false;
                    while (added == false) {
                        added = queue.offer(new DigestURLPair(digest, url), 1, TimeUnit.SECONDS);
                    }*/
                    line = reader.readLine();
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            System.out.println("queue filler has finished");
            for (int i = 0; i < numThreads; i++) {
                try {
                    queue.put(new DigestURLPairPoison());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("queue filler has finished adding poison");
            return;

        }
    }
    private class WGetter implements Callable<Integer> {
        final ArrayBlockingQueue<DigestURLPair> queue;
        WGetter(ArrayBlockingQueue<DigestURLPair> q) {
            this.queue = q;
        }


        @Override
        public Integer call() throws Exception {
            while (true) {
                try {
                    DigestURLPair p = queue.poll(1, TimeUnit.SECONDS);
                    if (p instanceof DigestURLPairPoison) {
                        return 1;
                    }
                    wget(p);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void wget(DigestURLPair p) throws IOException {
            ProcessBuilder pb = new ProcessBuilder();
            pb.inheritIO();
            Path targetPath = rootDir.resolve(p.digest.substring(0,2)+"/"+p.digest);
            if (Files.isRegularFile(targetPath)) {
                return;
            }
            Files.createDirectories(targetPath.getParent());

            String[] args = new String[]{
                    "wget",
                    "-O",
                    targetPath.toString(),
                    p.url
            };
            pb.command(args);
            Process process = pb.start();
            int exit = -1;
            System.out.println("about to start: "+p.digest + " : "+p.url);
            while (true) {
                try {
                    exit = process.exitValue();
                    break;
                } catch (IllegalThreadStateException e) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e2) {

                    }
                }
            }
            System.out.println("finished: "+p.digest + " : "+p.url);
        }
    }

    private class DigestURLPair {
        final String digest;
        final String url;

        DigestURLPair(String digest, String url) {
            this.digest = digest;
            this.url = url;
        }
    }

    private class DigestURLPairPoison extends DigestURLPair {
        DigestURLPairPoison() {
            super(null, null);
        }
    }

}
