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


import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * wrapper around wget to run it multi-threaded/process and output
 * the file by mime name
 */
public class WReGetter {
    private static final long MAX_FILE_LENGTH = 50_000_000;
    private static final int MAX_MILLIS = 120_000;
    private final Base32 base32 = new Base32();
    static AtomicInteger WGET_COUNTER = new AtomicInteger(0);


    private Path rootDir;
    public static void main(String[] args) throws Exception {
        WReGetter getter = new WReGetter();
        getter.execute(args);
    }

    private static void usage() {
        System.out.println("java -cp *.jar org.mitre.commoncrawl.WReGetter <numThreads> <digest_url_file> <outputdir> <table_outputdir>");
        System.out.println("The <url_digest_file> is a tab-delimited UTF-8 file with no escaped tabs");
        System.out.println("It has two columns: url\\tdigest");
    }

    private void execute(String[] args) throws IOException {
        if (args.length != 4) {
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
        new Thread(filler).start();
        rootDir = Paths.get(args[2]);
        Path tableDir = Paths.get(args[3]);
        if (! Files.isDirectory(tableDir)) {
            Files.createDirectories(tableDir);
        }
        System.out.println("creating thread pool");
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<Integer>(executorService);
        System.out.println("about to start");

        for (int i = 0; i < numThreads; i++) {
            System.out.println("submitted "+i);
            executorCompletionService.submit(new WGetter(queue, tableDir));
        }

        int completed = 0;
        while (completed < numThreads) {
            try {
                Future<Integer> future = executorCompletionService.poll(1, TimeUnit.SECONDS);
                if (future != null) {
                    completed++;
                    try {
                        future.get();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
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
                //has header
                line = reader.readLine();
                while (line != null) {

                    String[] cols = line.split("\t");
                    String digest = null;
                    String url = null;
                    if (cols.length == 1) {
                        url = cols[0];
                    } else {
                        url = cols[0];
                        digest = cols[1];
                    }
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
        int id = WGET_COUNTER.getAndIncrement();
        final ArrayBlockingQueue<DigestURLPair> queue;
        final BufferedWriter writer;

        WGetter(ArrayBlockingQueue<DigestURLPair> q, Path reportingDir) throws IOException {
            this.queue = q;
            System.out.println("WGETTER STARTED");
            writer = Files.newBufferedWriter(reportingDir.resolve("status_table_" + id + ".txt"), StandardCharsets.UTF_8);
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                try {
                    DigestURLPair p = queue.poll(1, TimeUnit.SECONDS);
                    System.out.println("WGOT: " + id + " : " + p.url);
                    if (p instanceof DigestURLPairPoison) {
                        break;
                    }
                    wget(p);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            writer.flush();
            writer.close();
            return 1;
        }

        private void wget(DigestURLPair p) throws IOException {
            ProcessBuilder pb = new ProcessBuilder();
            pb.inheritIO();
            String digest = p.digest;
            Path targetPath = Files.createTempFile("wgetter-", ".tmp");
            try {
                System.out.println(id + " going to get " + p.url);

                String[] args = new String[]{
                        "wget",
                        "-t", "1", //just try once
                        "-O",
                        targetPath.toString(),
                        p.url
                };
                pb.command(args);
                Process process = pb.start();
                int exit = -1;
                long started = System.currentTimeMillis();
                long elapsed = System.currentTimeMillis() - started;
                boolean timedOut = false;
                while (elapsed < MAX_MILLIS) {
                    try {
                        exit = process.exitValue();
                        break;
                    } catch (IllegalThreadStateException e) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e2) {
                            break;
                        }
                    }
                    elapsed = System.currentTimeMillis() - started;
                }
                if (exit == -1 || elapsed >= MAX_MILLIS) {
                    process.destroyForcibly();
                }
                System.out.println("FINISHED: " + elapsed + " : " + exit);
                String status = (elapsed <= MAX_MILLIS && exit == 0) ? "SUCCESS" : "FAILED";
                String newDigest = "";
                long length = -1;
                if ("SUCCESS".equals(status)) {
                    length = Files.size(targetPath);
                    if (length > MAX_FILE_LENGTH) {
                        status = "TOO_LONG";
                    } else {
                        try (InputStream is = Files.newInputStream(targetPath)) {
                            newDigest = base32.encodeToString(DigestUtils.sha1(is));
                        }
                        System.out.println(id + " " + digest + " -> " + newDigest);
                        Path repoTargetFile = rootDir.resolve(newDigest.substring(0, 2) + "/" + newDigest);
                        if (!Files.exists(repoTargetFile)) {
                            Files.createDirectories(repoTargetFile.getParent());
                            Files.copy(targetPath, repoTargetFile);
                        }
                    }
                }
                writer.write(clean(p.url) + "\t" +
                        clean(p.digest) + "\t" +
                        clean(newDigest) + "\t" +
                        status + "\t" +
                        Long.toString(length) + "\n");
                writer.flush();
            } finally {
                Files.delete(targetPath);
            }
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

    private static String clean (String cell) {
        return CCGetter.clean(cell);
    }
}
