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
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

/**
 * wrapper around file to run it multi-threaded/process and output
 * the file by mime name
 */
public class BatchFileIder {
    private static final Path POISON_PATH = Paths.get("");
    private Path rootDir;
    private Path outputDir;
    public static void main(String[] args) throws Exception {
        BatchFileIder getter = new BatchFileIder();
        try {
            getter.execute(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void usage() {
        System.out.println("java -jar *.jar org.tallison.cc.BatchFileIder <numThreads> <inputdir> <outputdir>");
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
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(1000);
        rootDir = Paths.get(args[1]);
        QueueFiller filler = new QueueFiller(rootDir, queue, numThreads);
        new Thread(filler).start();
        outputDir = Paths.get(args[2]);
        Files.createDirectories(outputDir);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<Integer>(executorService);

        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new FileRunner(queue,
                    Files.newBufferedWriter(outputDir.resolve("mimes-"+i+".txt"))));
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
        private final Path root;
        private final ArrayBlockingQueue<Path> queue;
        private final int numThreads;

        private QueueFiller(Path root, ArrayBlockingQueue<Path> q, int numThreads) {
            this.root = root;
            this.queue = q;
            this.numThreads = numThreads;
        }

        @Override
        public void run() {

            try {
                addDir(root);
            } catch (InterruptedException e) {
                //swallow
            }
            System.out.println("queue filler has finished");
            for (int i = 0; i < numThreads; i++) {
                try {
                    queue.put(POISON_PATH);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("queue filler has finished adding poison");
            return;

        }

        private void addDir(Path dir) throws InterruptedException {
            for (File f : dir.toFile().listFiles()) {
                if (f.isDirectory()) {
                    addDir(f.toPath());
                } else {
                    queue.put(f.toPath());
                }
            }
        }
    }


    private class FileRunner implements Callable<Integer> {
        final ArrayBlockingQueue<Path> queue;
        final BufferedWriter writer;
        FileRunner(ArrayBlockingQueue<Path> q, BufferedWriter w) {
            this.queue = q;
            this.writer = w;
        }


        @Override
        public Integer call() throws Exception {
            while (true) {
                try {
                    Path p = queue.poll(1, TimeUnit.SECONDS);
                    if (p.equals(POISON_PATH)) {
                        break;
                    }
                    runFile(p);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            writer.flush();
            writer.close();
            return 1;
        }

        private void runFile(Path p) throws IOException {
            System.out.println("running file: "+p.getFileName().toString());
            ProcessBuilder pb = new ProcessBuilder();
            String[] args = new String[]{
                    "file",
                    "-b",
                    "--mime-type",
                    p.toString(),
            };
            pb.command(args);
            Process process = pb.start();
            StreamGobbler gobbler = new StreamGobbler(process.getInputStream());
            Thread gobblerThread = new Thread(gobbler);
            StreamIgnorer ignorer = new StreamIgnorer(process.getErrorStream());
            Thread ignorerThread = new Thread(ignorer);
            gobblerThread.start();
            ignorerThread.start();

            int exit = -1;
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
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //
            }
            gobblerThread.interrupt();
            ignorerThread.interrupt();
            gobbler.close();
            ignorer.close();
            String mime = gobbler.getString().trim();
            writer.write(p.toString()+"\t"+mime+"\n");
        }
    }

    class StreamGobbler implements Runnable {
        final BufferedReader reader;
        StringBuilder sb = new StringBuilder();
        StreamGobbler(InputStream stream) {
            this.reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }

        @Override
        public void run() {
            try {
                String line = reader.readLine();
                while (line != null) {
                    sb.append(line+"\n");
                    line = reader.readLine();
                }
            } catch (IOException e) {
            }
        }

        String getString() {
            return sb.toString();
        }

        void close() {
            try {
                reader.close();
            } catch (IOException e) {

            }
        }
    }

    class StreamIgnorer implements Runnable {
        final BufferedReader reader;
        StreamIgnorer(InputStream stream) {
            this.reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }

        @Override
        public void run() {
            try {
                String line = reader.readLine();
                while (line != null) {
                    line = reader.readLine();
                }
            } catch (IOException e) {
            }
        }


        void close() {
            try {
                reader.close();
            } catch (IOException e) {

            }
        }
    }

}
