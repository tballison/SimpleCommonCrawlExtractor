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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class CCIndexReaderWrapper implements Callable<Integer> {
    public static final Path POISON = Paths.get("");

    private final ArrayBlockingQueue<Path> queue;
    private final IndexRecordProcessor processor;
    private final CCIndexReader reader = new CCIndexReader();

    public CCIndexReaderWrapper(ArrayBlockingQueue<Path> queue, IndexRecordProcessor processor) {
        this.queue = queue;
        this.processor = processor;
    }

    @Override
    public Integer call() throws Exception {
        while (true) {
            Path p = queue.take();//hang
            if (p.equals(POISON)) {
                break;
            }
            reader.process(p, processor);
        }
        processor.close();
        return 1;
    }
}
