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
package org.tallison.cc.index.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.IndexRecordProcessor;

/**
 * Created by TALLISON on 3/17/2016.
 */
public class AbstractRecordProcessor implements IndexRecordProcessor {

    private static Gson gson = new GsonBuilder().create();
    protected static AtomicInteger threadCounter = new AtomicInteger(0);

    private final int threadNumber;

    public AbstractRecordProcessor() {
        threadNumber = threadCounter.incrementAndGet();
    }


    @Override
    public void init(String[] args) throws Exception {

    }

    @Override
    public void process(String json) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    protected int getThreadNumber() {
        return threadNumber;
    }

    protected List<CCIndexRecord> parseRecords(String row) {
        AtomicInteger i = new AtomicInteger(0);
        List<CCIndexRecord> records = new ArrayList<>();
        //for now turn off multi row splitting
        //while (i.get() < row.length()) {
            CCIndexRecord record = parseRecord(row, i);
            if (record != null) {
                records.add(record);
            }/* else {
                break;
            }*/
        //}
        return records;

    }

    private CCIndexRecord parseRecord(String row, AtomicInteger i) {
        int urlI = row.indexOf(' ',i.get());
        int dateI = row.indexOf(' ', urlI+1);

        if (dateI == -1) {
            //barf
            return null;
        }
        int end = row.indexOf('}',dateI+1);
        if (end == -1) {
            //barf
            return null;
        }
        i.set(end+1);
//        String json = row.substring(dateI);
        String json = row.substring(dateI, end+1);
        CCIndexRecord r;
        try {
            r = gson.fromJson(json, CCIndexRecord.class);
        } catch (JsonSyntaxException e) {
            System.out.println(">>>"+row+"<<<");
            e.printStackTrace();
            return null;
        }
        return r;
    }

}
