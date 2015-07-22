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

package org.tallison.commoncrawl;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.tika.mime.MediaType;
import utils.MapValueSorter;

public class ExtractorStats {
    private Set<String> warcs = new HashSet<>();
    private Map<String, Integer> httpHeaderMimes = new HashMap<>();
    private Map<String, Integer> detectedMimes = new HashMap<>();
    private int visitedRecords = 0;
    private int extractedRecord = 0;
    private int truncatedRecord = 0;
    private long start = -1;
    private long finish = -1;


    public void addWarc(String warc) {
        warcs.add(warc);
    }
    public void visitedRecord() {
        visitedRecords++;
    }

    public void extractedRecord() {
        extractedRecord++;
    }

    public void truncatedRecord() {
        truncatedRecord++;
    }

    public void visitedHeaderMime(MediaType mediaType) {
        increment(mediaType, httpHeaderMimes);
    }

    public void visitedDetectedMimes(MediaType mediaType) {
        increment(mediaType, detectedMimes);
    }

    private void increment(MediaType mediaType, Map<String, Integer> map) {
        String mimeString = (mediaType == null) ? "null" :
                mediaType.toString().toLowerCase(Locale.ENGLISH);
        Integer cnt = map.get(mimeString);
        if (cnt == null) {
            cnt = 0;
        }
        cnt++;
        map.put(mimeString, cnt);
    }

    public void start() {
        start = new Date().getTime();
    }

    public void finish() {
        finish = new Date().getTime();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        long actualFinish = (finish == -1) ? new Date().getTime() : finish;
        sb.append("Total time: "+(((double)actualFinish-(double)start)/1000.0) + " seconds\n");
        sb.append("There were "+truncatedRecord + " truncated records\n");
        sb.append("I visited: "+visitedRecords + " records\n");
        sb.append("I extracted: " + extractedRecord+" records\n");
        sb.append("Header mime counts:\n");
        for (String m : MapValueSorter.sortByValue(httpHeaderMimes).keySet()) {
            sb.append("\t"+m+": "+httpHeaderMimes.get(m)+"\n");
        }
        sb.append("\n\nDetected mime counts:");
        for (String m : MapValueSorter.sortByValue(detectedMimes).keySet()) {
            sb.append("\t"+m+": "+detectedMimes.get(m)+"\n");
        }
        sb.append("\n");
        return sb.toString();

    }
}
