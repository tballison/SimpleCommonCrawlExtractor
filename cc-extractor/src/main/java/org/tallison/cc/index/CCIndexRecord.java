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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class CCIndexRecord {

    private static Pattern INT_PATTERN = Pattern.compile("^\\d+$");
    private static Gson gson = new GsonBuilder().create();


    private String url;
    private String mime;
    private String status;
    private String digest;
    private Integer length;
    private Integer offset;
    private String filename;

    public String getUrl() {
        return url;
    }

    public String getMime() {
        return mime;
    }

    public String getStatus() {
        return status;
    }

    public String getDigest() {
        return digest;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getOffset() {
        return offset;
    }

    public String getFilename() {
        return filename;
    }

    public static String normalizeMime(String s) {
        if (s == null) {
            return s;
        }
        s = s.toLowerCase(Locale.ENGLISH);
        s = s.replaceAll("^\"|\"$", "");
        s = s.replaceAll("\\s+", " ");
        return s;
    }


    public String getOffsetHeader() {
        return "bytes=" + offset + "-" + (offset+length-1);
    }

    /**
     *
     * @param url
     * @return "" if no tld could be extracted
     */
    public static String getTLD(String url) {
        if (url == null) {
            return "";
        }
        Matcher intMatcher = INT_PATTERN.matcher("");

        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            int i = host.lastIndexOf(".");
            String tld = "";
            if (i > -1 && i+1 < host.length()) {
                tld = host.substring(i+1);
            } else {
                //bad host...or do we want to handle xyz.com. ?
                return tld;
            }
            if (intMatcher.reset(tld).find()) {
                return "";
            }
            return tld;

        } catch (URISyntaxException e) {
            //swallow
        }
        return "";
    }

    public static List<CCIndexRecord> parseRecords(String row) {
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

    private static CCIndexRecord parseRecord(String row, AtomicInteger i) {
/*        int urlI = row.indexOf(' ',i.get());
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
        */
        String json = row;
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