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

import java.util.Locale;

public class CCIndexRecord {
    private String url;
    private String mime;
    private String status;
    private String digest;
    private String length;
    private String offset;
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

    public String getLength() {
        return length;
    }

    public String getOffset() {
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
}
