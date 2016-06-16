/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.schema;

import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * If a field matches this pattern, return ""
 */
public class FilterFieldMapper extends IndivFieldMapper {
    private final Pattern capture;

    public FilterFieldMapper(String toField, Pattern capture) {
        super(toField);
        this.capture = capture;
    }

    @Override
    public String[] map(String[] vals) {
        List<String> ret = new LinkedList<>();
        for (int i = 0; i < vals.length; i++) {
            String v = map(vals[i]);
            if (!StringUtils.isBlank(v)) {
                ret.add(v);
            }
        }
        return ret.toArray(new String[ret.size()]);
    }


    private String map(String val) {
        Matcher m = capture.matcher(val);
        if (! m.find()) {
            return val;
        }
        return "";
    }

    @Override
    public String toString() {
        return "CaptureFieldMapper{" +
                "capture=" + capture +
                "} " + super.toString();
    }
}
