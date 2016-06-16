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

public class CaptureFieldMapper extends IndivFieldMapper {


    public enum FAIL_POLICY {
        SKIP_FIELD,
        STORE_AS_IS,
        EXCEPTION
    }
    static Pattern GROUP_PATTERN = Pattern.compile("\\$(\\d+)");


    private final Pattern capture;
    private final String replace;
    private final FAIL_POLICY failPolicy;

    public CaptureFieldMapper(String toField, Pattern capture, String replace,
                              FAIL_POLICY failPolicy) {
        super(toField);
        this.capture = capture;
        this.replace = replace;
        this.failPolicy = failPolicy;
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
        if (m.find()) {
            StringBuilder sb = new StringBuilder();
            Matcher replacementMatcher = GROUP_PATTERN.matcher(replace);
            int last = 0;
            while (replacementMatcher.find()) {
                sb.append(replace.substring(last, replacementMatcher.start()));
                int groupNum = Integer.parseInt(replacementMatcher.group(1));
                sb.append(m.group(groupNum));
                last = replacementMatcher.end();
            }
            sb.append(replace.substring(last));
            return sb.toString();
        } else if (failPolicy.equals(FAIL_POLICY.SKIP_FIELD)) {
            //do nothing, return null
            return null;
        } else if (failPolicy.equals(FAIL_POLICY.STORE_AS_IS)) {
            return val;
        } else if (failPolicy.equals(FAIL_POLICY.EXCEPTION)) {
            throw new IllegalArgumentException("Couldn't find pattern: " +
                    capture.toString() + " in " + val);
        }
        throw new RuntimeException("Ran out of elses in CaptureFieldMapper ?!");
    }

    @Override
    public String toString() {
        return "CaptureFieldMapper{" +
                "capture=" + capture +
                ", replace='" + replace + '\'' +
                ", failPolicy=" + failPolicy +
                "} " + super.toString();
    }
}
