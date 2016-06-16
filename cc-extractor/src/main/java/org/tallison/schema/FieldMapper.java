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

import com.google.gson.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class FieldMapper {

    public static String NAME = "field_mapper";
    static String IGNORE_CASE_KEY = "ignore_case";


    Map<String, List<IndivFieldMapper>> mappers = new HashMap<>();
    private boolean ignoreCase = true;

    public static FieldMapper load() {
        try (InputStream is = FieldMapper.class.getResourceAsStream("/url_mappings.json")) {
            JsonParser parser = new JsonParser();
            JsonElement el = parser.parse(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)));
            if (el.isJsonObject()) {
                return load(el.getAsJsonObject().get(NAME));
            }
            throw new RuntimeException("NOT AN OBJ");
        } catch (Exception e) {
            throw new RuntimeException("failed to init FieldMapper", e);
        }
    }

    public static FieldMapper load(JsonElement el) {
        if (el == null) {
            throw new IllegalArgumentException(NAME+" must not be empty");
        }
        JsonObject root = el.getAsJsonObject();

        //build ignore case element
        JsonElement ignoreCaseElement = root.get(IGNORE_CASE_KEY);
        if (ignoreCaseElement == null || ! ignoreCaseElement.isJsonPrimitive()) {
            throw new IllegalArgumentException(
                    "ignore case element must not be null and must be a primitive: "
                    +((ignoreCaseElement == null) ? "" : ignoreCaseElement.toString()));
        }
        String ignoreCaseString = ((JsonPrimitive)ignoreCaseElement).getAsString().toLowerCase();
        FieldMapper mapper = new FieldMapper();
        if ("true".equals(ignoreCaseString)) {
            mapper.setIgnoreCase(true);
        } else if ("false".equals(ignoreCaseString)) {
            mapper.setIgnoreCase(false);
        } else {
            throw new IllegalArgumentException(IGNORE_CASE_KEY + " must have a value of \"true\" or \"false\"");
        }


        JsonArray mappings = root.getAsJsonArray("mappings");
        for (JsonElement mappingElement : mappings) {
            JsonObject mappingObj = mappingElement.getAsJsonObject();
            String from = mappingObj.getAsJsonPrimitive("f").getAsString();
            IndivFieldMapper indivFieldMapper = buildMapper(mappingObj);
            mapper.add(from, indivFieldMapper);
        }
        return mapper;
    }

    private static IndivFieldMapper buildMapper(JsonObject mappingObj) {
        List<IndivFieldMapper> tmp = new LinkedList<>();
        String to = mappingObj.getAsJsonPrimitive("t").getAsString();
        JsonObject mapper = mappingObj.getAsJsonObject("capture");
        if (mapper != null) {
            Pattern pattern = Pattern.compile(mapper.getAsJsonPrimitive("find").getAsString());
            String replace = mapper.getAsJsonPrimitive("replace").getAsString();
            String failPolicyString = mapper.getAsJsonPrimitive("fail_policy").getAsString().toLowerCase(Locale.ENGLISH);

            CaptureFieldMapper.FAIL_POLICY fp = null;
            if (failPolicyString == null) {
                //can this even happen?
                fp = CaptureFieldMapper.FAIL_POLICY.SKIP_FIELD;
            } else if (failPolicyString.equals("skip")) {
                fp = CaptureFieldMapper.FAIL_POLICY.SKIP_FIELD;
            } else if (failPolicyString.equals("store_as_is")) {
                fp = CaptureFieldMapper.FAIL_POLICY.STORE_AS_IS;
            } else if (failPolicyString.equals("exception")) {
                fp = CaptureFieldMapper.FAIL_POLICY.EXCEPTION;
            }
            tmp.add(new CaptureFieldMapper(to, pattern, replace, fp));
        } else {
            mapper = mappingObj.getAsJsonObject("filter");
            if (mapper != null) {
                Pattern pattern = Pattern.compile(mapper.getAsJsonPrimitive("find").getAsString());
                tmp.add(new FilterFieldMapper(to, pattern));
            }
        }

        if (tmp.size() == 0) {
            return new IdentityFieldMapper(to);
        } else if (tmp.size() == 1) {
            return tmp.get(0);
        } else {
            return new ChainedFieldMapper(to, tmp);
        }
    }

    public FieldMapper() {

    }

    public void add(String k, IndivFieldMapper m) {
        List<IndivFieldMapper> ms = mappers.get(k);
        if (ms == null) {
            ms = new LinkedList<>();
        }
        ms.add(m);
        mappers.put(k, ms);
    }

    public List<IndivFieldMapper> get(String field) {
        return mappers.get(field);
    }

    public Set<String> getTikaFields() {
        return mappers.keySet();
    }

    public void setIgnoreCase(boolean v) {
        this.ignoreCase = v;
    }

    public boolean getIgnoreCase() {
        return ignoreCase;
    }
}
