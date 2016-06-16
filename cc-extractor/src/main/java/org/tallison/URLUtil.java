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
package org.tallison;


import org.tallison.schema.FieldMapper;
import org.tallison.schema.IndivFieldMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class URLUtil {
    List<IndivFieldMapper> mappers = null;

    public URLUtil() {
        FieldMapper fieldMapper = FieldMapper.load();
        mappers = fieldMapper.get("input_url");

    }
    public String clean(String url) {
        String[] input = new String[]{url};
        for (IndivFieldMapper m : mappers) {
            input = m.map(input);
        }
        if (input.length == 0) {
            return "";
        }
        try {
            new URI(input[0]);
        } catch (URISyntaxException e) {
            return "";
        }
        return input[0];
    }
}
