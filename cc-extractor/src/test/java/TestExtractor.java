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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.mime.MediaType;
import org.junit.Ignore;
import org.junit.Test;
import org.tallison.cc.warc.Extractor;
import org.tallison.cc.warc.ExtractorConfig;

@Ignore
public class TestExtractor {
    //these unfortunately are pseudo tests used for dev only
    //TODO: add real tests

    @Test
    public void testMimes() {
        Extractor ex = new Extractor(null);
        MediaType mt = MediaType.parse("application/xml; charset=UTF-8");
        //System.out.println(ex.getExtension(mt));
    }

    @Test
    public void testExtract() throws IOException {
        Path warcFile = Paths.get("C:/data/warcwork/warcs");
        Path outDir = Paths.get("C:/data/warcwork/testOut");
        ExtractorConfig config = new ExtractorConfig(warcFile, outDir);
        config.setIncludeTruncated(false);
        Extractor ex = new Extractor(config);
        ex.execute();
    }
}
