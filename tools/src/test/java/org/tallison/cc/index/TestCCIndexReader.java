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

import org.junit.Ignore;
import org.junit.Test;
import org.tallison.cc.index.reducers.ConcatReducer;
import org.tallison.cc.index.reducers.SingleKeyReducer;

@Ignore("for local dev only...not actual unit tests yet")
public class TestCCIndexReader {

    @Test
    public void testBasic() throws Exception {

        String[] args = new String[] {
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.URLsFromDigestProcessor",
                "C:\\data\\warcwork\\poix_repull_digests.txt",
                "C:\\data\\warcwork\\urls\\urls-1.txt"
        };

        CCIndexReader.main(args);
    }

    @Test
    public void testDownSampling() throws Exception {

        String[] args = new String[] {
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.DownSamplingRecordProcessor",
                "C:\\data\\warcwork\\mime_selections.txt",
                "C:\\data\\warcwork\\output"
        };

        CCIndexReader.main(args);
    }

    @Test
    public void testMimeCounter() throws Exception {

        String[] args = new String[] {
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.MimeCounter",
                "C:\\data\\warcwork\\output"
        };

        CCIndexReader.main(args);
    }

    @Test
    public void testBatchMimeCounter() throws Exception {
        CCIndexBatchReader r = new CCIndexBatchReader();

        String[] args = new String[] {
                "10",
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.MimeCounter",
                "C:\\data\\warcwork\\output_batch"
        };
        r.execute(args);
    }

    @Test
    public void testBatchExtensionCounter() throws Exception {
        CCIndexBatchReader r = new CCIndexBatchReader();

        String[] args = new String[] {
                "10",
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.FileExtensionCounter",
                "C:\\data\\warcwork\\output_extensions"
        };
        r.execute(args);
    }

    @Test
    public void testMimeCountReducer() throws Exception {
        String[] args = new String[]{
                "C:\\data\\warcwork\\output_batch",
                "C:\\data\\warcwork\\mimes_mapped.txt"
        };
        SingleKeyReducer.main(args);

    }

    @Test
    public void testBatchIndexSampler() throws Exception {
        CCIndexBatchReader r = new CCIndexBatchReader();

        String[] args = new String[] {
                "10",
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.DownSamplingRecordProcessor",
                "C:\\data\\warcwork\\mime_selections.txt",
                "C:\\data\\warcwork\\output_batch_down_sampling"
        };
        r.execute(args);
    }

    @Test
    public void testIndexSamplerReducer() throws Exception {
        String[] args = new String[]{
                "C:\\data\\warcwork\\output_batch_down_sampling",
                "C:\\data\\warcwork\\down_sampled.txt"
        };
        ConcatReducer.main(args);
    }

    @Test
    public void testBatchURLSFromDigest() throws Exception {
        CCIndexBatchReader r = new CCIndexBatchReader();

        String[] args = new String[] {
                "10",
                "C:\\data\\warcwork\\index_gzs",
                "org.tallison.cc.index.mappers.URLsFromDigestProcessor",
                "C:\\data\\warcwork\\poix_repull_digests.txt",
                "C:\\data\\warcwork\\urls"

        };
        r.execute(args);
    }

}
