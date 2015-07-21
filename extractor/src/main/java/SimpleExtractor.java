import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.httpclient.Header;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.util.LaxHttpParser;

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
public class SimpleExtractor {

    public static void main(String[] args) throws Exception {
        Path warcPath = Paths.get(args[0]);
        InputStream is = Files.newInputStream(warcPath);
        ArchiveReader reader = WARCReaderFactory.get(warcPath.toString(), is, true);
        Path outDir = Paths.get("output");
        int i = 0;
        reader.setStrict(false);
        for (ArchiveRecord record : reader) {
            record.setStrict(false);

            ArchiveRecordHeader recordHeader = record.getHeader();
            if (! recordHeader.getMimetype().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE)) {
                continue;
            }
            for (String headerKey : recordHeader.getHeaderFieldKeys()) {
                System.out.println("WARD HEADER: "+ headerKey + " : " + recordHeader.getHeaderValue(headerKey));
            }
            System.out.println(record.available());
            String mime = recordHeader.getMimetype();
            String urlExt = FilenameUtils.getExtension(recordHeader.getUrl());
            System.out.println(record.getDigestStr());
            System.out.println(recordHeader.getUrl() + " : " + " : " + mime + " : " + recordHeader.getDigest());
            Path outFile = Paths.get(outDir.toString(), i + ".dump");
            Header[] headers = LaxHttpParser.parseHeaders(record, "ISO-8859-1");
            String contentTypeString = null;
            for (Header httpHeader : headers) {
                if (httpHeader.getName().equalsIgnoreCase("content-type")) {
                    contentTypeString = httpHeader.getValue();
                    break;
                }
            }
            byte[] rawBytes = new byte[record.available()];
            record.read(rawBytes);
            TikaInputStream tis = TikaInputStream.get(rawBytes);
            Detector d = new DefaultDetector();
            MediaType mt = d.detect(tis, new Metadata());
            System.out.println(mt);
            OutputStream os = Files.newOutputStream(outFile);

            IOUtils.copy(tis, os);
            os.flush();
            os.close();
            i++;
        }
    }
}
