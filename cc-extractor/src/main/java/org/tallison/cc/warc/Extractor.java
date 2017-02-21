package org.tallison.cc.warc;
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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.httpclient.Header;
import org.apache.commons.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.LaxHttpParser;


public class Extractor extends AbstractExtractor {
    final ExtractorConfig config;

    public Extractor(ExtractorConfig config) {
        super(config.getWarcPath());
        this.config = config;
    }

    void handleRecord(ArchiveRecord record) throws IOException {
        ArchiveRecordHeader recordHeader = record.getHeader();
        if (!recordHeader.getMimetype().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE)) {
            return;
        }

        if (recordHeader.getHeaderFields().containsKey(WARCConstants.HEADER_KEY_TRUNCATED)) {
            extractorStats.truncatedRecord();
        }
        Header[] headers = LaxHttpParser.parseHeaders(record, "ISO-8859-1");
        for (Header header : headers) {
            System.out.println("H: " + header.getName() + " : "+ header.getValue());
        }
        return;
/*        MediaType httpMediaType = getHttpMediaType(headers);
        if (httpMediaType != null) {
            System.out.println("HTTPMEDIA TYPE: " + httpMediaType.toString());
        }
        if (config.isIncludeTruncated() || ! recordHeader.getHeaderFields()
                .containsKey(WARCConstants.HEADER_KEY_TRUNCATED)) {
            //do nothing
        } else {
            System.out.println("truncated: " + httpMediaType + " : " + recordHeader.getUrl());
            return;
        }
        int payloadLength = record.available();
        if (payloadLength == 0) {
            //log
            return;
        } else if (config.getMaxPayloadBytes() > -1 && payloadLength > config.getMaxPayloadBytes()) {
            //log
            return;
        }

        String digest = getDigest(record);

        String urlExt = getExtension(recordHeader.getUrl());
        Path outputFile = getOutputFile(digest, urlExt);



        extractorStats.visitedHeaderMime(httpMediaType);
        //put this after the header mime visit so that the
        //header is still counted
        if (!config.isOverwrite() && Files.exists(outputFile)) {
            return;
        }

        if (!config.earlySelectExclude(urlExt, httpMediaType)) {
            return;
        }

        //now read the bytes and process appropriately
        handlePayload(record, urlExt, httpMediaType);
        */
    }



    private Path getOutputFile(String digest, String extension) {
        return config.getOutputDir().resolve(
                digest.substring(0, 2) + "/" +
                        digest + extension);
    }

    private void handlePayload(ArchiveRecord record, String urlExtension,
                               MediaType httpMediaType) throws IOException {

        ByteArrayOutputStream raw = new ByteArrayOutputStream(record.available());
        //no idea why, but reading into a byte[record.available] failed
        //to read stream completely
        int read = 0;
        int available = record.available();
        int maxToSniff = config.getNumBytesToSniffForDetection() < 0 ?
                available : config.getNumBytesToSniffForDetection();

        while (read < available && read < maxToSniff) {
            raw.write(record.read());
            read++;
        }
        TikaInputStream tis = TikaInputStream.get(raw.toByteArray());
        MediaType detectedMediaType = detector.detect(tis, new Metadata());

        if (detectedMediaType.equals(MediaType.OCTET_STREAM)) {
            //if all you can tell is that this is an octet stream so far,
            //read the rest of the bytes and try again.
            available = record.available();
            read = 0;
            while (read < available) {
                raw.write(record.read());
                read++;
            }
            tis = TikaInputStream.get(raw.toByteArray());
            detectedMediaType = detector.detect(tis, new Metadata());
        }
        extractorStats.visitedDetectedMimes(detectedMediaType);
        if (! config.select(urlExtension, httpMediaType, detectedMediaType)) {
            return;
        }

        //now, just make sure that you've read all the bytes
        available = record.available();
        read = 0;
        while (read < available) {
            raw.write(record.read());
            read++;
        }

        String digest = getDigest(record);
        if (digest == null) {
            digest = digest(raw.toByteArray());
        }
        String extension = pickExtension(urlExtension, httpMediaType, detectedMediaType);
        Path outFile = getOutputFile(digest, extension);
        if (! config.isOverwrite() && Files.exists(outFile)) {
            //log
            return;
        }
        Files.createDirectories(outFile.getParent());
        OutputStream os = Files.newOutputStream(outFile);
        IOUtils.copy(new ByteArrayInputStream(raw.toByteArray()), os);
        os.flush();
        os.close();
        extractorStats.extractedRecord();
    }

}
