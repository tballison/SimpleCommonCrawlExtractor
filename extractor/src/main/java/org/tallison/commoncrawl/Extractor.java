package org.tallison.commoncrawl;/*
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.Header;
import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.util.LaxHttpParser;


public class Extractor {

    public final static String UNKNOWN_EXTENSION = ".unk";

    private final static Pattern URL_EXTENSION_PATTERN =
            Pattern.compile("(?i)\\.([a-z0-9]{1,8})(?:\\Z|[ \\?])");

    private final ExtractorConfig config;
    private final Detector detector;
    private final TikaConfig tikaConfig;
    private final ExtractorStats extractorStats;


    public Extractor(ExtractorConfig config) {
        this.config = config;
        this.detector = new DefaultDetector();
        this.tikaConfig = TikaConfig.getDefaultConfig();
        extractorStats = new ExtractorStats();
    }

    public ExtractorStats execute() throws IOException {
        extractorStats.start();
        if (Files.isDirectory(config.getWarcPath())) {
            processDirectory(config.getWarcPath());
        } else {
            processWarc(config.getWarcPath());
        }
        extractorStats.finish();
        return extractorStats;
    }

    private void processDirectory(Path directory) throws IOException {
        DirectoryStream<Path> ds = Files.newDirectoryStream(directory);
        for (Path p : ds) {
            System.out.println("processing warc1: " + p);
            if (Files.isDirectory(p)) {
                processDirectory(p);
            } else if (p.getFileName().toString().endsWith(".warc.gz") ||
                    p.getFileName().toString().endsWith(".warc")) {
                processWarc(p);
            }
        }
    }

    private void processWarc(Path warcFile) throws IOException {
        extractorStats.addWarc(warcFile.getFileName().toString());
        InputStream is = Files.newInputStream(warcFile);
        ArchiveReader reader = WARCReaderFactory.get(warcFile.toString(), is, true);

        int i = 0;
        reader.setStrict(false);
        for (ArchiveRecord record : reader) {
            record.setStrict(false);
            extractorStats.visitedRecord();
            handleRecord(record);
            if (i++ % 1000 == 0) {
                System.out.println(extractorStats);
            }
        }
    }

    private void handleRecord(ArchiveRecord record) throws IOException {
        ArchiveRecordHeader recordHeader = record.getHeader();
        if (!recordHeader.getMimetype().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE)) {
            return;
        }

        if (recordHeader.getHeaderFields().containsKey(WARCConstants.HEADER_KEY_TRUNCATED)) {
            extractorStats.truncatedRecord();
        }

        if (config.isIncludeTruncated() || ! recordHeader.getHeaderFields()
                .containsKey(WARCConstants.HEADER_KEY_TRUNCATED)) {
            //do nothing
        } else {
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


        Header[] headers = LaxHttpParser.parseHeaders(record, "ISO-8859-1");

        MediaType httpMediaType = getHttpMediaType(headers);

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
    }

    private String getDigest(ArchiveRecord record) {

        ArchiveRecordHeader recordHeader = record.getHeader();
        String digest = recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST).toString();
        if (digest == null) {
            return null;
        }
        int colon = digest.indexOf(":");
        if (colon > -1) {
            digest = digest.substring(colon + 1);
        }
        return digest;

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

        String extension = pickExtension(urlExtension, httpMediaType, detectedMediaType);
        Path outFile = getOutputFile(digest, extension);
        if (! config.isOverwrite() && Files.exists(outFile)) {
            //log
            return;
        }
        Files.createDirectories(outFile.getParent());
        System.out.println("extracting file: " + outFile);
        OutputStream os = Files.newOutputStream(outFile);
        IOUtils.copy(new ByteArrayInputStream(raw.toByteArray()), os);
        os.flush();
        os.close();
        extractorStats.extractedRecord();
    }

    private String pickExtension(String urlExtension, MediaType httpMediaType, MediaType detectedMediaType) {
        if (detectedMediaType != null && ! detectedMediaType.equals(MediaType.OCTET_STREAM)) {
            return getExtension(detectedMediaType);
        }
        if (httpMediaType != null && ! detectedMediaType.equals(MediaType.OCTET_STREAM)) {
            return getExtension(httpMediaType);
        }
        if (urlExtension != null) {
            return urlExtension;
        }
        return UNKNOWN_EXTENSION;
    }

    public String getExtension(MediaType mediaType) {
        MimeTypes types = tikaConfig.getMimeRepository();
        try {
            MimeType mimeType = types.getRegisteredMimeType(mediaType.toString());
            if (mimeType == null) {
                mimeType = types.getRegisteredMimeType(mediaType.getBaseType().toString());
            }
            return mimeType.getExtension();
        } catch (MimeTypeException e) {

        }
        return null;
    }

    private MediaType getHttpMediaType(Header[] headers) {
        MediaType httpMediaType = null;
        for (Header h : headers) {
            if (h.getName().equalsIgnoreCase("content-type")) {
                String v = h.getValue();
                httpMediaType = MediaType.parse(v);
            }
        }
        return httpMediaType;
    }

    private static String getExtension(final String fullUrl) {

        String urlString = fullUrl;
        //try to get the file
        URL url = null;
        try {
            url = new URL(fullUrl);
        } catch (MalformedURLException e) {
        }
        if (url != null) {
            urlString = url.getFile();
        }

        //stop looking after the first ?
        int q = urlString.indexOf("?");
        if (q > -1) {
            urlString = urlString.substring(0, q);
        }

        Matcher m = URL_EXTENSION_PATTERN.matcher(urlString);
        String ext = "";
        while (m.find()) {
            ext = m.group(1).toLowerCase(Locale.ENGLISH);
        }
        if ("htm".equals(ext)) {
            return ".html";
        }
        return ".ext";
    }

    public ExtractorStats getExtractorStats() {
        return extractorStats;
    }
}
