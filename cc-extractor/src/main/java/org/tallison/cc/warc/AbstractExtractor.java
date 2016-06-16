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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.Header;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;

public abstract class AbstractExtractor {

    public final static String UNKNOWN_EXTENSION = ".unk";

    private final static Pattern URL_EXTENSION_PATTERN =
            Pattern.compile("(?i)\\.([a-z0-9]{1,8})(?:\\Z|[ \\?])");

    private final Base32 base32 = new Base32();
    final Detector detector;
    private final TikaConfig tikaConfig;
    final ExtractorStats extractorStats;
    final Path warcPath;


    public AbstractExtractor(Path warcPath) {
        this.warcPath = warcPath;
        this.detector = new DefaultDetector();
        this.tikaConfig = TikaConfig.getDefaultConfig();
        extractorStats = new ExtractorStats();
    }

    public ExtractorStats execute() throws IOException {
        extractorStats.start();
        if (Files.isDirectory(warcPath)) {
            processDirectory(warcPath);
        } else {
            processWarc(warcPath);
        }
        extractorStats.finish();
        return extractorStats;
    }

    private void processDirectory(Path directory) throws IOException {
        DirectoryStream<Path> ds = Files.newDirectoryStream(directory);
        for (Path p : ds) {
            System.err.println("processing warc1: " + p);
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
                System.err.println(extractorStats);
            }
        }
    }

    abstract void handleRecord(ArchiveRecord record) throws IOException;

    static String getDigest(ArchiveRecord record) {

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

    String digest(byte[] bytes) {
        return base32.encodeToString(DigestUtils.sha1(bytes));
    }

    String pickExtension(String urlExtension, MediaType httpMediaType, MediaType detectedMediaType) {
        if (detectedMediaType != null && !detectedMediaType.equals(MediaType.OCTET_STREAM)) {
            return getExtension(detectedMediaType);
        }
        if (httpMediaType != null && !detectedMediaType.equals(MediaType.OCTET_STREAM)) {
            return getExtension(httpMediaType);
        }
        if (urlExtension != null) {
            return urlExtension;
        }
        return UNKNOWN_EXTENSION;
    }

    String getExtension(MediaType mediaType) {
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

    MediaType getHttpMediaType(Header[] headers) {
        MediaType httpMediaType = null;
        for (Header h : headers) {
            if (h.getName().equalsIgnoreCase("content-type")) {
                String v = h.getValue();
                httpMediaType = MediaType.parse(v);
            }
        }
        return httpMediaType;
    }

    static String getExtension(final String fullUrl) {

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
