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

package org.tallison.cc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.Header;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.RedirectLocations;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCRecord;
import org.archive.util.LaxHttpParser;
import org.tallison.cc.index.CCIndexRecord;

/**
 * Class to read in an index file or a subset of an index file
 * and to "get" those files from cc to a local directory
 *
 * This relies heavily on centic9's CommonCrawlDocumenDownload.
 * Thank you, Dominik!!!
 */
public class CCGetter {

    enum FETCH_STATUS {
        BAD_URL, //0
        FETCHED_IO_EXCEPTION,//1
        FETCHED_NOT_200,//2
        FETCHED_IO_EXCEPTION_READING_ENTITY,//3
        FETCHED_IO_EXCEPTION_SHA1,//4
        ALREADY_IN_REPOSITORY,//5
        FETCHED_EXCEPTION_COPYING_TO_REPOSITORY,//6
        ADDED_TO_REPOSITORY; //7
    }

    private final static String AWS_BASE = "https://commoncrawl.s3.amazonaws.com/";
    static Logger logger = Logger.getLogger(CCGetter.class);

    private Base32 base32 = new Base32();
    private boolean writtenHeader = false;

    private final String proxyHost;
    private final int proxyPort;

    public CCGetter(String proxyHost, int proxyPort) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
    }

    private void execute(Path indexFile, Path rootDir, Path statusFile) throws IOException {

        int count = 0;
        BufferedWriter writer = Files.newBufferedWriter(statusFile, StandardCharsets.UTF_8);
        InputStream is = null;
        try {
            if (indexFile.endsWith(".gz")) {
                is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(indexFile)));
            } else {
                is = new BufferedInputStream(Files.newInputStream(indexFile));
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                while (line != null) {
                    processRow(line, rootDir, writer);
                    if (++count % 100 == 0) {
                        logger.info(indexFile.getFileName().toString() + ": " + count);
                    }
                    line = reader.readLine();
                }

            }
        } finally {
            IOUtils.closeQuietly(is);
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processRow(String row, Path rootDir, BufferedWriter writer) throws IOException {
        for (CCIndexRecord r : CCIndexRecord.parseRecords(row)) {
            fetch(r, rootDir, writer);
        }
    }

    private void fetch(CCIndexRecord r, Path rootDir, BufferedWriter writer) throws IOException {
        Path targFile = rootDir.resolve(r.getDigest().substring(0, 2) + "/" + r.getDigest());

        if (Files.isRegularFile(targFile)) {
            writeStatus(r, FETCH_STATUS.ALREADY_IN_REPOSITORY, writer);
            logger.info("already retrieved:"+targFile.toAbsolutePath());
            return;
        }

        String url = AWS_BASE+r.getFilename();
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            logger.warn("Bad url: " + url);
            writeStatus(r, FETCH_STATUS.BAD_URL, writer);
            return;
        }
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpHost target = new HttpHost(uri.getHost());
        String urlPath = uri.getRawPath();
        if (uri.getRawQuery() != null) {
            urlPath += "?" + uri.getRawQuery();
        }
        HttpGet httpGet = null;
        try {
            httpGet = new HttpGet(urlPath);
        } catch (Exception e) {
            logger.warn("bad path " + uri.toString(), e);
            writeStatus(r, FETCH_STATUS.BAD_URL, writer);
            return;
        }
        if (proxyHost != null && proxyPort > -1) {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort, "http");
            RequestConfig requestConfig = RequestConfig.custom()
                    .setProxy(proxy).build();
            httpGet.setConfig(requestConfig);
        }
        httpGet.addHeader("Range", r.getOffsetHeader());
        HttpCoreContext coreContext = new HttpCoreContext();
        CloseableHttpResponse httpResponse = null;
        URI lastURI = null;
        try {
            httpResponse = httpClient.execute(target, httpGet, coreContext);
            RedirectLocations redirectLocations = (RedirectLocations) coreContext.getAttribute(
                    DefaultRedirectStrategy.REDIRECT_LOCATIONS);
            if (redirectLocations != null) {
                for (URI redirectURI : redirectLocations.getAll()) {
                    lastURI = redirectURI;
                }
            } else {
                lastURI = httpGet.getURI();
            }
        } catch (IOException e) {
            logger.warn("IOException for " + uri.toString(), e);
            writeStatus(r, FETCH_STATUS.FETCHED_IO_EXCEPTION, writer);
            return;
        }
        lastURI = uri.resolve(lastURI);

        if (httpResponse.getStatusLine().getStatusCode() != 200 && httpResponse.getStatusLine().getStatusCode() != 206) {
            logger.warn("Bad status for " + uri.toString() + " : " + httpResponse.getStatusLine().getStatusCode());
            writeStatus(r, FETCH_STATUS.FETCHED_NOT_200, writer);
            return;
        }
        Path tmp = null;
        Header[] headers = null;
        boolean isTruncated = false;
        try {
            //this among other parts is plagiarized from centic9's CommonCrawlDocumentDownload
            //probably saved me hours.  Thank you, Dominik!
            tmp = Files.createTempFile("cc-getter", "");
            try (InputStream is = new GZIPInputStream(httpResponse.getEntity().getContent())) {
                WARCRecord warcRecord = new WARCRecord(new FastBufferedInputStream(is), "", 0);
                ArchiveRecordHeader archiveRecordHeader = warcRecord.getHeader();
                if (archiveRecordHeader.getHeaderFields()
                        .containsKey(WARCConstants.HEADER_KEY_TRUNCATED)) {
                    isTruncated = true;
                }
                headers = LaxHttpParser.parseHeaders(warcRecord, "UTF-8");

                Files.copy(warcRecord,
                        tmp,
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            writeStatus(r, null, headers, 0L, isTruncated, FETCH_STATUS.FETCHED_IO_EXCEPTION_READING_ENTITY, writer);
            deleteTmp(tmp);
            return;
        }

        String digest = null;
        long tmpLength = 0l;
        try (InputStream is = Files.newInputStream(tmp)) {
            digest = base32.encodeAsString(DigestUtils.sha1(is));
            tmpLength = Files.size(tmp);
        } catch (IOException e) {
            writeStatus(r, null, headers, tmpLength, isTruncated, FETCH_STATUS.FETCHED_IO_EXCEPTION_SHA1, writer);
            logger.warn("IOException during digesting: " + tmp.toAbsolutePath());
            deleteTmp(tmp);
            return;
        }

        if (Files.exists(targFile)) {
            writeStatus(r, digest, headers, tmpLength, isTruncated, FETCH_STATUS.ALREADY_IN_REPOSITORY, writer);
            deleteTmp(tmp);
            return;
        }
        try {
            Files.createDirectories(targFile.getParent());
            Files.copy(tmp, targFile);
        } catch (IOException e) {
            writeStatus(r, digest, headers, tmpLength, isTruncated, FETCH_STATUS.FETCHED_EXCEPTION_COPYING_TO_REPOSITORY, writer);
            deleteTmp(tmp);

        }
        writeStatus(r, digest, headers, tmpLength, isTruncated, FETCH_STATUS.ADDED_TO_REPOSITORY, writer);
        deleteTmp(tmp);
    }

    private void writeStatus(CCIndexRecord r, FETCH_STATUS fetchStatus, BufferedWriter writer) throws IOException {
        writeStatus(r, null, null, -1l, false, fetchStatus, writer);
    }

    private void writeStatus(CCIndexRecord r, String actualDigest,
                             Header[] headers, long actualLength,
                             boolean isTruncated,
                             FETCH_STATUS fetchStatus, BufferedWriter writer) throws IOException {
        List<String> row = new LinkedList<>();


        if (! writtenHeader) {
            row.addAll(
                    Arrays.asList(new String[]{"URL", "CC_MIME", "CC_MIME_DETECTED",
                            "CC_LANGUAGES", "CC_CHARSET", "CC_DIGEST", "COMPUTED_DIGEST", "HEADER_ENCODING", "HEADER_TYPE",
                            "HEADER_LANGUAGE", "HEADER_LENGTH", "ACTUAL_LENGTH", "WARC_IS_TRUNCATED", "FETCH_STATUS"}));
            writer.write(StringUtils.join(row, "\t"));
            writer.write("\n");
            writer.flush();
            row.clear();
            writtenHeader = true;
        }

        row.add(clean(r.getUrl()));
        row.add(clean(r.getMime()));
        row.add(clean(r.getMimeDetected()));
        row.add(clean(r.getLanguages()));
        row.add(clean(r.getCharset()));
        row.add(clean(r.getDigest()));
        if (actualDigest != null) {
            row.add(actualDigest);
        } else {
            row.add("");
        }
        row.add(getHeader("content-encoding", headers));
        row.add(getHeader("content-type", headers));
        row.add(getHeader("content-language", headers));
        row.add(getHeader("content-length", headers));
        row.add(Long.toString(actualLength));
        if (isTruncated) {
            row.add("TRUE");
        } else {
            row.add("");
        }
        row.add(clean(fetchStatus.toString()));

        writer.write(StringUtils.join(row, "\t"));
        writer.write("\n");
        writer.flush();
    }

    private String getHeader(String headerNameLC, Header[] headers) {
        if (headers == null) {
            return "";
        }
        for (Header header : headers) {
            if (header.getName().equalsIgnoreCase(headerNameLC)) {
                return clean(header.getValue());
            }
        }
        return "";
    }

    static String clean(String s) {
        //make sure that the string doesn't contain \t or new line
        if (s == null) {
            return "";
        }

        if (s.startsWith("\"")) {
            s = s.substring(1);
        }
        if (s.endsWith("\"")) {
            s = s.substring(0,s.length()-1);
        }
        if (s.contains("\"")) {
            s = "\""+s.replaceAll("\"", "\"\"")+"\"";
        }
        return s.replaceAll("\\s", " ");
    }

    private void deleteTmp(Path tmp) {
        try {
            Files.delete(tmp);
        } catch (IOException e1) {
            logger.error("Couldn't delete tmp file: " + tmp.toAbsolutePath());
        }
    }

    public static void main(String[] args) throws IOException {
        Path indexFile = Paths.get(args[0]);
        Path rootDir = Paths.get(args[1]);
        Path statusFile = Paths.get(args[2]);
        String proxy = null;
        int proxyPort = -1;
        if (args.length > 3) {
            proxy = args[3];
            proxyPort = Integer.parseInt(args[4]);
        }
        CCGetter ccGetter = new CCGetter(proxy, proxyPort);
        ccGetter.execute(indexFile, rootDir, statusFile);
    }
}

