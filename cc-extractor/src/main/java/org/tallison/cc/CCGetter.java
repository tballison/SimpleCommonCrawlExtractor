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
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
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
                while (line != null && count < 200) {
                    processRow(line, rootDir, writer);
                    if (++count % 100 == 0) {

                        System.err.println(indexFile.getFileName().toString() + ": " + count);
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
        logger.info("going to get " + url);
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
        try {
            //this among other parts is plagiarized from centic9's CommonCrawlDocumentDownload
            //probably saved me hours.  Thank you, Dominik!
            tmp = Files.createTempFile("cc-getter", "");
            InputStream is = new GZIPInputStream(httpResponse.getEntity().getContent());
            WARCRecord warcRecord = new WARCRecord(new FastBufferedInputStream(is), "", 0);
            LaxHttpParser.parseHeaders(warcRecord,"UTF-8");

            Files.copy(warcRecord,
                    tmp,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            writeStatus(r, FETCH_STATUS.FETCHED_IO_EXCEPTION_READING_ENTITY, writer);
            deleteTmp(tmp);
            return;
        }

        String digest = null;
        try (InputStream is = Files.newInputStream(tmp)) {
            digest = base32.encodeAsString(DigestUtils.sha1(is));
        } catch (IOException e) {
            writeStatus(r, FETCH_STATUS.FETCHED_IO_EXCEPTION_SHA1, writer);
            logger.warn("IOException during digesting: " + tmp.toAbsolutePath());
            deleteTmp(tmp);
            return;
        }

        Path targFile = rootDir.resolve(digest.substring(0, 2) + "/" + digest);
        if (Files.exists(targFile)) {
            writeStatus(r, digest, FETCH_STATUS.ALREADY_IN_REPOSITORY, writer);
            deleteTmp(tmp);
            return;
        }
        try {
            Files.createDirectories(targFile.getParent());
            Files.copy(tmp, targFile);
        } catch (IOException e) {
            writeStatus(r, digest, FETCH_STATUS.FETCHED_EXCEPTION_COPYING_TO_REPOSITORY, writer);
            deleteTmp(tmp);

        }
        writeStatus(r, digest, FETCH_STATUS.ADDED_TO_REPOSITORY, writer);
        deleteTmp(tmp);
    }

    private void writeStatus(CCIndexRecord r, FETCH_STATUS fetchStatus, BufferedWriter writer) throws IOException {
        writeStatus(r, null, fetchStatus, writer);
    }

    private void writeStatus(CCIndexRecord r, String actualDigest, FETCH_STATUS fetchStatus, BufferedWriter writer) throws IOException {
        List<String> row = new LinkedList<>();
        row.add(clean(r.getUrl()));
        row.add(clean(r.getMime()));
        row.add(clean(r.getDigest()));
        if (actualDigest != null) {
            row.add(actualDigest);
        } else {
            row.add("");
        }
        row.add(Integer.toString(r.getLength()));
        row.add(clean(fetchStatus.toString()));

        writer.write(StringUtils.join(row, "\t"));
        writer.write("\n");
    }

    private String clean(String s) {
        //make sure that the string doesn't contain \t or new line
        if (s == null) {
            return "";
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

