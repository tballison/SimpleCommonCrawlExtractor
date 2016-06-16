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
package org.tallison.wikilinks;

import org.apache.commons.lang.StringUtils;
import org.tallison.URLUtil;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

/**
 * Class to extract and filter links from
 * wikipedia external link dumps
 */
public class ExternalLinkExtractor {
    private final static int BACKSLASH = (int)'\\';
    private final static int COMMA = (int)',';
    private final static int OPEN_PAREN = (int)'(';
    private final static int CLOSE_PAREN = (int)')';
    private final static int SQUOTE = (int)'\'';
    URLUtil urlUtil = new URLUtil();

    public static void main(String[] args) throws Exception {
        ExternalLinkExtractor ex = new ExternalLinkExtractor();
        Path gzLinksFile = Paths.get(args[0]);
        Path table = Paths.get(args[1]);
        ex.execute(gzLinksFile, table);
    }

    private void execute(Path gzLinksFile, Path table) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(table, StandardCharsets.UTF_8);
        int recordCount = 0;
        try (InputStream is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(gzLinksFile), 10000000))) {
            readToVALUES(is);
            boolean keepGoing = true;
            while (keepGoing) {
                int chr = is.read();

                switch (chr) {
                    case OPEN_PAREN:
                        readRecord(is, writer);
                        recordCount++;
                        if (recordCount % 1000 == 0) {
                            System.out.println(recordCount);
                        }
                        break;
                    case -1:
                        keepGoing = false;
                        break;
                }
            }
        } finally {
            writer.flush();
            writer.close();
        }
    }

    private void readToVALUES(InputStream is) throws IOException {
        //abomination
        while (true) {
            int c = is.read();
            if (c == (int)'V') {
                c = is.read();
                if (c == (int)'A') {
                    c = is.read();
                    if (c == (int)'L') {
                        c = is.read();
                        if (c == (int)'U') {
                            c = is.read();
                            if (c == (int)'E') {
                                c = is.read();
                                if (c == (int)'S') {
                                    return;
                                }
                            }
                        }
                    }
                }
                if (c == -1) {
                    throw new IOException("EOF reached before finding Values");
                }
            }
        }
    }

    private void readRecord(InputStream is, BufferedWriter writer) throws IOException {
        boolean keepGoing = true;
        int colNumber = 0;
        OutputStream bos = new ByteArrayOutputStream();
        while (keepGoing) {
            int chr = is.read();
            switch (chr) {
                case -1:
                    throw new IOException("EOF reading record");
                case SQUOTE:
                    readToSquote(colNumber, is, bos);
                    break;
                case CLOSE_PAREN:
                    handleCell(colNumber, bos, writer);
                    return;
                case COMMA:
                    handleCell(colNumber, bos, writer);
                    ((ByteArrayOutputStream) bos).reset();
                    colNumber++;
                    break;
                default:
                    if (colNumber == 3) {
                        bos.write(chr);
                    }
                    break;
            }
        }
    }

    private void handleCell(int colNumber, OutputStream bos, BufferedWriter writer) throws IOException {
        if (colNumber != 3) {
            return;
        }

        String urlString = new String(((ByteArrayOutputStream)bos).toByteArray(), StandardCharsets.UTF_8);
        String origString = urlString;
        if (urlString.startsWith("//")) {
            urlString = "http:"+urlString;
        }
        urlString = urlString.replaceAll("\\s+", " ");//get rid of new lines and tabs
        urlString = urlUtil.clean(urlString);
        if (!StringUtils.isBlank(urlString)) {
            URI uri = null;
            try {
                uri = new URI(urlString);
            } catch (URISyntaxException e) {

            }
            String host = (uri == null) ? "" : uri.getHost();
            writer.write(host+"\t"+urlString);
            writer.newLine();
        }
    }

    private void readToSquote(int colNumber, InputStream is, OutputStream bos) throws IOException {
        boolean keepGoing = true;
        while (keepGoing) {
            int chr = is.read();
            switch (chr) {
                case -1:
                    throw new IOException("HIT EOF before end of squoted cell");
                case BACKSLASH:
                    int nxt = is.read();
                    if (nxt == -1) {
                        throw new IOException("EOF in squoted cell after backslash");
                    } else if (colNumber == 3){
                        bos.write(nxt);
                    }
                    break;
                case SQUOTE:
                    return;
                default:
                    if (colNumber == 3) {
                        bos.write(chr);
                    }
            }
        }
    }

}
