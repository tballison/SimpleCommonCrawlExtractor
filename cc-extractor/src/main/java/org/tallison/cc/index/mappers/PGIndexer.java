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
package org.tallison.cc.index.mappers;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.tika.io.IOExceptionWithCause;
import org.tallison.cc.index.CCIndexRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.commons.lang3.StringUtils.truncate;

public class PGIndexer extends AbstractRecordProcessor {
    private static final int MAX_URL_LENGTH = 10000;
    static Logger LOGGER = Logger.getLogger(PGIndexer.class);
    PreparedStatement insert;
    Connection connection;
    private static final AtomicLong ADDED = new AtomicLong(0);
    private static final AtomicLong CONSIDERED = new AtomicLong(0);
    static AtomicInteger THREAD_COUNTER = new AtomicInteger(-1);
    static AtomicInteger THREAD_CLOSED = new AtomicInteger(-1);

    private static final StringCache MIME_CACHE = new StringCache("mimes", 2000);
    private static final StringCache DETECTED_MIME_CACHE = new StringCache("detected_mimes", 2000);
    private static final StringCache LANGUAGE_CACHE = new StringCache("languages", 2000);
    private static final StringCache TRUNCATED_CACHE = new StringCache("truncated", 12);
    private static final StringCache WARC_FILENAME_CACHE =
            new StringCache("warc_file_name", 200);

    private static final long STARTED = System.currentTimeMillis();

    private final int id = THREAD_COUNTER.incrementAndGet();
    private long added = 0;
    @Override
    public void init(String[] args) throws Exception {
        super.init(args);

        String url = null;
        if (args.length == 1) {
            url = args[0];
        } else if (args.length == 2) {
            String user = args[0];
            String pw = args[1];
            url = "jdbc:postgresql://localhost/commoncrawl?user="+user+"&password="+pw;
        } else if (args.length == 3) {
            String user = args[0];
            String pw = args[1];
            Integer port = Integer.parseInt(args[2]);
            url = "jdbc:postgresql://localhost:"+port+"/commoncrawl?user="+user+"&password="+pw;
        }
        System.out.println("trying to connect: "+url);
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        insert = connection.prepareStatement("insert into urls (" +
                "url,"+
                "digest, mime, mime_detected, charset, " +
                "languages, status, truncated, warc_file_name, warc_offset, warc_length) values" +
                " (" +
                "?," +
                "?,?,?,?,?,?,?,?,?,?)");
        initTables(MIME_CACHE, DETECTED_MIME_CACHE, LANGUAGE_CACHE, TRUNCATED_CACHE, WARC_FILENAME_CACHE);
    }

    private synchronized void initTables(StringCache ... caches) throws SQLException {
        if (id == 0) {
            connection.createStatement().execute("drop table if exists urls");
            connection.createStatement().execute("create table urls " +
                    "(" +
                    "url varchar("+MAX_URL_LENGTH+")," +
                    " digest varchar(64)," +
                    " mime integer," +
                    " mime_detected integer," +
                    " charset varchar(64)," +
                    " languages integer,"+
                    " status integer,"+
                    " truncated integer," +
                    " warc_file_name integer," +
                    " warc_offset bigint," +
                    " warc_length bigint);");

            for (StringCache cache : caches) {
                connection.createStatement().execute("drop table if exists "+cache.getTableName());
                connection.createStatement().execute("create table "+cache.getTableName()+
                        "(id integer primary key," +
                        "name varchar("+cache.getMaxLength()+"))");
                cache.prepareStatement(connection);
            }

            connection.commit();
        }
    }

    private boolean tableExists(String tableName) throws SQLException {
        String sql = "SELECT EXISTS (\n" +
                "   SELECT 1\n" +
                "   FROM   information_schema.tables \n" +
                "   WHERE  table_schema = 'schema_name'\n" +
                "   AND    table_name = '"+tableName+"'\n"+
                "   );";

        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    @Override
    void usage() {

    }

    @Override
    public void process(String json) throws IOException {
        List<CCIndexRecord> records = CCIndexRecord.parseRecords(json);
        for (CCIndexRecord r : records) {
            String mime = CCIndexRecord.normalizeMime(r.getMime());
            String mimeDetected = CCIndexRecord.normalizeMime(r.getMimeDetected());
            CONSIDERED.incrementAndGet();
            String url = r.getUrl();
            String u = (url == null) ? "" : url.toLowerCase(Locale.US);

            /*if (mimeDetected != null &&
                    (mimeDetected.equals("text/html") || mimeDetected.equals("application/xhtml+xml"))) {
                return;
            }*/
            //if (mime.contains("onenote") || mimeDetected.contains("onenote")) {
                try {
                    long total= ADDED.getAndIncrement();
                    if (++added % 100000 == 0) {
                        insert.executeBatch();
                        connection.commit();
                        long elapsed = System.currentTimeMillis()-STARTED;
                        double elapsedSec = (double)elapsed/(double)1000;
                        double per = (double)total/elapsedSec;
                        System.out.println("considered: "+CONSIDERED.get());
                        System.out.println("committing "+added+ " ("+
                                        total+") in "+elapsed +
                                " ms " + per + " recs/per second");
                    }
                    int i = 0;
                    insert.setString(++i, truncate(r.getUrl(), MAX_URL_LENGTH));
                    insert.setString(++i, r.getDigest());
                    insert.setInt(++i, MIME_CACHE.getInt(mime));
                    insert.setInt(++i, DETECTED_MIME_CACHE.getInt(mimeDetected));
                    if (StringUtils.isEmpty(r.getCharset())) {
                        insert.setString(++i, "");
                    } else {
                        insert.setString(++i, truncate(r.getCharset(), 64));
                    }
                    insert.setInt(++i, LANGUAGE_CACHE.getInt(getPrimaryLanguage(r.getLanguages())));
                    insert.setInt(++i, r.getStatus());
//                    insert.setInt(++i, r.getLength());
                    insert.setInt(++i, TRUNCATED_CACHE.getInt(r.getTruncated()));
                    insert.setInt(++i, WARC_FILENAME_CACHE.getInt(r.getFilename()));
                    insert.setInt(++i, r.getOffset());
                    insert.setInt(++i, r.getLength());
                    insert.addBatch();
                    LOGGER.debug(
                            StringUtils.joinWith("\t",
                                    r.getUrl(),
                                    r.getDigest(),
                                    mime, mimeDetected)
                    );
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        //}
    }

    private String getPrimaryLanguage(String languages) {
        if (languages == null) {
            return "";
        }
        String[] langs = languages.split(",");
        if (langs.length > 0) {
            return langs[0];
        } else {
            return languages;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            int closed = THREAD_CLOSED.incrementAndGet();
            if (closed == THREAD_COUNTER.get()) {
                for (StringCache cache : new StringCache[]{MIME_CACHE, DETECTED_MIME_CACHE, TRUNCATED_CACHE, LANGUAGE_CACHE}) {
                    cache.close();
                }
            }
            insert.executeBatch();
            insert.close();

            connection.commit();
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    private static class StringCache {

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final Map<String, Integer> map = new HashMap<>();

        private PreparedStatement insert;
        private final String tableName;
        private final int maxLength;

        StringCache(String tableName, int maxLength) {
            this.tableName = tableName;
            this.maxLength = maxLength;

        }
        private void prepareStatement(Connection connection) throws SQLException {
            insert = connection.prepareStatement("insert into "+tableName+" (id, name) values (?,?)");
        }


        int getInt(String s) throws SQLException {
            lock.readLock().lock();
            String key = s;
            if (key == null) {
                key = "";
            }
            if (key.length() > maxLength) {
                key = key.substring(0, maxLength);
            }
            try {
                if (map.containsKey(key)) {
                    return map.get(key);
                }
            } finally {
                lock.readLock().unlock();
            }
            try {
                lock.writeLock().lock();
                //need to recheck state
                if (map.containsKey(key)) {
                    return map.get(key);
                } else {
                    int index = map.size();
                    if (index > Integer.MAX_VALUE - 10) {
                        throw new RuntimeException("TOO MANY IN CACHE!");
                    }
                    map.put(key, index);
                    insert.clearParameters();
                    insert.setInt(1, index);
                    insert.setString(2, key);
                    insert.execute();
                }
            } finally {
                lock.writeLock().unlock();
            }
            return map.get(key);
        }

        public String getTableName() {
            return tableName;
        }

        public int getMaxLength() {
            return maxLength;
        }

        public void close() throws SQLException {
            insert.close();
        }
    }
}
