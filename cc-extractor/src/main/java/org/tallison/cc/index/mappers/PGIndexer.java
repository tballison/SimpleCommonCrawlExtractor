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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.commons.lang3.StringUtils.truncate;

public class PGIndexer extends AbstractRecordProcessor {
    static Logger LOGGER = Logger.getLogger(PGIndexer.class);
    PreparedStatement insert;
    Connection connection;
    private static final AtomicInteger ADDED = new AtomicInteger(0);
    static AtomicInteger COUNTER = new AtomicInteger(0);
    private static final StringCache MIME_CACHE = new StringCache("mimes", 2000);
    private static final StringCache DETECTED_MIME_CACHE = new StringCache("detected_mimes", 2000);
    private static final StringCache LANGUAGE_CACHE = new StringCache("languages", 2000);
    private static final StringCache TRUNCATED_CACHE = new StringCache("truncated", 12);
    private static final long STARTED = System.currentTimeMillis();

    private final int id = COUNTER.getAndIncrement();
    private int added = 0;
    @Override
    public void init(String[] args) throws Exception {
        super.init(args);

        String user = args[0];
        String pw = args[1];
        String url = "jdbc:postgresql://localhost/commoncrawl?user="+user+"&password="+pw;
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        insert = connection.prepareStatement("insert into urls (digest, mime, mime_detected, charset, " +
                "languages, status, length, truncated) values" +
                " (?,?,?,?,?,?,?,?)");
        initTables(MIME_CACHE, DETECTED_MIME_CACHE, LANGUAGE_CACHE, TRUNCATED_CACHE);
    }

    private synchronized void initTables(StringCache ... caches) throws SQLException {
        if (id == 0) {
            connection.createStatement().execute("drop table if exists urls");
            connection.createStatement().execute("create table urls " +
                    "(" +
                    //"url varchar(60000)," +
                    " digest varchar(64)," +
                    " mime integer," +
                    " mime_detected integer," +
                    " charset varchar(64)," +
                    " languages integer,"+
                    " status integer,"+
                    " length integer," +
                    " truncated integer);");

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
            //if (mime.contains("onenote") || mimeDetected.contains("onenote")) {
                try {
                    int total= ADDED.getAndIncrement();
                    if (++added % 100000 == 0) {
                        insert.executeBatch();
                        connection.commit();
                        long elapsed = System.currentTimeMillis()-STARTED;
                        double elapsedSec = elapsed/1000;
                        double per = (double)total/elapsedSec;
                        System.out.println("committing "+added+ " ("+
                                        total+") in "+elapsed +
                                " ms " + per + " recs/per second");
                    }
                    int i = 0;
//                    insert.setString(++i, truncate(r.getUrl(), 60000));
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
                    insert.setInt(++i, r.getLength());
                    insert.setInt(++i, TRUNCATED_CACHE.getInt(r.getTruncated()));
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
            if (id == 0) {
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
