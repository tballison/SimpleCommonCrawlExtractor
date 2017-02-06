package org.tallison.cc.index.mappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.IndexRecordProcessor;


abstract class AbstractRecordProcessor implements IndexRecordProcessor {

    private static Gson gson = new GsonBuilder().create();
    protected static AtomicInteger threadCounter = new AtomicInteger(0);

    private final int threadNumber;

    public AbstractRecordProcessor() {
        threadNumber = threadCounter.incrementAndGet();
    }


    public void init(String[] args) throws Exception {
        if (args[0].equals("-h") || args[0].equals("--help")) {
            usage();
            System.exit(1);
        }

    }

    abstract void usage();

    protected int getThreadNumber() {
        return threadNumber;
    }

    protected List<CCIndexRecord> parseRecords(String row) {
        AtomicInteger i = new AtomicInteger(0);
        List<CCIndexRecord> records = new ArrayList<>();
        //for now turn off multi row splitting
        //while (i.get() < row.length()) {
            CCIndexRecord record = parseRecord(row, i);
            if (record != null) {
                records.add(record);
            }/* else {
                break;
            }*/
        //}
        return records;

    }

    private CCIndexRecord parseRecord(String row, AtomicInteger i) {
        int urlI = row.indexOf(' ',i.get());
        int dateI = row.indexOf(' ', urlI+1);

        if (dateI == -1) {
            //barf
            return null;
        }
        int end = row.indexOf('}',dateI+1);
        if (end == -1) {
            //barf
            return null;
        }
        i.set(end+1);
//        String json = row.substring(dateI);
        String json = row.substring(dateI, end+1);
        CCIndexRecord r;
        try {
            r = gson.fromJson(json, CCIndexRecord.class);
        } catch (JsonSyntaxException e) {
            System.out.println(">>>"+row+"<<<");
            e.printStackTrace();
            return null;
        }
        return r;
    }

    String getExtension(String u) {
        if (u == null || u.length() == 0) {
            return null;
        }
        int i = u.lastIndexOf('.');
        if (i < 0 || i+6 < u.length()) {
            return null;
        }
        String ext = u.substring(i+1);
        ext = ext.trim();
        Matcher m = Pattern.compile("^\\d+$").matcher(ext);
        if (m.find()) {
            return null;
        }
        ext = ext.toLowerCase(Locale.ENGLISH);
        ext = ext.replaceAll("\\/$", "");
        return ext;
    }

    //returns "" if key is null, otherwise, trims and converts remaining \r\n\t to " "
    protected static String clean(String key) {
        if (key == null) {
            return "";
        }
        return key.trim().replaceAll("[\r\n\t]", " ");
    }

}
