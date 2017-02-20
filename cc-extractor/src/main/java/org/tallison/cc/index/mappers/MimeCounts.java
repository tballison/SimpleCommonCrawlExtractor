package org.tallison.cc.index.mappers;

import java.util.HashMap;
import java.util.Map;


public class MimeCounts implements Comparable<MimeCounts> {
        private int total = 0;
        Map<String, Integer> m = new HashMap<>();
        void increment(String mime) {
            Integer cnt = m.get(mime);
            if (cnt == null) {
                cnt = new Integer(1);
            } else {
                cnt++;
            }
            m.put(mime, cnt);
            total++;
        }
        int getTotal() {
            return total;
        }


        @Override
        public int compareTo(MimeCounts o) {
            return new Integer(total).compareTo(new Integer(o.total));
        }
}
