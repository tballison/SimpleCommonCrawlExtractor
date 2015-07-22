package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapValueSorter {


    public static Map<String, Integer> sortByValue(Map<String, Integer> m) {
        List<Map.Entry<String,Integer>> list = new ArrayList<>(m.entrySet());

        Collections.sort(list, new Comparator<Map.Entry <String, Integer>>() {
            public int compare(Map.Entry<String, Integer> e1,
                               Map.Entry<String, Integer> e2) {
                return (e2.getValue().compareTo(e1.getValue()));
            }
        });
        Map<String, Integer> sorted = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> e : list) {
            sorted.put(e.getKey(), e.getValue());
        }
        return sorted;
    }
}
