package helpers;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class to implement HashMap sorting by key and by value.
 */
public class MapHelper {
    public static <K, V extends Comparable<? super V>> java.util.Map<K, V> sortByValue(java.util
                                                                                          .Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(java.util.Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        java.util.Map.Entry::getKey,
                        java.util.Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    public static <K extends Comparable<? super K>, V> java.util.Map<K, V> sortByKey(java.util
                                                                                            .Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(java.util.Map.Entry.comparingByKey(new Comparator<K>() {
                    @Override
                    public int compare(K k, K t1) {
                        String first = k.toString();
                        String second = t1.toString();

                        CustomKeyComparator keyComparator = new CustomKeyComparator();
                        return keyComparator.compare(first, second);
                    }
                }))
                .collect(Collectors.toMap(
                        java.util.Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}
