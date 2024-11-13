package tbrugz.sqldump.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @see: http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
 */
public class MapEntryValueComparator<K,V> implements Comparator<Map.Entry<K,V>> {
	public MapEntryValueComparator() {}
	public MapEntryValueComparator(boolean inverse) {
		this.inverse = inverse;
	}

	boolean inverse = false;
	
	@SuppressWarnings("unchecked")
	@Override
	public int compare(Entry<K,V> o1, Entry<K,V> o2) {
		if(inverse) {
			return ((Comparable<V>) o2.getValue())
					.compareTo(o1.getValue());
		}
		return ((Comparable<V>) o1.getValue())
				.compareTo(o2.getValue());
	}

	public static <K,V> Map<K,V> sortByValue(Map<K,V> map) {
		return sortByValue(map, false);
	}

	public static <K,V> Map<K,V> sortByValue(Map<K,V> map, boolean inverse) {
		List<Map.Entry<K,V>> list = new LinkedList<Map.Entry<K,V>>(map.entrySet());
		Collections.sort(list, new MapEntryValueComparator<K,V>(inverse));

		Map<K,V> result = new LinkedHashMap<K,V>();
		for (Iterator<Map.Entry<K,V>> it = list.iterator(); it.hasNext();) {
			Map.Entry<K,V> entry = it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
	
}
