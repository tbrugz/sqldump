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
public class MapEntryValueComparator implements Comparator<Map.Entry> {
	public MapEntryValueComparator() {}
	public MapEntryValueComparator(boolean inverse) {
		this.inverse = inverse;
	}

	boolean inverse = false;
	
	@Override
	public int compare(Entry o1, Entry o2) {
		if(inverse) {
			return ((Comparable) o2.getValue())
					.compareTo(o1.getValue());
		}
		return ((Comparable) o1.getValue())
				.compareTo(o2.getValue());
	}

	public static Map sortByValue(Map map) {
		return sortByValue(map, false);
	}

	public static Map sortByValue(Map map, boolean inverse) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new MapEntryValueComparator(inverse));

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry)it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	} 
	
}
