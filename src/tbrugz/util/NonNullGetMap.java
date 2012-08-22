package tbrugz.util;

import java.util.HashMap;

//XXX should this be a decorator?
public class NonNullGetMap<K,V> extends HashMap<K,V> {
	
	Class<V> initialValueClass;
	
	public NonNullGetMap(Class<V> initialValueClass) {
		this.initialValueClass = initialValueClass;
	}
	
	@Override
	public V get(Object key) { //(K key)
		V v = super.get(key);
		if(v==null) {
			try {
				v = initialValueClass.newInstance();
				put((K)key, v);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return v;
	}
}

