package tbrugz.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

//XXXxx should this be a decorator? yes!
public class NonNullGetMap<K,V> implements Map<K,V> {
	
	Map<K,V> map;
	Class<V> initialValueClass;
	
	public NonNullGetMap(Map<K,V> map, Class<V> initialValueClass) {
		this.map = map;
		//this.initialValueClass = (Class<V>) map.values().iterator().next().getClass();
		this.initialValueClass = initialValueClass;
	}
	
	@Override
	public V get(Object key) { //(K key)
		V v = map.get(key);
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

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Override
	public boolean equals(Object o) {
		return map.equals(o);
	}

	@Override
	public int hashCode() {
		return map.hashCode();
	}
	
}

