package tbrugz.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

//XXXxx should this be a decorator? yes!
public class NonNullGetMap<K,V> implements Map<K,V> {
	
	final Map<K,V> map;
	final Class<V> initialValueClass;
	final GenericFactory<V> factory;
	
	public NonNullGetMap(Map<K,V> map, Class<V> initialValueClass) {
		this.map = map;
		//this.initialValueClass = (Class<V>) map.values().iterator().next().getClass();
		this.initialValueClass = initialValueClass;
		this.factory = null;
	}

	public NonNullGetMap(Map<K,V> map, GenericFactory<V> factory) {
		this.map = map;
		this.initialValueClass = null;
		this.factory = factory;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) { //(K key)
		V v = map.get(key);
		if(v==null) {
			try {
				if(factory!=null) {
					v = factory.getInstance();
				}
				else {
					v = initialValueClass.newInstance();
				}
				put((K)key, v);
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
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

