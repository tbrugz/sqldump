package tbrugz.sqldump.resultset.pivot;

import java.util.Arrays;

public class Key implements Comparable<Key> {
	final Object[] values;
	
	public Key(Object[] values) {
		if(values==null) {
			throw new RuntimeException("values may not be null");
		}
		this.values = values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Key["+Arrays.toString(values)+"]";
	}

	// Compares this object with the specified object for order.
	// Returns a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
	@SuppressWarnings({"unchecked","rawtypes"})
	@Override
	public int compareTo(Key o) {
		//int ret = (values.length < o.values.length)?-1:
		//			(values.length > o.values.length)?1:0;
		//if(ret!=0) { return ret; }
		int len = Math.min(values.length, o.values.length);
		
		for(int i=0;i<len;i++) {
			Object v = values[i];
			Object ov = o.values[i];
			//System.out.println("v: "+v+" ["+v.getClass()+"] / ov: "+ov+" ["+ov.getClass()+"]");
			if(!v.equals(ov)) {
				
				if(v instanceof Comparable) {
					Comparable c = (Comparable) v;
					Comparable oc = (Comparable) ov;
					return c.compareTo(oc);
				}
				if(v instanceof Number) {
					Number n = (Number) v;
					Number on = (Number) ov;
					return on.intValue()-n.intValue();
				}
				/*if(v instanceof String) {
					return values[i].toString().compareTo(o.values[i].toString());
				}
				if(v instanceof Date) {
					Date d = (Date) v;
					Date od = (Date) ov;
					return d.compareTo(od);
				}*/
				return String.valueOf(v).compareTo(String.valueOf(ov));
				//throw new IllegalArgumentException("v: "+v+" ["+v.getClass()+"]");
			}
		}
		if((values.length < o.values.length)) { return -1; }
		if((values.length > o.values.length)) { return 1; }
		return 0;
	}

}
