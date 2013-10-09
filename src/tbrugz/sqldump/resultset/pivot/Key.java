package tbrugz.sqldump.resultset.pivot;

import java.util.Arrays;

public class Key {
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

}
