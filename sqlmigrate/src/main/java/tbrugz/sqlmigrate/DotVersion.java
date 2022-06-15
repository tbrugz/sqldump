package tbrugz.sqlmigrate;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.Utils;

public class DotVersion implements Comparable<DotVersion> {

	final List<Integer> version;
	
	public DotVersion(List<Integer> version) {
		validateVersion(version);
		this.version = version;
	}
	
	public static void validateVersion(List<Integer> version) {
		if(version==null) {
			throw new IllegalArgumentException("null");
		}
		if(version.size()==0) {
			throw new IllegalArgumentException(version.toString());
		}
		for(Integer i: version) {
			if(i==null) {
				throw new IllegalArgumentException(version.toString());
			}
		}
	}
	
	public static List<Integer> getIntListVersion(String str) {
		try {
			String[] parts = str.split("[\\.-]");
			List<Integer> ret = new ArrayList<>();
			for(String s: parts) {
				int i = Integer.parseInt(s);
				ret.add(i);
			}
			return ret;
		}
		catch(NumberFormatException e) {
			throw new IllegalArgumentException("Invalid version: "+str);
		}
	}
	
	public static boolean isValidVersion(String str) {
		try {
			getIntListVersion(str);
			return true;
		}
		catch(IllegalArgumentException e) {
			return false;
		}
	}
	
	public static DotVersion getDotVersion(String str) {
		return new DotVersion(getIntListVersion(str));
	}
	
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof DotVersion)) {
			return false;
		}
		DotVersion other = (DotVersion) obj;
		return version.equals(other.version);
	}
	
	@Override
	public int hashCode() {
		return version.hashCode();
	}
	
	@Override
	public String toString() {
		return String.valueOf(version);
	}
	
	@Override
	public int compareTo(DotVersion o) {
		int size = this.version.size();
		int othersize = o.version.size();
		for(int i=0;i<size && i<othersize;i++) {
			int cmp = this.version.get(i).compareTo(o.version.get(i));
			if(cmp!=0) { return cmp; }
		}
		return size - othersize;
	}
	
	public String asVersionString() {
		return Utils.join(version, ".");
	}

}
