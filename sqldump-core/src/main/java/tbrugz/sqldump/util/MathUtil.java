package tbrugz.sqldump.util;

public class MathUtil {

	public static int sumInts(int[] ints) {
		int sum = 0;
		for(int i=0;i<ints.length;i++) {
			sum += ints[i];
		}
		return sum;
	}
	
	public static Integer minIgnoreNull(Integer... values) {
		Integer min = null;
		for(Integer i: values) {
			if(min==null) { min = i; continue; }
			if(min!=null && i!=null) { min = Math.min(min, i); }
		}
		return min;
	}

}
