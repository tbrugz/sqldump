package tbrugz.sqldump.sqlrun;

public class Util {

	public static int sumInts(int[] ints) {
		int sum = 0;
		for(int i=0;i<ints.length;i++) {
			sum += ints[i];
		}
		return sum;
	}
}
