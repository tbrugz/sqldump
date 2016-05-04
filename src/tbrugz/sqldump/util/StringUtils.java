package tbrugz.sqldump.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class StringUtils {

	// see: http://stackoverflow.com/questions/15567010/what-is-a-good-alternative-of-ltrim-and-rtrim-in-java
	private final static Pattern RTRIM = Pattern.compile("\\s+$");

	public static boolean equalsWithTrim(String s1, String s2) {
		return s1.trim().equals(s2.trim());
	}

	public static boolean equalsWithRightTrim(String s1, String s2) {
		return rtrim(s1).equals(rtrim(s2));
	}
	
	public static String rtrim(String s) {
		return RTRIM.matcher(s).replaceFirst("");
	}
	
	public static <T> List<String> getClassSimpleNameList(List<Class<T>> classes) {
		List<String> ret = new ArrayList<String>();
		for(Class<T> c: classes) {
			ret.add(c.getSimpleName());
		}
		return ret;
	}
	

}
