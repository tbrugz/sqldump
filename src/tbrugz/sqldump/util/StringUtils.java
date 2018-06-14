package tbrugz.sqldump.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class StringUtils {

	// see: http://stackoverflow.com/questions/15567010/what-is-a-good-alternative-of-ltrim-and-rtrim-in-java
	static final String NL = "\n";
	private final static Pattern RTRIM = Pattern.compile("\\s+$");

	public static boolean equalsWithTrim(String s1, String s2) {
		return s1.trim().equals(s2.trim());
	}

	public static boolean equalsWithRightTrim(String s1, String s2) {
		return rtrim(s1).equals(rtrim(s2));
	}
	
	public static boolean equalsNullsAllowed(String s1, String s2) {
		return (s1==null && s2==null) ? true :
			(s1==null || s2==null) ? false :
			s1.equals(s2);
	}

	public static boolean equalsWithUpperCase(String s1, String s2) {
		return s1.toUpperCase().equals(s2.toUpperCase());
	}
	
	public static boolean equalsNullsAsEmpty(String s1, String s2) {
		if(s1==null) { s1 = ""; }
		if(s2==null) { s2 = ""; }
		return s1.equals(s2);
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
	
	// http://stackoverflow.com/a/309718/616413
	public static String readInputStream(final InputStream is, final int bufferSize) throws IOException {
		final char[] buffer = new char[bufferSize];
		final StringBuilder out = new StringBuilder();
		Reader in = new InputStreamReader(is, "UTF-8");
		for (;;) {
			int rsz = in.read(buffer, 0, buffer.length);
			if (rsz < 0)
				break;
			out.append(buffer, 0, rsz);
		}
		return out.toString();
	}
	
	public static boolean equalsIgnoreWhitespacesEachLine(String s1, String s2) {
		if(s1==null && s2==null) { return true; }
		if(s1==null || s2==null) { return false; }
		
		String[] s1a = s1.split(NL);
		String[] s2a = s2.split(NL);
		
		if(s1a.length!=s2a.length) { return false; }
		
		for(int i=0;i<s1a.length;i++) {
			//if(!StringUtils.equalsWithTrim(s1a[i], s2a[i])) { return false; }
			if(!StringUtils.equalsWithRightTrim(s1a[i], s2a[i])) { return false; }
		}
		return true;
	}
	
	// http://stackoverflow.com/a/1102916/616413
	/* public static boolean isNumeric(String str) {
		try {
			Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	} */

}
