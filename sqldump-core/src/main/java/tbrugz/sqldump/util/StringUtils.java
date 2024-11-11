package tbrugz.sqldump.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import tbrugz.sqldiff.WhitespaceIgnoreType;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerUtil;

public class StringUtils {

	// see: http://stackoverflow.com/questions/15567010/what-is-a-good-alternative-of-ltrim-and-rtrim-in-java
	static final String NL = "\n";

	private final static Pattern LTRIM = Pattern.compile("\\A\\s+");
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
	
	public static String ltrim(String s) {
		return LTRIM.matcher(s).replaceFirst("");
	}

	public static String rtrim(String s) {
		return RTRIM.matcher(s).replaceFirst("");
	}

	public static String lrtrim(String s) {
		return ltrim(rtrim(s));
	}

	// https://stackoverflow.com/a/391978/616413
	public static String rightPad(String s, int n) {
		return String.format("%-" + n + "s", s);
	}

	/*public static String leftPad(String s, int n) {
		return String.format("%" + n + "s", s);
	}*/

	public static boolean contains(String[] arr, String s) {
		for(String ss: arr) {
			if(ss.equals(s)) { return true; }
		}
		return false;
	}
	
	public static List<Class<?>> getClassListFromObjectList(List<?> objects) {
		List<Class<?>> ret = new ArrayList<Class<?>>();
		if(objects != null) {
			for(Object o: objects) {
				ret.add(o!=null?o.getClass():null);
			}
		}
		return ret;
	}
	
	public static List<String> getClassSimpleNameList(List<Class<?>> classes) {
		List<String> ret = new ArrayList<String>();
		if(classes != null) {
			for(Class<?> c: classes) {
				ret.add(c!=null?c.getSimpleName():null);
			}
		}
		return ret;
	}

	public static <T> List<String> getClassSimpleNameListT(List<Class<T>> classes) {
		List<String> ret = new ArrayList<String>();
		if(classes != null) {
			for(Class<T> c: classes) {
				ret.add(c.getSimpleName());
			}
		}
		return ret;
	}
	
	public static List<String> getClassSimpleNameListFromObjectList(List<?> objects) {
		List<Class<?>> classes = getClassListFromObjectList(objects);
		return getClassSimpleNameList(classes);
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

	public static boolean equalsIgnoreWhitespacesEachLine(String s1, String s2, WhitespaceIgnoreType wsIgnore) {
		if(s1==null && s2==null) { return true; }
		if(s1==null || s2==null) { return false; }
		
		List<String> s1a = stringToLines(s1, wsIgnore);
		List<String> s2a = stringToLines(s2, wsIgnore);
		
		if(s1a.size() != s2a.size()) { return false; }
		
		for(int i=0;i<s1a.size();i++) {
			if(! s1a.get(i).equals(s2a.get(i))) { return false; }
		}
		return true;
	}

	static final Pattern PTRN_LEADING_WHITESPACE = Pattern.compile("^\\s+", Pattern.MULTILINE);
	static final Pattern PTRN_TRAILING_WHITESPACE = Pattern.compile("\\s+$", Pattern.MULTILINE);
	
	static List<String> stringToLines(String s, WhitespaceIgnoreType wsIgnore) {
		if(s==null) { s = ""; }
		if(wsIgnore.stripInside()) {
			s = TokenizerUtil.removeMultipleWhitespaces(s);
		}
		else {
			if(wsIgnore.stripSol()) {
				s = PTRN_LEADING_WHITESPACE.matcher(s).replaceAll("");
			}
			if(wsIgnore.stripEol()) {
				s = PTRN_TRAILING_WHITESPACE.matcher(s).replaceAll("");
			}
		}
		return Arrays.asList(s.split("\n"));
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
	
	public static String exceptionTrimmed(Throwable t) {
		if(t==null) return "";
		String str = t.toString();
		if(str==null) return "";
		return str.trim();
	}

}
