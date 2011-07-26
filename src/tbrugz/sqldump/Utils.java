package tbrugz.sqldump;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class Utils {
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	public static DateFormat dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
	
	/*
	 * http://snippets.dzone.com/posts/show/91
	 * http://stackoverflow.com/questions/1515437/java-function-for-arrays-like-phps-join
	 */
	public static String join(Collection<?> s, String delimiter) {
		return join(s, delimiter, null, false);
	}
	
	public static String join(Collection<?> s, String delimiter, String enclosing, boolean enclosingJustForNonNulls) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			Object elem = iter.next();
			if(enclosing!=null && !"".equals(enclosing)) {
				if(enclosingJustForNonNulls && elem==null) {
					buffer.append(elem);
				}
				else {
					buffer.append(enclosing+elem+enclosing);
				}
			}
			else {
				buffer.append(elem);
			}

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	static String DEFAULT_ENCLOSING = "'";
	
	public static String join4sql(Collection<?> s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			Object elem = iter.next();
			if(elem == null) {
				buffer.append(elem);
			}
			else if(elem instanceof String) {
				/* XXX?: String escaping? "\n, \r, ', ..."
				 * see: http://www.orafaq.com/wiki/SQL_FAQ#How_does_one_escape_special_characters_when_writing_SQL_queries.3F 
				 */
				elem = ((String) elem).replaceAll("'", "''");
				buffer.append(DEFAULT_ENCLOSING+elem+DEFAULT_ENCLOSING);
			}
			else if(elem instanceof Integer) {
				buffer.append(elem);
			}
			else if(elem instanceof Date) {
				buffer.append(dateFormatter.format((Date)elem));
			}
			else {
				buffer.append(elem);
			}

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	public static String normalizeEnumStringConstant(String strEnumConstant) {
		return strEnumConstant.replace(' ', '_');
	}

	/*
	 * creates dir for file if it doesn't already exists
	 */
	public static void prepareDir(File f) {
		File parent = f.getParentFile();
		if(parent!=null) {
			if(!parent.exists()) {
				parent.mkdirs();
			}
		}
	}
	
	/*
	public static String denormalizeEnumStringConstant(String strEnumConstant) {
		return strEnumConstant.replace('_', ' ');
	}
	*/
	
	public static boolean getPropBool(Properties prop, String key) {
		String value = prop.getProperty(key);
		if(value==null) { return false; }
		return "true".equals(value.trim());
	}
	
	public static Long getPropLong(Properties prop, String key) {
		String str = prop.getProperty(key);
		try {
			long l = Long.parseLong(str);
			return l;
		}
		catch(Exception e) {
			return null;
		}
	}
	
}
