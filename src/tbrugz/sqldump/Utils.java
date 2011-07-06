package tbrugz.sqldump;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;

public class Utils {
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
				//TODO: String escaping? "\n, \r, ', ..."
				buffer.append(DEFAULT_ENCLOSING+elem+DEFAULT_ENCLOSING);
			}
			else if(elem instanceof Integer) {
				buffer.append(elem);
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
}
