package tbrugz.sqldump;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Utils {
	
	static Logger log = Logger.getLogger(Utils.class);
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	public static DateFormat dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
	public static NumberFormat floatFormatterSQL = null;
	public static NumberFormat floatFormatterBR = null;
	public static NumberFormat longFormatter = null;
	
	static {
		floatFormatterSQL = NumberFormat.getNumberInstance(Locale.ENGLISH); //new DecimalFormat("##0.00#");
		DecimalFormat df = (DecimalFormat) floatFormatterSQL;
		df.setGroupingUsed(false);
		df.applyPattern("###0.00#");
	}

	static {
		floatFormatterBR = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) floatFormatterBR;
		df.setGroupingUsed(false);
		df.applyPattern("###0.000");
	}

	static {
		longFormatter = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) longFormatter;
		df.setGroupingUsed(false);
		df.setMaximumIntegerDigits(20); //E??
		df.applyPattern("###0");//87612933000118
	}
	
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
	static String DOUBLEQUOTE = "\"";
	
	public static String join4sql(Collection<?> s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(getFormattedSQLValue(iter.next()));

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	public static String getFormattedSQLValue(Object elem) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			/* XXX?: String escaping? "\n, \r, ', ..."
			 * see: http://www.orafaq.com/wiki/SQL_FAQ#How_does_one_escape_special_characters_when_writing_SQL_queries.3F 
			 */
			elem = ((String) elem).replaceAll("'", "''");
			return DEFAULT_ENCLOSING+elem+DEFAULT_ENCLOSING;
		}
		else if(elem instanceof Date) {
			return dateFormatter.format((Date)elem);
		}
		else if(elem instanceof Float) {
			return floatFormatterSQL.format((Float)elem);
		}
		else if(elem instanceof Double) {
			//log.debug("format:: "+elem+" / "+floatFormatterSQL.format((Double)elem));
			return floatFormatterSQL.format((Double)elem);
		}
		/*else if(elem instanceof Integer) {
			return String.valueOf(elem);
		}*/

		return String.valueOf(elem);
	} 
	
	public static String getFormattedJSONValue(Object elem) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			elem = ((String) elem).replaceAll(DOUBLEQUOTE, "&quot;");
			return DOUBLEQUOTE+elem+DOUBLEQUOTE;
		}
		else if(elem instanceof Date) {
			//XXX: JSON dateFormatter?
			return dateFormatter.format((Date)elem);
		}
		else if(elem instanceof Long) {
			//log.warn("long: "+(Long)elem+"; "+longFormatter.format((Long)elem));
			return longFormatter.format((Long)elem);
		}

		return String.valueOf(elem);
	} 

	public static String getFormattedCSVBrValue(Object elem) {
		if(elem == null) {
			return "";
		}
		else if(elem instanceof Double) {
			return floatFormatterBR.format((Double)elem);
		}

		return String.valueOf(elem);
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
	
	public static String readPassword(String message) {
		Console cons = System.console();
		//log.info("console: "+cons);
		char[] passwd;
		if (cons != null) {
			System.out.print(message);
			passwd = cons.readPassword(); //"[%s]", message
			return new String(passwd);
			//java.util.Arrays.fill(passwd, ' ');
		}
		else {
			//XXX: System.console() doesn't work in Eclipse - https://bugs.eclipse.org/bugs/show_bug.cgi?id=122429
			return Utils.readPasswordIntern(message, "");
		}
	}
	
	static String readPasswordIntern(String message, String replacer) {
		System.out.print(message);
		StringBuffer sb = new StringBuffer();
		InputStream is = System.in;
		try {
			int read = 0;
			while((read = is.read()) != -1) {
				if(read==13) break;
				char c = (char) (read & 0xff);
				sb.append(c);
				System.out.print(replacer);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public static Object getClassInstance(String className) {
		try {
			Class<?> c = Class.forName(className);
			return c.newInstance();
		} catch (ClassNotFoundException e) {
			log.debug("class not found: "+e.getMessage(), e);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) {
		String s = readPasswordIntern("pass: ", "*");
		System.out.println("s = "+s);
	}
	
}
