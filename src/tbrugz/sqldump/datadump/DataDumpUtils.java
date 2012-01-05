package tbrugz.sqldump.datadump;

import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;

public class DataDumpUtils {

	static Logger log = Logger.getLogger(DataDumpUtils.class);
	
	static String DEFAULT_ENCLOSING = "'";
	static String DOUBLEQUOTE = "\"";

	static boolean resultSetWarnedForSQLValue = false;
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	public static DateFormat dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
	public static NumberFormat floatFormatterSQL = null;
	//public static NumberFormat floatFormatterBR = null;
	public static NumberFormat longFormatter = null;
	
	static {
		floatFormatterSQL = NumberFormat.getNumberInstance(Locale.ENGLISH); //new DecimalFormat("##0.00#");
		DecimalFormat df = (DecimalFormat) floatFormatterSQL;
		df.setGroupingUsed(false);
		df.applyPattern("###0.00#");
	}

	/*static {
		floatFormatterBR = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) floatFormatterBR;
		df.setGroupingUsed(false);
		df.applyPattern("###0.000");
	}*/

	static {
		longFormatter = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) longFormatter;
		df.setGroupingUsed(false);
		df.setMaximumIntegerDigits(20); //E??
		df.applyPattern("###0");//87612933000118
	}
	
	public static String getFormattedCSVValue(Object elem, NumberFormat floatFormatter, String separator, String nullValue) {
		if(elem == null) {
			return nullValue;
		}
		else if(elem instanceof Double) {
			return floatFormatter.format((Double)elem);
		}
		else if(elem instanceof ResultSet) {
			return nullValue;
		}

		// String output:
		if(separator==null) {
			return String.valueOf(elem);
		}
		else {
			return String.valueOf(elem).replaceAll(separator, "");
		}
	} 

	public static String getFormattedJSONValue(Object elem, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			elem = ((String) elem).replaceAll(DOUBLEQUOTE, "&quot;");
			return DOUBLEQUOTE+elem+DOUBLEQUOTE;
		}
		else if(elem instanceof Date) {
			//XXXdone: JSON dateFormatter?
			return df.format((Date)elem);
		}
		else if(elem instanceof Long) {
			//log.warn("long: "+(Long)elem+"; "+longFormatter.format((Long)elem));
			return longFormatter.format((Long)elem);
		}

		return String.valueOf(elem);
	}

	public static String getFormattedSQLValue(Object elem, DateFormat df) {
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
			return df.format((Date)elem);
		}
		else if(elem instanceof Float) {
			return floatFormatterSQL.format((Float)elem);
		}
		else if(elem instanceof Double) {
			//log.debug("format:: "+elem+" / "+floatFormatterSQL.format((Double)elem));
			return floatFormatterSQL.format((Double)elem);
		}
		else if(elem instanceof ResultSet) {
			if(!resultSetWarnedForSQLValue) {
				log.warn("can't dump ResultSet as SQL type");
				resultSetWarnedForSQLValue = true;
			}
			return null;
		}
		/*else if(elem instanceof Integer) {
			return String.valueOf(elem);
		}*/

		return String.valueOf(elem);
	} 
	
	public static Collection<String> values4sql(Collection<?> s, DateFormat df) {
		Iterator<?> iter = s.iterator();
		List<String> ret = new ArrayList<String>();
		while (iter.hasNext()) {
			ret.add( getFormattedSQLValue(iter.next(), df) );
		}
		return ret;
	}

	public static String join4sql(Collection<?> s, DateFormat df, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(getFormattedSQLValue(iter.next(), df));

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
}
