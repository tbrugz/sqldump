package tbrugz.sqldump.datadump;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;

public class DataDumpUtils {

	static Log log = LogFactory.getLog(DataDumpUtils.class);
	
	static final String DEFAULT_SQL_STRING_ENCLOSING = "'";
	static final String DOUBLEQUOTE = "\"";
	static final String EMPTY_STRING = "";

	static boolean resultSetWarnedForSQLValue = false;
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	public static DateFormat dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
	public static NumberFormat floatFormatterSQL = null;
	//public static NumberFormat floatFormatterBR = null;
	public static NumberFormat longFormatter = null;
	public static boolean csvWriteEnclosingAllFields = false; //TODO: add prop for csv_write_enclosing_all_fields
	
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
	
	//dumpers: CSV, FFC
	public static String getFormattedCSVValue(Object elem, Class type, NumberFormat floatFormatter, String separator, String lineSeparator, String enclosing, String nullValue) {
		if(elem == null) {
			return nullValue;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatter.format(elem);
		}
		else if(ResultSet.class.isAssignableFrom(type)) {
			return nullValue;
		}

		// String output:
		
		if(enclosing!=null) {
			if(csvWriteEnclosingAllFields) {
				return enclosing+String.valueOf(elem).replaceAll(enclosing, enclosing+enclosing)+enclosing;
			}
			else {
				//return String.valueOf(elem).replaceAll(enclosing, EMPTY_STRING); //XXX: replace by "'"?
				String val = String.valueOf(elem);
				if(val.contains(enclosing)) {
					return enclosing+val.replaceAll(enclosing, enclosing+enclosing)+enclosing;
				}
				else if(val.contains(separator) || val.contains(lineSeparator)) {
					return enclosing+val+enclosing;
				}
				else {
					return val;
				}
			}
		}
		if(separator==null) {
			//return String.valueOf(elem);
			if(lineSeparator==null) {
				return String.valueOf(elem);
			}
			else {
				return String.valueOf(elem).replaceAll(lineSeparator, EMPTY_STRING);
			}
		}
		else {
			if(lineSeparator==null) {
				return String.valueOf(elem).replaceAll(separator, EMPTY_STRING);
			}
			else {
				return String.valueOf(elem).replaceAll(separator, EMPTY_STRING).replaceAll(lineSeparator, EMPTY_STRING);
			}
		}
	} 

	//dumpers: JSON
	public static String getFormattedJSONValue(Object elem, Class type, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(String.class.isAssignableFrom(type)) {
			elem = ((String) elem).replaceAll(DOUBLEQUOTE, "&quot;");
			return DOUBLEQUOTE+elem+DOUBLEQUOTE;
		}
		else if(Date.class.isAssignableFrom(type)) {
			//XXXdone: JSON dateFormatter?
			return df.format((Date)elem);
		}
		else if(Long.class.isAssignableFrom(type)) {
			//log.warn("long: "+(Long)elem+"; "+longFormatter.format((Long)elem));
			return longFormatter.format((Long)elem);
		}

		return String.valueOf(elem);
	}

	//dumpers: insertinto, updatebypk
	public static String getFormattedSQLValue(Object elem, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			/* XXX?: String escaping? "\n, \r, ', ..."
			 * see: http://www.orafaq.com/wiki/SQL_FAQ#How_does_one_escape_special_characters_when_writing_SQL_queries.3F 
			 */
			elem = ((String) elem).replaceAll("'", "''");
			return DEFAULT_SQL_STRING_ENCLOSING+elem+DEFAULT_SQL_STRING_ENCLOSING;
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

	//dumpers: XML, HTML
	//XXX: XML format: translate '<', '>', '&'?
	public static String getFormattedXMLValue(Object elem, Class type, NumberFormat floatFormatter, String nullValue) {
		if(elem == null) {
			return nullValue;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatter.format(elem);
		}
		else if(ResultSet.class.isAssignableFrom(type)) {
			return nullValue;
		}

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

	public static void dumpRS(DumpSyntax ds, ResultSetMetaData rsmd, ResultSet rs, String tableName, Writer writer, boolean resetRS) throws Exception {
		//int ncol = rsmd.getColumnCount();
		ds.initDump(tableName, null, rsmd);
		ds.dumpHeader(writer);
		int count = 0;
		while(rs.next()) {
			ds.dumpRow(rs, count, writer);
			count++;
		}
		ds.dumpFooter(writer);
		if(resetRS) {
			try {
				rs.first();
			}
			catch (SQLException e) {
				rs.close();
			}
		}
	}
	
	static void logResultSetColumnsTypes(ResultSetMetaData md, String tableName) throws SQLException {
		int numCol = md.getColumnCount();		
		List<String> lsColNames = new ArrayList<String>();
		List<Class> lsColTypes = new ArrayList<Class>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<numCol;i++) {
			sb.append("\n\t"+lsColNames.get(i)+" ["+lsColTypes.get(i).getSimpleName()+"/t:"+md.getColumnType(i+1)+"/p:"+md.getPrecision(i+1)+"/s:"+md.getScale(i+1)+"]; ");
		}
		DataDump.log.debug("dump columns ["+tableName+"]: "+sb);
	}
	
}
