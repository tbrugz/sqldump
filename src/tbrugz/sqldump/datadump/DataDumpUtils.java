package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.Array;
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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DumpSyntax.PivotInfo;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.resultset.RSMetaDataTypedAdapter;
import tbrugz.sqldump.resultset.ResultSetArrayAdapter;
import tbrugz.sqldump.resultset.ResultSetProjectionDecorator;
import tbrugz.sqldump.resultset.pivot.PivotResultSet;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class DataDumpUtils {

	private static final Log log = LogFactory.getLog(DataDumpUtils.class);
	
	public static final String QUOTE = "'";
	public static final String DOUBLEQUOTE = "\"";
	public static final String EMPTY_STRING = "";
	public static final String NEWLINE = "\n";
	
	public static final String CHARSET_UTF8 = "UTF-8";
	public static final String CHARSET_ISO_8859_1 = "ISO-8859-1";

	static final String DEFAULT_SQL_STRING_ENCLOSING = QUOTE;
	
	static boolean resultSetWarnedForSQLValue = false;
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	static final NumberFormat floatFormatterSQL;
	//public static NumberFormat floatFormatterBR = null;
	static final NumberFormat longFormatter;
	static boolean csvWriteEnclosingAllFields = false; //TODO: add prop for csv_write_enclosing_all_fields
	
	public static class SyntaxCOutCallback implements CategorizedOut.Callback {
		final DumpSyntaxInt ds;
		public SyntaxCOutCallback(DumpSyntaxInt ds) {
			this.ds = ds;
		}
		@Override
		public void callOnOpen(Writer w) throws IOException {
			ds.dumpHeader(w);
		}
	}
	
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
	public static String getFormattedCSVValue(Object elem, Class<?> type, NumberFormat floatFormatter, DateFormat dateFormatter, String separator, String lineSeparator, String enclosing, String nullValue) {
		if(elem == null) {
			return nullValue;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatter.format(elem);
		}
		else if(Date.class.isAssignableFrom(type) && elem instanceof Date) {
			return dateFormatter.format((Date)elem);
		}
		else if(ResultSet.class.isAssignableFrom(type)) {
			return nullValue;
		}

		// String output:
		String val = getString(elem);
		
		if(enclosing!=null) {
			// XXX quoting strategy? see: https://docs.python.org/2/library/csv.html:: csv.QUOTE_ALL, csv.QUOTE_MINIMAL, csv.QUOTE_NONNUMERIC, csv.QUOTE_NONE
			// implement QUOTE_NONNUMERIC & QUOTE_NONE? (enclosing==null equals QUOTE_NONE) 
			if(csvWriteEnclosingAllFields) {
				return enclosing+val.replaceAll(enclosing, enclosing+enclosing)+enclosing;
			}
			else {
				//return String.valueOf(elem).replaceAll(enclosing, EMPTY_STRING); //XXX: replace by "'"?
				if(val.contains(enclosing)) {
					// doubling "enclosing"...
					return enclosing+val.replaceAll(enclosing, enclosing+enclosing)+enclosing;
				}
				else if(val.contains(separator) || val.contains(lineSeparator) || val.contains(NEWLINE)) {
					// XXX other special chars besides newline (\n) ?
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
				return val;
			}
			else {
				return val.replaceAll(lineSeparator, EMPTY_STRING);
			}
		}
		else {
			if(lineSeparator==null) {
				return val.replaceAll(separator, EMPTY_STRING);
			}
			else {
				return val.replaceAll(separator, EMPTY_STRING).replaceAll(lineSeparator, EMPTY_STRING);
			}
		}
	} 

	//dumpers: JSON
	public static String getFormattedJSONValue(Object elem, Class<?> type, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatterSQL.format(elem);
		}
		else if(String.class.isAssignableFrom(type)) {
			return getFormattedJSONString(elem);
		}
		else if(Date.class.isAssignableFrom(type)) {
			//XXXdone: JSON dateFormatter?
			return df.format((Date)elem);
		}
		else if(Number.class.isAssignableFrom(type)) {
			return longFormatter.format((Number)elem);
		}
		/*else if(Long.class.isAssignableFrom(type)) {
			//log.warn("long: "+(Long)elem+"; "+longFormatter.format((Long)elem));
			return longFormatter.format((Long)elem);
		}*/

		return getFormattedJSONString(elem);
	}
	
	public static String getFormattedJSONString(Object elem) {
		// see: http://stackoverflow.com/questions/19176024/how-to-escape-special-characters-in-building-a-json-string
		String val = getString(elem);
		val = val.replaceAll("\\\\", "\\\\\\\\");
		val = val.replaceAll(DOUBLEQUOTE, "\\\\\"");
		//val = val.replaceAll("\r\n", "\n");
		val = val.replaceAll("\b", "\\\\b");
		val = val.replaceAll("\f", "\\\\f");
		val = val.replaceAll("\r", "\\\\r");
		val = val.replaceAll("\n", "\\\\n");
		val = val.replaceAll("\t", "\\\\t");
		val = val.replaceAll( "/", "\\\\/");
		return DOUBLEQUOTE+val+DOUBLEQUOTE;
	}

	//dumpers: insertinto, updatebypk
	public static String getFormattedSQLValue(Object elem, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			return getFormattedSQLString(elem);
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
		else if(elem instanceof Integer || elem instanceof Long) {
			return getString(elem);
		}
		else if(elem instanceof ResultSet) {
			if(!resultSetWarnedForSQLValue) {
				log.warn("can't dump ResultSet as SQL type");
				resultSetWarnedForSQLValue = true;
			}
			return null;
		}
		else if(elem instanceof Object[]) {
			return null;
		}
		/*else if(elem instanceof Integer) {
			return String.valueOf(elem);
		}*/

		return getFormattedSQLString(elem);
	}
	
	public static String getFormattedSQLString(Object elem) {
		/* XXX?: String escaping? "\n, \r, ', ..."
		 * see: http://www.orafaq.com/wiki/SQL_FAQ#How_does_one_escape_special_characters_when_writing_SQL_queries.3F 
		 */
		elem = getString(elem).replaceAll("'", "''");
		return DEFAULT_SQL_STRING_ENCLOSING+elem+DEFAULT_SQL_STRING_ENCLOSING;
	}

	//dumpers: XML, HTML
	//XXXdone: XML format: translate '<', '>', '&'?
	public static String getFormattedXMLValue(Object elem, Class<?> type, NumberFormat floatFormatter, DateFormat df, String nullValue, boolean escape) {
		String value = getFormattedXMLValue(elem, type, floatFormatter, df, escape);
		if(value == null) {
			return nullValue;
		}

		return value;
	} 

	public static String getFormattedXMLValue(Object elem, Class<?> type, NumberFormat floatFormatter, DateFormat df, boolean escape) {
		if(elem == null) {
			return null;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatter.format(elem);
		}
		else if(Date.class.isAssignableFrom(type)) {
			return df.format((Date)elem);
		}
		else if(ResultSet.class.isAssignableFrom(type)) {
			return null;
		}

		return escape?xmlEscapeText(getString(elem)):getString(elem);
	}
	
	/*
	 * see: http://stackoverflow.com/a/10035382/616413
	 * http://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references#Predefined_entities_in_XML
	 */
	public static String xmlEscapeText(String t) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < t.length(); i++) {
			char c = t.charAt(i);
			switch (c) {
			case '<':
				sb.append("&lt;");
				break;
			case '>':
				sb.append("&gt;");
				break;
			/*case '\"':
				sb.append("&quot;");
				break;*/
			case '&':
				sb.append("&amp;");
				break;
			/*case '\'':
				sb.append("&apos;");
				break;*/
			default:
				/*if (c > 0x7e) {
					sb.append("&#" + ((int) c) + ";");
				} else {*/
					sb.append(c);
				/*}*/
			}
		}
		return sb.toString();
	}

	public static String xmlEscapeTextFull(String t) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < t.length(); i++) {
			char c = t.charAt(i);
			switch (c) {
			case '<':
				sb.append("&lt;");
				break;
			case '>':
				sb.append("&gt;");
				break;
			case '\"':
				sb.append("&quot;");
				break;
			case '&':
				sb.append("&amp;");
				break;
			case '\'':
				sb.append("&apos;");
				break;
			default:
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	/*public static String xmlEscapeAttributeValue(String t) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < t.length(); i++) {
			char c = t.charAt(i);
			switch (c) {
			case '\"':
				sb.append("&quot;");
				break;
			case '\'':
				sb.append("&apos;");
				break;
			default:
				sb.append(c);
			}
		}
		return sb.toString();
	}*/
	
	public static Collection<String> values4sql(Collection<?> s, DateFormat df) {
		Iterator<?> iter = s.iterator();
		List<String> ret = new ArrayList<String>();
		while (iter.hasNext()) {
			ret.add( getFormattedSQLValue(iter.next(), df) );
		}
		return ret;
	}

	public static String join4sql(Collection<?> s, DateFormat df, String delimiter) {
		StringBuilder buffer = new StringBuilder();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			Object obj = iter.next();
			/*if(obj!=null && ResultSet.class.isAssignableFrom(obj.getClass())) {
				obj = null;
			}*/
			buffer.append(getFormattedSQLValue(obj, df));

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}

	/*@Deprecated
	public static void dumpRS(DumpSyntaxInt ds, ResultSetMetaData rsmd, ResultSet rs, String tableName, Writer writer, boolean resetRS) throws IOException, SQLException {
		dumpRS(ds, rsmd, rs, null, tableName, writer, resetRS);
	}*/
	
	public static void dumpRS(DumpSyntaxInt ds, ResultSet rs, String schema, String tableName, Writer writer, boolean resetRS) throws IOException, SQLException {
		dumpRS(ds, rs.getMetaData(), rs, schema, tableName, writer, resetRS);
	}
	
	public static void dumpRS(DumpSyntaxInt ds, ResultSetMetaData rsmd, ResultSet rs, String schema, String tableName, Writer writer, boolean resetRS) throws IOException, SQLException {
		//int ncol = rsmd.getColumnCount();
		ds.initDump(schema, tableName, null, rsmd);
		ds.dumpHeader(writer);
		int count = 0;
		while(rs.next()) {
			ds.dumpRow(rs, count, writer);
			count++;
		}
		ds.dumpFooter(count, false, writer);
		if(resetRS) {
			try {
				rs.first();
			}
			catch (SQLException e) {
				rs.close();
			}
		}
	}
	
	//XXX: add columnTypeMapper?
	public static void logResultSetColumnsTypes(ResultSetMetaData md, String tableName, Log log) throws SQLException {
		if(log.isDebugEnabled()) {
			log.debug("dump columns ["+tableName+"]:\n\t"+Utils.join(getResultSetColumnsTypes(md), ";\n\t"));
		}
	}
	
	public static List<String> getResultSetColumnsTypes(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<String> lsColNames = new ArrayList<String>();
		List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnLabel(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		List<String> strs = new ArrayList<String>();
		for(int i=0;i<numCol;i++) {
			String colName = lsColNames.get(i);
			String colType = lsColTypes.get(i).getSimpleName();
			int type = md.getColumnType(i+1);
			String typename = null;
			try {
				typename = SQLUtils.getTypeName(type);
			} catch (Exception e) {
				log.warn("getTypeName: exception: "+e);
			}
			int precision = md.getPrecision(i+1);
			int scale = md.getScale(i+1);
			/*if( (type==Types.DECIMAL || type== Types.NUMERIC) && (precision==0 || precision<0 || scale<0)) {
				log.warn("numeric type with precision 0 or precision/scale<0 [table="+tableName+",col="+colName+",type="+type+",class="+colType+",precision="+precision+",scale="+scale+"]");
			}*/
			strs.add(colName+" ["+colType+"/t:"+type+"/tn:"+typename+"/p:"+precision+"/s:"+scale+"]");
		}
		return strs;
	}
	
	public static List<String> getColumnNames(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<String> names = new ArrayList<String>();
		for(int i=0;i<numCol;i++) {
			names.add(md.getColumnLabel(i+1));
		}
		return names;
	}

	public static List<Class<?>> getColumnTypes(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<Class<?>> types = new ArrayList<Class<?>>();
		for(int i=0;i<numCol;i++) {
			types.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		return types;
	}

	public static List<Column> getColumns(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<Column> columns = new ArrayList<Column>();
		for(int i=0;i<numCol;i++) {
			int colpos = i+1;
			Column c = new Column();
			c.setName(md.getColumnLabel(colpos));
			c.setType(md.getColumnTypeName(colpos));
			int precision = md.getPrecision(colpos);
			int scale = md.getScale(colpos);
			if(precision!=0) {
				c.setColumSize(precision);
			}
			if(scale!=0) {
				c.setDecimalDigits(scale);
			}
			c.setOrdinalPosition(colpos);
			columns.add(c);
		}
		return columns;
	}
	
	//see: http://tools.ietf.org/html/rfc2822 (NO-WS-CTL)
	//XXX: [\\x0b-\\x0c] needed? - vertical tab, form feed
	static final String REGEX_CONTROL_CHARS = "[\\x01-\\x08]|[\\x0b-\\x0c]|[\\x0e-\\x1f]|\\x7f";
	static final Pattern patternControlChars = Pattern.compile(REGEX_CONTROL_CHARS);
	static final String STR_CONTROL_CHARS_REPLACEMENT = "?";
	
	/*
	 * XXX: option to change replacement char? option to NOT replace control chars?
	 */
	public static String getPrintableString(Object s) {
		return patternControlChars.matcher(s.toString()).replaceAll(STR_CONTROL_CHARS_REPLACEMENT);
	}

	static String getString(Object s) {
		return (s==null)?"null":getPrintableString(s);
	}
	
	public static boolean isArray(Class<?> c, Object val) {
		return (Array.class.isAssignableFrom(c)) // || c.isArray()
			&&
			(val instanceof Object[] || (val instanceof Collection) );
			//(val instanceof Object[] || (val!=null && Collection.class.isAssignableFrom(val.getClass())) );
	}

	public static boolean isResultSet(Class<?> c, Object val) {
		//return ResultSet.class.isAssignableFrom(c) || (val instanceof ResultSet);
		return val instanceof ResultSet;
	}
	
	//see: Table.getColumnNames
	//public static List<String> getColumnNames(List<Column> columns) //?
	
	/*
	 * XXX move to tbrugz.sqldump.resultset.ResultSetUtils?
	 * XXX create ResultSetMetaDataDecorator?
	 */
	public static ResultSet projectResultSetByCols(ResultSet rs, List<String> colNamesToDump) throws SQLException {
		return new ResultSetProjectionDecorator(rs, colNamesToDump);
	}
	
	@Deprecated
	static ResultSetMetaData filterResultSetMetaData(ResultSetMetaData rsmd, List<String> colNamesToDump) throws SQLException {
		//Integer[] ctArr = new Integer[colNamesToDump.size()];
		int colCount = rsmd.getColumnCount();
		List<String> finalColNamesToDump = new ArrayList<String>();
		List<Integer> finalColTypesToDump = new ArrayList<Integer>();
		for(int i=1;i<=colCount;i++) {
			String colName = rsmd.getColumnName(i);
			int colType = rsmd.getColumnType(i);
			int idx = colNamesToDump.indexOf(colName);
			if(idx==-1) {
				log.debug("colName '"+colName+"' not found in "+colNamesToDump);
				String colLabel = rsmd.getColumnLabel(i);
				idx = colNamesToDump.indexOf(colLabel);
				if(idx==-1) {
					log.debug("colLabel '"+colLabel+"' not found in "+colNamesToDump);
					continue;
				}
				colName = colLabel;
			}
			finalColNamesToDump.add(colName);
			finalColTypesToDump.add(colType);
			//if(idx==-1) { continue; }
			//ctArr[idx] = colType;
		}
		//List<Integer> colTypes = Arrays.asList(ctArr);
		if(finalColNamesToDump.size() != colNamesToDump.size()) {
			log.info("filtering ResultSet by (updated) columns: "+finalColNamesToDump);
		}
		
		RSMetaDataTypedAdapter ret = new RSMetaDataTypedAdapter(rsmd.getSchemaName(1), rsmd.getTableName(1), finalColNamesToDump, finalColTypesToDump);
		//RSMetaDataTypedAdapter ret = new RSMetaDataTypedAdapter(rsmd.getSchemaName(1), rsmd.getTableName(1), colNamesToDump, colTypes);
		return ret;
	}
	
	public static DumpSyntaxInt buildDumpSyntax(final DumpSyntaxInt ds, String schemaName, String tableOrQueryName, List<String> keyColumns, ResultSetMetaData md) throws SQLException {
		if(ds instanceof DumpSyntaxBuilder) {
			//log.info("syntax '"+ds.getSyntaxId()+"' is a DumpSyntaxBuilder :)");
			DumpSyntaxInt ret = ((DumpSyntaxBuilder) ds).build(schemaName, tableOrQueryName, keyColumns, md);
			return ret;
		}
		log.warn("syntax '"+ds.getSyntaxId()+"' isn't a DumpSyntaxBuilder");
		ds.initDump(schemaName, tableOrQueryName, keyColumns, md);
		return ds;
	}
	
	@SuppressWarnings("rawtypes")
	public static ResultSet getResultSetFromArray(Object obj, boolean withIndexColumn, String columnName) {
		if(obj==null) { return null; }
		
		Object[] arr = null;
		if(obj instanceof Collection) {
			arr = ((Collection) obj).toArray();
		}
		else if(obj instanceof Object[]) {
			arr = (Object[]) obj;
		}
		else {
			throw new IllegalArgumentException("object '"+obj+"' is not array nor collection ["+obj.getClass().getName()+"]");
		}
		return new ResultSetArrayAdapter(arr, withIndexColumn, columnName);
	}
	
	public static boolean isNumericType(Class<?> clazz) {
		if(clazz.equals(Integer.class) || clazz.equals(Double.class)) {
			return true;
		}
		return false; 
	}
	
	public static PivotInfo guessPivotCols(List<String> colNames) {
		String colSepPattern = Pattern.quote(PivotResultSet.COLS_SEP);
		String colValSepPattern = Pattern.quote(PivotResultSet.COLVAL_SEP);
		return guessPivotCols(colNames, colSepPattern, colValSepPattern);
	}
	
	public static PivotInfo guessPivotCols(List<String> colNames, String colSepPattern, String colValSepPattern) {
		int onColsColCount = 0;
		int onRowsColCount = 0;
		for(int i=0;i<colNames.size();i++) {
			int l = colNames.get(i).split(colSepPattern).length;
			if(l>1) {
				if(l>onColsColCount) {
					onColsColCount = l;
					onRowsColCount = i;
					break;
				}
			}
		}
		
		if(onColsColCount==0 && onRowsColCount==0) {
			for(int i=0;i<colNames.size();i++) {
				int l2 = colNames.get(i).split(colValSepPattern).length;
				if(l2>1) {
					onColsColCount = 1;
					onRowsColCount = i;
					break;
				}
			}
		}
		
		/*if(onColsColCount==0 && onRowsColCount==0) {
			// when onColsColCount==0, "guess" is not effective
			return null;
		}*/
		return new PivotInfo(onColsColCount, onRowsColCount);
	}
	
	public static class PivotHeaderRow {
		String colname = null;
		boolean measuresRow = false;
		List<PivotHeaderCol> rows = new ArrayList<>();
	}
	
	public static class PivotHeaderCol {
		String collabel = null;
		boolean blank = false;
		boolean dimoncol = false;
		boolean measure = false;
		boolean isNull = false;
	}
	
	protected static List<PivotHeaderRow> getPivotedTableHeaderRows(PivotInfo pivotInfo, List<String> colNames) {
		String colSepPattern = Pattern.quote(PivotResultSet.COLS_SEP);
		String colValSepPattern = Pattern.quote(PivotResultSet.COLVAL_SEP);
		return getPivotedTableHeaderRows(pivotInfo, colNames, colSepPattern, colValSepPattern, PivotResultSet.NULL_PLACEHOLDER);
	}
	
	protected static List<PivotHeaderRow> getPivotedTableHeaderRows(PivotInfo pivotInfo, List<String> colNames, String colSepPattern, String colValSepPattern, String nullPlaceholder) {
		//String nullPlaceholderReplacer) {
		//StringBuilder sb = new StringBuilder();
		List<PivotHeaderRow> ret = new ArrayList<>();
		
		//System.out.println("[1:beforeguess] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
		//boolean dumpedAsLeast1row = false;
		if(pivotInfo.isPivotResultSet()) {
			//DataDumpUtils.guessPivotCols(colNames, colSepPattern, colValSepPattern); //guess cols/rows, since measures may be present or not...
			//System.out.println("[2:afterguess ] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
			for(int cc=0;cc<pivotInfo.onColsColCount;cc++) {
				PivotHeaderRow prow = new PivotHeaderRow();
				//StringBuilder sbrow = new StringBuilder();
				//String colname = null;
				//String collabel = null;
				//boolean measuresRow = false;
				//boolean blank = false;
				//boolean dimoncol = false;
				//boolean measure = false;
				//boolean isNull = false;
				for(int i=0;i<colNames.size();i++) {
					PivotHeaderCol phc = new PivotHeaderCol();
					String[] parts = colNames.get(i).split(colSepPattern);
					
					if(parts.length>cc) {
						if(i<pivotInfo.onRowsColCount) {
							/*sbrow.append("<th class=\"blank\""+
									(i<pivotInfo.onRowsColCount?" dimoncol=\"true\"":"")+
									"/>");*/
							prow.measuresRow = true;
							phc.blank = true;
							phc.dimoncol = true;
							//colname = DataDumpUtils.xmlEscapeText(parts[cc]);
						}
						else {
							//split...
							String[] p2 = parts[cc].split(colValSepPattern);
							if(p2.length>1) {
								String thValue = p2[1];
								//String nullAttrib = "";
								if(nullPlaceholder.equals(thValue)) {
									//thValue = nullPlaceholderReplacer;
									//nullAttrib = " null=\"true\"";
									phc.isNull = true;
								}
								else {
									phc.collabel = thValue;
								}
								//sbrow.append("<th"+nullAttrib+">"+thValue+"</th>");
								prow.colname = DataDumpUtils.xmlEscapeText(p2[0]);
							}
							else {
								//sbrow.append("<th measure=\"true\">"+parts[cc]+"</th>");
								prow.measuresRow = true;
								phc.measure = true;
								phc.collabel = parts[cc];
							}
						}
					}
					else if(cc+1==pivotInfo.onColsColCount) {
						if(i<pivotInfo.onRowsColCount) {
							//sbrow.append("<th dimoncol=\"true\" measure=\"true\">"+colNames.get(i)+"</th>");
							prow.measuresRow = true;
							phc.dimoncol = true;
							phc.measure = true;
						}
						//else {
						//	sbrow.append("<th>"+colNames.get(i)+"</th>");
						//}
						phc.collabel = colNames.get(i);
					}
					else {
						if(i<pivotInfo.onRowsColCount) {
							phc.dimoncol = true;
						}
						//sbrow.append("<th class=\"blank\""+
						//		(i<pivotInfo.onRowsColCount?" dimoncol=\"true\"":"")+
						//		"/>");
						phc.blank = true;
					}
					prow.rows.add(phc);
					//ret.get(cc).add(phr);
				}
				ret.add(prow);
				//sb.append("\t<tr"+(colname!=null?" colname=\""+colname+"\"":"")+(measuresRow?" measuresrow=\"true\"":"")+">");
				//sb.append(sbrow);
				//sb.append("</tr>");
				//dumpedAsLeast1row = true;
			}
		}
		
		return ret;
		/*
		boolean dumpHeaderRow = !dumpedAsLeast1row &&
				(!innerTable || innerArrayDumpHeader || finalColNames.size()!=1) &&
				!breakColsAddColumnHeaderBefore && !breakColsAddColumnHeaderAfter;
		//log.info("dumpHeaderRow=="+dumpHeaderRow+" ;; dumpedAsLeast1row="+dumpedAsLeast1row+" ; innerTable="+innerTable+" ; innerArrayDumpHeader="+innerArrayDumpHeader+" ; finalColNames.size()="+finalColNames.size());
		if(dumpHeaderRow) {
			appendTableHeaderRow(sb);
		}
		*/
		//sb.append("\n");
	}
	
}
