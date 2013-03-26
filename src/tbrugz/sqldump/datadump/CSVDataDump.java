package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.Utils;

/**
 * CSV RFC: http://tools.ietf.org/html/rfc4180 / http://tools.ietf.org/pdf/rfc4180.pdf
 */
/*
   TODO: CSV Syntax: comply with RFC 4180
   6.  Fields containing line breaks (CRLF), double quotes, and commas
       should be enclosed in double-quotes.  For example:

       "aaa","b CRLF
       bb","ccc" CRLF
       zzz,yyy,xxx

   7.  If double-quotes are used to enclose fields, then a double-quote
       appearing inside a field must be escaped by preceding it with
       another double quote.  For example:

       "aaa","b""bb","ccc"
 */
public class CSVDataDump extends DumpSyntax {
	
	static Log log = LogFactory.getLog(CSVDataDump.class);
	
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.csv.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.csv.columndelimiter";
	static final String PROP_DATADUMP_ENCLOSING = "sqldump.datadump.csv.enclosing";
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.csv.tablenameheader";
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.csv.columnnamesheader";
	//static final String PROP_DATADUMP_CSV_FLOATLOCALE = "sqldump.datadump.csv.floatlocale";

	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ",";
	static final String ENCLOSING_DEFAULT = "\""; //XXXxx: should be '"'? yes!

	static final String CSV_SYNTAX_ID = "csv";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	ResultSetMetaData md;
	
	boolean doTableNameHeaderDump = false;
	boolean doColumnNamesHeaderDump = true;
	String columnDelimiter;
	String recordDelimiter;
	String enclosing;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		enclosing = prop.getProperty(PROP_DATADUMP_ENCLOSING, ENCLOSING_DEFAULT);
		doTableNameHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_TABLENAMEHEADER, doTableNameHeaderDump);
		doColumnNamesHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_COLUMNNAMESHEADER, doColumnNamesHeaderDump);
	}
	
	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		this.md = md;
		numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}

		//doTableNameHeaderDump = (Boolean) os[5];
		//doColumnNamesHeaderDump = (Boolean) os[6];
		//columnDelimiter = (String) os[7];
		//recordDelimiter = (String) os[8];
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		//headers
		if(doTableNameHeaderDump) {
			out("[table "+tableName+"]", fos, recordDelimiter);
		}
		if(doColumnNamesHeaderDump) {
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<numCol;i++) {
				sb.append( (i!=0?columnDelimiter:"") + lsColNames.get(i));
			}
			out(sb.toString(), fos, recordDelimiter);
		}
	}
	/*
	void dumpRowsCSVSyntax(ResultSet rs, String tableName, int numCol, String columnDelimiter, String recordDelimiter, long rowlimit, Writer fos) throws Exception {
		//lines
		int count = 0;
		do {
			out(SQLUtils.getRowFromRS(rs, numCol, tableName, columnDelimiter), fos, recordDelimiter);
			count++;
			if(rowlimit<=count) { break; }
		}
		while(rs.next());
		log.info("dumped "+count+" rows from table: "+tableName);
	}*/

	//CSV
	void oldDumpRow(ResultSet rs, String tableName, int numCol, String columnDelimiter, String recordDelimiter, Writer fos) throws Exception {
		out(SQLUtils.getRowFromRS(rs, numCol, tableName, columnDelimiter), fos, recordDelimiter);
	}

	static boolean resultSetWarned = false;
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		StringBuffer sb = new StringBuffer();
		List<?> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColTypes.size();i++) {
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				
				/*out(sb.toString(), fos, recordDelimiter);
				sb = new StringBuffer();
				ResultSet rsInt = (ResultSet) vals.get(i);
				CSVDataDump csvdd = new CSVDataDump();
				csvdd.columnDelimiter = this.columnDelimiter;
				csvdd.recordDelimiter = this.recordDelimiter; //change this?
				SQLUtils.dumpRS(csvdd, rsInt.getMetaData(), rsInt, fos);*/
				
				if(!resultSetWarned) {
					log.warn("can't dump ResultSet as column");
					resultSetWarned = true;
				}
				sb.append( (i!=0?columnDelimiter:"") + nullValueStr);
			}
			else {
				sb.append( (i!=0?columnDelimiter:"") + DataDumpUtils.getFormattedCSVValue(vals.get(i), lsColTypes.get(i), floatFormatter, columnDelimiter, recordDelimiter, enclosing, nullValueStr) );
			}
		}
		out(sb.toString(), fos, recordDelimiter);
	}

	@Override
	public void dumpFooter(Writer fos) {
		//do nothing
	}
	
	void out(String s, Writer pw, String recordDelimiter) throws IOException {
		pw.write(s+recordDelimiter);
	}

	@Override
	public String getSyntaxId() {
		return CSV_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return "text/csv";
	}
	
}
