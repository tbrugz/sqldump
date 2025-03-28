package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/**
 * CSV RFC: http://tools.ietf.org/html/rfc4180 / http://tools.ietf.org/pdf/rfc4180.pdf
 * Frictionless spec: https://specs.frictionlessdata.io/csv-dialect/
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
   
   semantic CSV?
   http://www.w3.org/2013/05/lcsv-charter - CSV on the Web
   http://tiree.snipit.org/talis/tables/
   https://github.com/clarkparsia/csv2rdf
   http://w3c.github.io/csvw/csv2rdf/ - http://www.w3.org/TR/csv2rdf/
   http://www.iana.org/assignments/media-types/text/csv-schema
   
   sep=<x>? (for excel)
   https://stackoverflow.com/questions/20395699/sep-statement-breaks-utf8-bom-in-csv-file-which-is-generated-by-xsl
 */
public class CSVDataDump extends AbstractDumpSyntax implements Cloneable, DumpSyntaxBuilder {
	
	static final Log log = LogFactory.getLog(CSVDataDump.class);
	
	//static final String PREFIX = "sqldump.datadump.";

	/*
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.csv.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.csv.columndelimiter";
	static final String PROP_DATADUMP_ENCLOSING = "sqldump.datadump.csv.enclosing";
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.csv.tablenameheader";
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.csv.columnnamesheader";
	//static final String PROP_DATADUMP_CSV_FLOATLOCALE = "sqldump.datadump.csv.floatlocale";

	//static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.csv.charset";
	//static final String PROP_DATADUMP_WRITEBOM = "sqldump.datadump.csv."+DataDump.SUFFIX_DATADUMP_WRITEBOM;
	static final String PROP_DATADUMP_WRITEBOM_UTF8 = "sqldump.datadump.csv.x-writebom-utf8";
	*/

	static final String SUFFIX_RECORDDELIMITER = "recorddelimiter";
	static final String SUFFIX_COLUMNDELIMITER = "columndelimiter";
	static final String SUFFIX_ENCLOSING = "enclosing";
	static final String SUFFIX_TABLENAMEHEADER = "tablenameheader";
	static final String SUFFIX_COLUMNNAMESHEADER = "columnnamesheader";
	static final String SUFFIX_WRITEBOM_UTF8 = "x-writebom-utf8";
	
	public static final String DELIM_RECORD_DEFAULT = "\r\n"; // RFC: record delimiter is \r\n
	public static final String DELIM_COLUMN_DEFAULT = ",";
	static final String ENCLOSING_DEFAULT = "\""; //XXXxx: should be '"'? yes!
	static final boolean DEFAULT_COLUMNNAMESHEADER = true;
	
	static final String CSV_SYNTAX_ID = "csv";
	
	//ResultSetMetaData md;
	
	boolean doTableNameHeaderDump = false;
	boolean doColumnNamesHeaderDump = DEFAULT_COLUMNNAMESHEADER;
	String columnDelimiter;
	String recordDelimiter;
	String enclosing;
	boolean writeUft8Bom = false;
	boolean dumpSimpleColumnNames = false;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		recordDelimiter = prop.getProperty(fullPrefix() + SUFFIX_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		columnDelimiter = prop.getProperty(fullPrefix() + SUFFIX_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		enclosing = prop.getProperty(fullPrefix() + SUFFIX_ENCLOSING, ENCLOSING_DEFAULT);
		doTableNameHeaderDump = Utils.getPropBool(prop, fullPrefix() + SUFFIX_TABLENAMEHEADER, doTableNameHeaderDump);
		doColumnNamesHeaderDump = Utils.getPropBool(prop, fullPrefix() + SUFFIX_COLUMNNAMESHEADER, DEFAULT_COLUMNNAMESHEADER);
		writeUft8Bom = Utils.getPropBool(prop, fullPrefix() + SUFFIX_WRITEBOM_UTF8, writeUft8Bom);
		dumpSimpleColumnNames = Utils.getPropBool(prop, fullPrefix() + SUFFIX_SIMPLE_COLUMNNAME, dumpSimpleColumnNames);
		postProcProperties();
	}

	/*
	String fullPrefix() {
		return PREFIX + getSyntaxId() + ".";
	}
	*/
	
	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		//this.md = md;

		//doTableNameHeaderDump = (Boolean) os[5];
		//doColumnNamesHeaderDump = (Boolean) os[6];
		//columnDelimiter = (String) os[7];
		//recordDelimiter = (String) os[8];
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		//headers
		if(writeUft8Bom) {
			fos.write(DataDump.UTF8_BOM);
		}
		if(doTableNameHeaderDump) {
			out("[table "+tableName+"]", fos, recordDelimiter);
		}
		if(doColumnNamesHeaderDump) {
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<numCol;i++) {
				sb.append( (i!=0?columnDelimiter:"") + normalizeColumnName(lsColNames.get(i)) );
			}
			out(sb.toString(), fos, recordDelimiter);
		}
	}
	/*
	void dumpRowsCSVSyntax(ResultSet rs, String tableName, int numCol, String columnDelimiter, String recordDelimiter, long rowlimit, Writer fos) throws IOException, SQLException {
		//lines
		int count = 0;
		do {
			out(SQLUtils.getRowFromRS(rs, numCol, tableName, columnDelimiter), fos, recordDelimiter);
			count++;
			if(rowlimit<=count) { break; }
		}
		while(rs.next());
		log.info("dumped "+count+" rows from table: "+tableName);
	} */

	//CSV
	void oldDumpRow(ResultSet rs, String tableName, int numCol, String columnDelimiter, String recordDelimiter, Writer fos) throws IOException, SQLException {
		out(SQLUtils.getRowFromRS(rs, numCol, tableName, columnDelimiter), fos, recordDelimiter);
	}

	static boolean resultSetWarned = false;
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		List<?> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		dumpValues(vals, count, fos);
	}
	
	void dumpValues(List<?> vals, long count, Writer fos) throws IOException {
		StringBuilder sb = new StringBuilder();
		//log.info("lsColTypes:: "+lsColTypes.size()+" / "+lsColTypes+" vals: "+vals.size()); 
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
				sb.append( (i!=0?columnDelimiter:"") + DataDumpUtils.getFormattedCSVValue(vals.get(i), lsColTypes.get(i), floatFormatter, dateFormatter, columnDelimiter, recordDelimiter, enclosing, nullValueStr) );
			}
		}
		out(sb.toString(), fos, recordDelimiter);
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
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
	
	@Override
	public void updateProperties(DumpSyntax ds) {
		CSVDataDump dd = (CSVDataDump) ds;
		super.updateProperties(dd);
		
		dd.columnDelimiter = this.columnDelimiter;
		dd.doTableNameHeaderDump = this.doTableNameHeaderDump;
		dd.doColumnNamesHeaderDump = this.doColumnNamesHeaderDump;
		dd.enclosing = this.enclosing;
		dd.recordDelimiter = this.recordDelimiter;
	}

	@Override
	public CSVDataDump clone() throws CloneNotSupportedException {
		CSVDataDump dd = (CSVDataDump) super.clone();
		updateProperties(dd);
		return dd;
	}

	public String normalizeColumnName(String s) {
		if(dumpSimpleColumnNames) {
			List<String> values = DataDumpUtils.guessPivotColValues(s);
			return Utils.join(values, "/");
		}
		return DataDumpUtils.getFormattedCsvString( s, columnDelimiter, recordDelimiter, enclosing, nullValueStr );
	}

}
