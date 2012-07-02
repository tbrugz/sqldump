package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * CSV RFC: http://tools.ietf.org/html/rfc4180 / http://tools.ietf.org/pdf/rfc4180.pdf
 */
public class CSVDataDump extends DumpSyntax {
	
	static Log log = LogFactory.getLog(CSVDataDump.class);
	
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.csv.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.csv.columndelimiter";
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.csv.tablenameheader";
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.csv.columnnamesheader";
	//static final String PROP_DATADUMP_CSV_FLOATLOCALE = "sqldump.datadump.csv.floatlocale";

	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ",";

	static final String CSV_SYNTAX_ID = "csv";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	ResultSetMetaData md;
	
	boolean doTableNameHeaderDump = false;
	boolean doColumnNamesHeaderDump = true;
	String columnDelimiter;
	String recordDelimiter;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		doTableNameHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_TABLENAMEHEADER, doTableNameHeaderDump);
		doColumnNamesHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_COLUMNNAMESHEADER, doColumnNamesHeaderDump);
	}
	
	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws Exception {
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
	public void dumpHeader(Writer fos) throws Exception {
		//headers
		if(doTableNameHeaderDump) {
			out("[table "+tableName+"]", fos, recordDelimiter);
		}
		if(doColumnNamesHeaderDump) {
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<numCol;i++) {
				sb.append( (i!=0?columnDelimiter:"") + md.getColumnName(i+1));
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
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		StringBuffer sb = new StringBuffer();
		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
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
				sb.append( (i!=0?columnDelimiter:"") + DataDumpUtils.getFormattedCSVValue(vals.get(i), lsColTypes.get(i), floatFormatter, columnDelimiter, nullValueStr) );
			}
		}
		out(sb.toString(), fos, recordDelimiter);
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		//do nothing
	}
	
	void out(String s, Writer pw, String recordDelimiter) throws IOException {
		pw.write(s+recordDelimiter);
	}

	@Override
	public String getSyntaxId() {
		return CSV_SYNTAX_ID;
	}
	
}
