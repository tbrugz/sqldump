package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.Utils;

public class CSVDataDump extends DumpSyntax {
	
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.columndelimiter";
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.tablenameheader";
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.columnnamesheader";

	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ";";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	ResultSetMetaData md;
	
	boolean doTableNameHeaderDump;
	boolean doColumnNamesHeaderDump;
	String columnDelimiter;
	String recordDelimiter;		
	
	@Override
	public void procProperties(Properties prop) {
		recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		doTableNameHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_TABLENAMEHEADER);
		doColumnNamesHeaderDump = Utils.getPropBool(prop, PROP_DATADUMP_COLUMNNAMESHEADER);
	}

	@Override
	public void initDump(String tableName, ResultSetMetaData md) throws Exception {
		this.tableName = tableName;
		this.md = md;
		numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
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
				sb.append(md.getColumnName(i+1)+columnDelimiter);
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

	@Override
	public void dumpRow(ResultSet rs, int count, Writer fos) throws Exception {
		StringBuffer sb = new StringBuffer();
		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColTypes.size();i++) {
			sb.append( (i!=0?columnDelimiter:"")+ Utils.getFormattedCSVBrValue(vals.get(i)) );
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
	
}
