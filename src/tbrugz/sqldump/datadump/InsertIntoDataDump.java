package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class InsertIntoDataDump extends DumpSyntax {

	static final String INSERTINTO_SYNTAX_ID = "insertinto";
	static final String PROP_DATADUMP_INSERTINTO_WITHCOLNAMES = "sqldump.datadump.insertinto.withcolumnnames";
	static final String PROP_INSERTINTO_DUMPCURSORS = "sqldump.datadump.insertinto.dumpcursors";
	//XXX: option/prop to include or not columns that are cursor expressions (ResultSets) as null
	static final String PROP_INSERTINTO_HEADER = "sqldump.datadump.insertinto.header";
	static final String PROP_INSERTINTO_FOOTER = "sqldump.datadump.insertinto.footer";
	static final String PROP_INSERTINTO_COMPACT = "sqldump.datadump.insertinto.compactmode";
	
	static final String COMPACTMODE_IDENT = "  ";

	protected String tableName;
	protected int numCol;
	String colNames;
	protected final List<String> lsColNames = new ArrayList<String>();
	protected final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	protected List<String> pkCols = null;
	
	boolean doColumnNamesDump = true;
	boolean doDumpCursors = false;
	boolean dumpCompactMode = false;
	String header;
	String footer;
	
	Properties prop;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		doColumnNamesDump = Utils.getPropBool(prop, PROP_DATADUMP_INSERTINTO_WITHCOLNAMES, doColumnNamesDump);
		doDumpCursors = Utils.getPropBool(prop, PROP_INSERTINTO_DUMPCURSORS, doDumpCursors);
		dumpCompactMode = Utils.getPropBool(prop, PROP_INSERTINTO_COMPACT, dumpCompactMode);
		//XXX replace [schemaname], [tablename] in header & footer
		header = prop.getProperty(PROP_INSERTINTO_HEADER);
		footer = prop.getProperty(PROP_INSERTINTO_FOOTER);
		this.prop = prop;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "sql";
	}

	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		this.pkCols = pkCols;
		numCol = md.getColumnCount();
		lsColTypes.clear();
		lsColNames.clear();
		//List<String> lsColNames = new ArrayList<String>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		colNames = "("+Utils.join(lsColNames, ", ")+")";
	}

	//XXX: option to dump ResultSet columns
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, doDumpCursors);
		String valsStr = DataDumpUtils.join4sql(vals, dateFormatter, ", ");
		if(dumpCompactMode) {
			out((count>0?COMPACTMODE_IDENT+",":COMPACTMODE_IDENT+" ")+
				"("+
				valsStr+
				")", fos);
		}
		else {
			out("insert into "+tableName+
				(doColumnNamesDump?" "+colNames:"")+
				" values ("+
				valsStr+
				");", fos);
		}
		
		if(doDumpCursors) {
			for(int i=0;i<lsColNames.size();i++) {
				if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
					ResultSet rsInt = (ResultSet) vals.get(i);
					if(rsInt==null) { continue; }
					InsertIntoDataDump iidd = new InsertIntoDataDump();
					iidd.procProperties(prop);
					DataDumpUtils.dumpRS(iidd, rsInt.getMetaData(), rsInt, lsColNames.get(i), fos, true);
				}
			}
		}
	}
	
	protected void out(String s, Writer pw) throws IOException {
		pw.write(s+"\n");
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		if(header!=null) {
			out(header, fos);
		}
		if(dumpCompactMode) {
			out("insert into "+tableName+
				(doColumnNamesDump?" "+colNames:"")+
				" values", fos);
		}
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		if(dumpCompactMode) {
			out(COMPACTMODE_IDENT+";", fos);
		}
		if(footer!=null) {
			out(footer, fos);
		}
		//always add (empty) footer?
		/*
		else {
			out("", fos);
		}
		*/
	}

	@Override
	public String getSyntaxId() {
		return INSERTINTO_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return "text/plain";
	}
}
