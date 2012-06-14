package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class InsertIntoDataDump extends DumpSyntax {

	static final String INSERTINTO_SYNTAX_ID = "insertinto";
	static final String PROP_DATADUMP_INSERTINTO_WITHCOLNAMES = "sqldump.datadump.insertinto.withcolumnnames";

	String tableName;
	int numCol;
	String colNames;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	boolean doColumnNamesDump = true;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		doColumnNamesDump = Utils.getPropBool(prop, PROP_DATADUMP_INSERTINTO_WITHCOLNAMES, doColumnNamesDump);
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "sql";
	}

	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
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

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		out("insert into "+tableName+" "+
			colNames+" values ("+
			DataDumpUtils.join4sql(vals, dateFormatter, ", ")+
			");", fos);
	}
	
	void out(String s, Writer pw) throws IOException {
		pw.write(s+"\n");
	}

	@Override
	public void dumpHeader(Writer fos) throws Exception {
		//do nothing
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		//do nothing
	}

	@Override
	public String getSyntaxId() {
		return INSERTINTO_SYNTAX_ID;
	}
}
