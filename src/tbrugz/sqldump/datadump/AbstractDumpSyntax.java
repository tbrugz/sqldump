package tbrugz.sqldump.datadump;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.SQLUtils;

public abstract class AbstractDumpSyntax extends DumpSyntax {

	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	ResultSetMetaData md;
	List<String> pkCols;
	
	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		this.md = md;
		this.pkCols = pkCols;
		this.numCol = md.getColumnCount();
		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
	}
	
}
