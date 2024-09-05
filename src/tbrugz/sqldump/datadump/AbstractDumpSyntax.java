package tbrugz.sqldump.datadump;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLUtils;

public abstract class AbstractDumpSyntax extends DumpSyntax {

	public static final String DEFAULT_DATADUMP_PREFIX = "sqldump.datadump.";
	
	protected String schemaName;
	protected String tableName;
	protected int numCol;
	protected List<String> lsColNames = new ArrayList<String>();
	protected List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	//ResultSetMetaData md;
	protected List<String> pkCols;
	
	public String fullPrefix() {
		return DEFAULT_DATADUMP_PREFIX + getSyntaxId() + ".";
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.schemaName = schema;
		this.tableName = tableName;
		//this.md = md;
		this.pkCols = pkCols;
		this.numCol = md.getColumnCount();
		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnLabel(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
	}

	public AbstractDumpSyntax build(String schemaName, String tableName,
			List<String> pkCols, ResultSetMetaData md) throws SQLException {
		try {
			AbstractDumpSyntax dd = (AbstractDumpSyntax) clone();
			dd.initDump(schemaName, tableName, pkCols, md);
			return dd;
		}
		catch(CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public AbstractDumpSyntax clone() throws CloneNotSupportedException {
		AbstractDumpSyntax ds = (AbstractDumpSyntax) super.clone();
		ds.lsColNames = new ArrayList<String>();
		ds.lsColTypes = new ArrayList<Class<?>>();
		return ds;
	}
	
}
