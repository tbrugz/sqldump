package tbrugz.sqldump.def;

import java.sql.Connection;

public abstract class AbstractSQLProc extends AbstractSchemaProcessor {
	
	protected Connection conn;

	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public Connection getConnection() {
		return conn;
	}
	
	@Override
	public boolean needsConnection() {
		return true;
	}
	
	@Override
	public Connection getNewConnection() {
		return null;
	}
	
}
