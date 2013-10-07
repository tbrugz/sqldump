package tbrugz.sqldump.pivot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class SQLPivotDriver implements java.sql.Driver {
	
	static final String SQLPIVOT_DRIVER_PREFIX = "jdbc:sqlpivot:";
	
	static final SQLPivotDriver instance = new SQLPivotDriver();
	
	static {
		try {
			DriverManager.registerDriver(instance);
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if(!acceptsURL(url)) {
			//throw new RuntimeException("url '"+url+"' does not match required prefix: "+SQLPIVOT_DRIVER_PREFIX);
			return null;
		}
		String innerUrl = "jdbc:"+url.substring(SQLPIVOT_DRIVER_PREFIX.length());
		Connection conn = DriverManager.getConnection(innerUrl, info);
		
		return new PivotConnection(conn);
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(SQLPIVOT_DRIVER_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return null;
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

}
