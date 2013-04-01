package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.SQLUtils;

public class TestUtil {

	public static Connection getConn(Properties prop, String prefix) throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = SQLUtils.ConnectionUtil.initDBConnection(prefix, prop);
		DBMSResources.instance().setup(prop);
		DBMSResources.instance().updateMetaData(conn.getMetaData());
		return conn;
	}

	public static void setProperties(Properties p, String[] vmparams) {
		for(String s: vmparams) {
			String key = null, value = null; 
			if(s.startsWith("-D")) {
				int i = s.indexOf("=");
				key = s.substring(2,i);
				value = s.substring(i+1);
				p.setProperty(key, value);
			}
		}
	}
}
