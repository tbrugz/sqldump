package tbrugz.sqldump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.IOUtil;

public class TestUtil {

	public static final String[] NULL_PARAMS = null;

	public static Connection getConn(Properties prop, String prefix) throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = ConnectionUtil.initDBConnection(prefix, prop);
		DBMSResources.instance().setup(prop);
		//DBMSResources.instance().updateMetaData(conn.getMetaData());
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
	
	public static Properties createProperties(String[] vmparams) {
		Properties p = new Properties();
		setProperties(p, vmparams);
		return p;
	}
	
	public static int countLinesFromPath(String path) throws IOException {
		String content = IOUtil.readFromFilename(path);
		return countLines(content);
	}
	
	public static int countLines(String content) throws IOException {
		BufferedReader sr = new BufferedReader( new StringReader(content) );
		@SuppressWarnings("unused")
		String news = null;
		int count = 0;
		while((news = sr.readLine()) != null) {
			count++;
		} 
		return count;
	}
}
