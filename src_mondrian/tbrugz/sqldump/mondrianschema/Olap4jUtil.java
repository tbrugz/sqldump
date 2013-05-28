package tbrugz.sqldump.mondrianschema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.olap4j.OlapConnection;

public class Olap4jUtil {

	public static OlapConnection getConnection(String driverClass, String schemaFile, String dataSource, String url, String username, String password) throws ClassNotFoundException, SQLException {
		//construct mondrian URL
		String mondrianUrl = 
				"jdbc:mondrian:" +
				"JdbcDrivers="+driverClass+";" +
				//"Jdbc="+url+";" +
				"Catalog="+schemaFile+";";
		if(dataSource!=null) {
			mondrianUrl += "DataSource=" + dataSource + ";";
		}
		else {
			mondrianUrl += "Jdbc="+url+";";
		}
		if(username != null && username.length() > 0) {
			mondrianUrl += "JdbcUser=" + username + ";";
		}
		if(password != null && password.length() > 0) {
			mondrianUrl += "JdbcPassword=" + password + ";";
		}

		//create connection
		Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
		Connection connection = DriverManager.getConnection(mondrianUrl);
		OlapConnection oConnection = connection.unwrap(OlapConnection.class);
		
		return oConnection;
	}
}
