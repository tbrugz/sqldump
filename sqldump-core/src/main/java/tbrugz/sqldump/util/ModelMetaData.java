package tbrugz.sqldump.util;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

public class ModelMetaData {
	
	public static String SQLDUMP_VERSION = "sqldump.version",
		MODEL_GRABDATE = "model.grabdate",
		CONN_URL = "conn.url",
		CONN_USER = "conn.user",
		DB_PRODUCT = "db.product",
		DB_VERSION_STRING = "db.version.string",
		DB_VERSION_MAJOR = "db.version.major",
		DB_VERSION_MINOR = "db.version.minor",
		DRIVER_NAME = "jdbc.driver.name",
		DRIVER_VERSION = "jdbc.driver.version";
	
	/*
	static final String[] KEYS = {
		SQLDUMP_VERSION, MODEL_GRABDATE, DB_URL, DB_USER, DB_PRODUCT, DB_VERSION_STRING, DB_VERSION_MAJOR, DB_VERSION_MINOR
	};
	*/
	
	/*
	more info?

	- jdbc.driver.class - what if datasource?
	
	sys-properties:
	- file.encoding
	- os.arch
	- os.name
	- os.version
	- java.version
	- java.runtime.version
	 */
	
	/*
	 * XXX: add option to select which properties should be included
	 */
	public static Map<String,String> getProperties(DatabaseMetaData dbmd) throws SQLException {
		Map<String,String> prop = new TreeMap<String,String>();
		
		prop.put(SQLDUMP_VERSION, Version.getVersion());
		
		//http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		prop.put(MODEL_GRABDATE, df.format(new Date()));
		
		prop.put(CONN_URL, dbmd.getURL());
		prop.put(CONN_USER, dbmd.getUserName());
		prop.put(DB_PRODUCT, dbmd.getDatabaseProductName());
		prop.put(DB_VERSION_STRING, dbmd.getDatabaseProductVersion());
		prop.put(DB_VERSION_MAJOR, String.valueOf(dbmd.getDatabaseMajorVersion()));
		prop.put(DB_VERSION_MINOR, String.valueOf(dbmd.getDatabaseMinorVersion()));
		prop.put(DRIVER_NAME, dbmd.getDriverName());
		prop.put(DRIVER_VERSION, dbmd.getDriverVersion());
		
		return prop;
	}
}
