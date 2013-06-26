package tbrugz.sqldump.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class ConnectionUtil {
	
	//connection props
	public static final String CONN_PROP_USER = "user";
	public static final String CONN_PROP_PASSWORD = "password";
	
	//connection properties
	public static final String SUFFIX_CONNECTION_DATASOURCE = ".datasource"; //.(conn(ection))pool(name)
	public static final String SUFFIX_DRIVERCLASS = ".driverclass";
	public static final String SUFFIX_URL = ".dburl";
	public static final String SUFFIX_USER = ".user";
	public static final String SUFFIX_PASSWD = ".password";
	public static final String SUFFIX_ASKFORUSERNAME = ".askforusername";
	public static final String SUFFIX_ASKFORPASSWD = ".askforpassword";
	public static final String SUFFIX_ASKFORUSERNAME_GUI = ".askforusernamegui";
	public static final String SUFFIX_ASKFORPASSWD_GUI = ".askforpasswordgui";
	public static final String SUFFIX_INITSQL = ".initsql";

	public static Connection initDBConnection(String propsPrefix, Properties papp) throws ClassNotFoundException, SQLException, NamingException {
		// AutoCommit==false: needed for postgresql for refcursor dumping. see: http://archives.postgresql.org/pgsql-sql/2005-06/msg00176.php
		// anyway, i think 'false' should be default
		return initDBConnection(propsPrefix, papp, false);
	}

	public static Connection initDBConnection(String propsPrefix, Properties papp, boolean autoCommit) throws ClassNotFoundException, SQLException, NamingException {
		//init database
		SQLUtils.log.debug("initDBConnection...");
		
		String connectionDataSource = papp.getProperty(propsPrefix+SUFFIX_CONNECTION_DATASOURCE);
		String driverClass = papp.getProperty(propsPrefix+SUFFIX_DRIVERCLASS);
		String dbUrl = papp.getProperty(propsPrefix+SUFFIX_URL);
		Connection conn = null;
		if(connectionDataSource!=null) {
			conn = getConnectionFromDataSource(connectionDataSource);
		}
		else {
			conn = creteNewConnection(propsPrefix, papp, driverClass, dbUrl);
		}
		
		if(SQLUtils.log.isDebugEnabled()) {
			try {
				Properties pclient = conn.getClientInfo();
				if(pclient.size()==0) {
					SQLUtils.log.debug("no Connection.getClientInfo() info avaiable");
				}
				else {
					for(Object key: pclient.keySet()) {
						SQLUtils.log.debug("client-info: "+key+" = "+pclient.getProperty((String)key));
					}
				}
			}
			catch(Exception ex) {
				SQLUtils.log.warn("exception on Connection.getClientInfo: "+ex);
			}
			catch(LinkageError e) {
				SQLUtils.log.warn("error on Connection.getClientInfo: "+e);
			}
		}
		
		if(conn==null) { return null; }
		
		conn.setAutoCommit(autoCommit);
		
		String dbInitSql = papp.getProperty(propsPrefix+SUFFIX_INITSQL);
		if(dbInitSql!=null) {
			try {
				int count = conn.createStatement().executeUpdate(dbInitSql);
				SQLUtils.log.info("init sql [prefix '"+propsPrefix+"'; updateCount="+count+"]: "+dbInitSql);
			}
			catch(SQLException e) {
				SQLUtils.log.warn("error in init sql: "+dbInitSql+" [ex:"+e+"]");
				try { conn.rollback(); }
				catch(SQLException ee) { SQLUtils.log.warn("error in rollback(): "+ee.getMessage()); }
			}
		}
		return conn;
	}

	public static boolean isBasePropertiesDefined(String propsPrefix, Properties papp) {
		String connectionDataSource = papp.getProperty(propsPrefix+SUFFIX_CONNECTION_DATASOURCE);
		if(connectionDataSource!=null) { return true; }
		
		String driverClass = papp.getProperty(propsPrefix+SUFFIX_DRIVERCLASS);
		String dbUrl = papp.getProperty(propsPrefix+SUFFIX_URL);
		if(driverClass!=null && dbUrl!=null) { return true; }
		
		return false;
	}
	
	static Connection creteNewConnection(String propsPrefix, Properties papp, String driverClass, String dbUrl) throws ClassNotFoundException, SQLException {
		if(driverClass==null) {
			SQLUtils.log.error("driver class property '"+propsPrefix+SUFFIX_DRIVERCLASS+"' undefined. can't proceed");
			return null;
		}
		if(dbUrl==null) {
			SQLUtils.log.error("db url property '"+propsPrefix+SUFFIX_URL+"' undefined. can't proceed");
			return null;
		}

		Class.forName(driverClass);
		
		Driver driver = DriverManager.getDriver(dbUrl);
		if(driver!=null) {
			SQLUtils.log.debug("jdbc driver: "+driver+"; version: "+driver.getMajorVersion()+"."+driver.getMinorVersion()+"; jdbc-compliant: "+driver.jdbcCompliant());
		}
		else {
			SQLUtils.log.warn("jdbc driver not found [url: "+dbUrl+"]");
		} 
		
		Properties p = new Properties();
		String user = papp.getProperty(propsPrefix+SUFFIX_USER, "");
		p.setProperty(CONN_PROP_USER, user);
		p.setProperty(CONN_PROP_PASSWORD, papp.getProperty(propsPrefix+SUFFIX_PASSWD, ""));
		
		if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME)) {
			user = Utils.readText("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': ");
			p.setProperty(CONN_PROP_USER, user);
		}
		else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME_GUI)) {
			user = Utils.readTextGUI("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': ");
			p.setProperty(CONN_PROP_USER, user);
		}

		if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD)) {
			p.setProperty(CONN_PROP_PASSWORD, Utils.readPassword("password [user="+user+"]: "));
		}
		else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD_GUI)) {
			p.setProperty(CONN_PROP_PASSWORD, Utils.readPasswordGUI("password [user="+user+"]: "));
		}

		//use DatabaseMetaData: getUserName() & getUrl()?
		SQLUtils.log.debug("conn: "+user+"@"+dbUrl);
		
		return DriverManager.getConnection(dbUrl, p);
	}
	
	// see: http://www.tomcatexpert.com/blog/2010/04/01/configuring-jdbc-pool-high-concurrency
	//XXX: prop for initial context lookup? like "java:/comp/env"...
	static Connection getConnectionFromDataSource(String dataSource) throws SQLException, NamingException {
		SQLUtils.log.debug("getting connection from datasource: "+dataSource);
		Context initContext = new InitialContext();
		Context envContext  = (Context) initContext.lookup("java:/comp/env");
		DataSource datasource = (DataSource) envContext.lookup(dataSource);
		return datasource.getConnection();
	}
	
	public static void closeConnection(Connection conn) {
		if(conn!=null) {
			SQLUtils.log.info("closing connection: "+conn);
			try {
				try {
					conn.rollback();
				}
				catch(Exception e) {
					SQLUtils.log.warn("error trying to 'rollback': "+e);
				}
				conn.close();
			} catch (SQLException e) {
				SQLUtils.log.warn("error trying to close connection: "+e);
				SQLUtils.log.debug("error trying to close connection [conn="+conn+"]", e);
			}
		}
	}
	
	public static void showDBInfo(DatabaseMetaData dbmd) {
		try {
			SQLUtils.log.info("database info: "+dbmd.getDatabaseProductName()+"; "+dbmd.getDatabaseProductVersion()+" ["+dbmd.getDatabaseMajorVersion()+"."+dbmd.getDatabaseMinorVersion()+"]");
			SQLUtils.log.info("jdbc driver info: "+dbmd.getDriverName()+"; "+dbmd.getDriverVersion()+" ["+dbmd.getDriverMajorVersion()+"."+dbmd.getDriverMinorVersion()+"]");
			SQLUtils.log.debug("jdbc version: "+dbmd.getJDBCMajorVersion()+"."+dbmd.getJDBCMinorVersion());
		} catch (Exception e) {
			SQLUtils.log.warn("error grabbing database/jdbc driver info: "+e);
			//e.printStackTrace();
		} catch (LinkageError e) {
			SQLUtils.log.warn("error grabbing database/jdbc driver info: "+e);
		}
	}
}