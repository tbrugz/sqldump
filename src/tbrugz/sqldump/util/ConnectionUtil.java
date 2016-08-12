package tbrugz.sqldump.util;

import java.io.UnsupportedEncodingException;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConnectionUtil {
	
	static final Log log = LogFactory.getLog(ConnectionUtil.class);
	
	static final String DEFAULT_INITIAL_CONTEXT = "java:/comp/env";
	
	//connection props
	public static final String CONN_PROP_USER = "user";
	public static final String CONN_PROP_PASSWORD = "password";
	
	//connection properties suffixes
	public static final String SUFFIX_CONNECTION_DATASOURCE = ".datasource"; //.(conn(ection))pool(name)
	public static final String SUFFIX_DATASOURCE_CONTEXTLOOKUP = ".datasource.contextlookup";
	public static final String SUFFIX_DRIVERCLASS = ".driverclass";
	public static final String SUFFIX_URL = ".dburl";
	public static final String SUFFIX_USER = ".user";
	public static final String SUFFIX_PASSWD = ".password";
	public static final String SUFFIX_PASSWD_BASE64 = ".password.base64";
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
		log.debug("initDBConnection... [propsPrefix="+propsPrefix+"] [autoCommit="+autoCommit+"]");
		
		String connectionDataSource = papp.getProperty(propsPrefix+SUFFIX_CONNECTION_DATASOURCE);
		String initialContextLookup = papp.getProperty(propsPrefix+SUFFIX_DATASOURCE_CONTEXTLOOKUP, DEFAULT_INITIAL_CONTEXT);
		
		String driverClass = papp.getProperty(propsPrefix+SUFFIX_DRIVERCLASS);
		String dbUrl = papp.getProperty(propsPrefix+SUFFIX_URL);

		Connection conn = null;
		if(connectionDataSource!=null) {
			conn = getConnectionFromDataSource(connectionDataSource, initialContextLookup);
		}
		else {
			conn = creteNewConnection(propsPrefix, papp, driverClass, dbUrl);
		}
		if(conn==null) { throw new IllegalStateException("connection is null!"); }
		
		if(log.isDebugEnabled()) {
			try {
				Properties pclient = conn.getClientInfo();
				if(pclient.size()==0) {
					log.debug("no Connection.getClientInfo() info available");
				}
				else {
					for(Object key: pclient.keySet()) {
						log.debug("client-info: "+key+" = "+pclient.getProperty((String)key));
					}
				}
			}
			catch(Exception ex) {
				log.warn("exception on Connection.getClientInfo: "+ex);
			}
			catch(LinkageError e) {
				log.warn("error on Connection.getClientInfo: "+e);
			}
		}
		
		conn.setAutoCommit(autoCommit);
		
		//XXX: initsql: log only 1st execution?
		//XXX: initsql: option to execute multiple statements?
		String dbInitSql = papp.getProperty(propsPrefix+SUFFIX_INITSQL);
		if(dbInitSql!=null) {
			try {
				int count = conn.createStatement().executeUpdate(dbInitSql);
				log.info("init sql [prefix '"+propsPrefix+"'; updateCount="+count+"]: "+dbInitSql);
			}
			catch(SQLException e) {
				log.warn("error in init sql: "+dbInitSql+" [ex:"+e+"]");
				try { conn.rollback(); }
				catch(SQLException ee) { log.warn("error in rollback(): "+ee.getMessage()); }
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
	
	public static String getBasePropertiesSuffixStr() {
		return "('"+SUFFIX_DRIVERCLASS+"' and '"+SUFFIX_URL+"') or '"+SUFFIX_CONNECTION_DATASOURCE+"'";
	}
	
	static Connection creteNewConnection(String propsPrefix, Properties papp, String driverClass, String dbUrl) throws ClassNotFoundException, SQLException {
		if(driverClass==null) {
			String message = "driver class property '"+propsPrefix+SUFFIX_DRIVERCLASS+"' undefined (using JDBC 4+ ?)";
			log.debug(message);
		}
		else {
			Class.forName(driverClass);
		}

		if(dbUrl==null) {
			String message = "db url property '"+propsPrefix+SUFFIX_URL+"' undefined. can't proceed"; 
			log.error(message);
			throw new RuntimeException(message);
		}
		
		Driver driver = null;
		try {
			driver = DriverManager.getDriver(dbUrl);
			if(driver!=null) {
				log.debug("jdbc driver: "+driver+"; version: "+driver.getMajorVersion()+"."+driver.getMinorVersion()+"; jdbc-compliant: "+driver.jdbcCompliant());
			}
			else {
				log.warn("jdbc driver not found [url: "+dbUrl+"]?");
			}
		}
		catch(SQLException e) {
			log.warn("jdbc driver not found [url: '"+dbUrl+"'"
				+(driverClass==null?" ; property '"+propsPrefix+SUFFIX_DRIVERCLASS+"' undefined":"")
				+"]");
			throw e;
		}
		
		Properties p = new Properties();
		String user = papp.getProperty(propsPrefix+SUFFIX_USER);
		String password = papp.getProperty(propsPrefix+SUFFIX_PASSWD);
		
		if(user==null) {
		if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME)) {
			user = Utils.readText("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': ");
		}
		else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME_GUI)) {
			user = Utils.readTextGUI("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': ");
		}
		}

		if(password==null) {
			String passBase64 = papp.getProperty(propsPrefix+SUFFIX_PASSWD_BASE64);
			if(passBase64!=null) {
				try {
					password = Utils.parseBase64(passBase64);
				} catch (UnsupportedEncodingException e) {
					log.warn("error loading base64 password [prop '"+propsPrefix+SUFFIX_PASSWD_BASE64+"']"); //show exception?
				}
			}
		}
		if(password==null) {
		if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD)) {
			password = Utils.readPassword("password [user="+user+"]: ");
		}
		else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD_GUI)) {
			password = Utils.readPasswordGUI("password [user="+user+"]: ");
		}
		}
		
		if(user!=null) { p.setProperty(CONN_PROP_USER, user); }
		if(password!=null) { p.setProperty(CONN_PROP_PASSWORD, password); }

		//use DatabaseMetaData: getUserName() & getUrl()?
		log.debug("conn: "+user+"@"+dbUrl);
		
		try {
			return DriverManager.getConnection(dbUrl, p);
		}
		catch(SQLException e) {
			log.warn("error creating connection: '"+user+"@"+dbUrl+"'"
					+((password==null)?" [null password]":"")
					);
			throw e;
		}
	}
	
	// see: http://www.tomcatexpert.com/blog/2010/04/01/configuring-jdbc-pool-high-concurrency
	//TODOne: prop for initial context lookup? like "java:/comp/env"...
	static Connection getConnectionFromDataSource(String dataSource, String contextLookup) throws SQLException, NamingException {
		log.debug("getting connection from datasource '"+dataSource+"' [context: "+contextLookup+"]");
		Context initContext = new InitialContext();
		Context envContext  = (Context) initContext.lookup(contextLookup);
		DataSource datasource = (DataSource) envContext.lookup(dataSource);
		return datasource.getConnection();
	}
	
	public static void closeConnection(Connection conn) {
		if(conn!=null) {
			log.debug("closing connection: "+conn);
			try {
				try {
					conn.rollback();
				}
				catch(Exception e) {
					log.warn("error trying to 'rollback': "+e);
				}
				conn.close();
			} catch (SQLException e) {
				log.warn("error trying to close connection: "+e);
				log.debug("error trying to close connection [conn="+conn+"]", e);
			}
		}
	}
	
	public static void showDBInfo(DatabaseMetaData dbmd) {
		try {
			log.info("database info: "+dbmd.getDatabaseProductName()+"; "+dbmd.getDatabaseProductVersion()+" ["+dbmd.getDatabaseMajorVersion()+"."+dbmd.getDatabaseMinorVersion()+"]");
			log.info("jdbc driver info: "+dbmd.getDriverName()+"; "+dbmd.getDriverVersion()+" ["+dbmd.getDriverMajorVersion()+"."+dbmd.getDriverMinorVersion()+"]");
			log.debug("jdbc version: "+dbmd.getJDBCMajorVersion()+"."+dbmd.getJDBCMinorVersion());
		} catch (Exception e) {
			log.warn("error grabbing database/jdbc driver info: "+e);
			//e.printStackTrace();
		} catch (LinkageError e) {
			log.warn("error grabbing database/jdbc driver info: "+e);
		}
	}
}
