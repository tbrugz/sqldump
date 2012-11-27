package tbrugz.sqldump;

import java.io.PrintStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class SQLUtils {
	
	public static class ConnectionUtil {
		
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
			log.debug("initDBConnection...");
			
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
			
			conn.setAutoCommit(autoCommit);
			
			String dbInitSql = papp.getProperty(propsPrefix+SUFFIX_INITSQL);
			if(dbInitSql!=null) {
				try {
					int count = conn.createStatement().executeUpdate(dbInitSql);
					log.info("init sql [count="+count+"]: "+dbInitSql);
				}
				catch(SQLException e) {
					log.warn("error in init sql: "+dbInitSql);
					try { conn.rollback(); }
					catch(SQLException ee) { log.warn("error in rollback(): "+ee.getMessage()); }
				}
			}
			return conn;
		}
		
		static Connection creteNewConnection(String propsPrefix, Properties papp, String driverClass, String dbUrl) throws ClassNotFoundException, SQLException {
			if(driverClass==null) {
				log.error("driver class property '"+propsPrefix+SUFFIX_DRIVERCLASS+"' undefined. can't proceed");
				return null;
			}
			if(dbUrl==null) {
				log.error("db url property '"+propsPrefix+SUFFIX_URL+"' undefined. can't proceed");
				return null;
			}

			Class.forName(driverClass);
			
			Driver driver = DriverManager.getDriver(dbUrl);
			if(driver!=null) {
				log.debug("jdbc driver: "+driver+"; version: "+driver.getMajorVersion()+"."+driver.getMinorVersion()+"; jdbc-compliant: "+driver.jdbcCompliant());
			}
			else {
				log.warn("jdbc driver not found [url: "+dbUrl+"]");
			} 
			
			Properties p = new Properties();
			p.setProperty(CONN_PROP_USER, papp.getProperty(propsPrefix+SUFFIX_USER, ""));
			p.setProperty(CONN_PROP_PASSWORD, papp.getProperty(propsPrefix+SUFFIX_PASSWD, ""));
			
			if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME)) {
				p.setProperty(CONN_PROP_USER, Utils.readText("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': "));
			}
			else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORUSERNAME_GUI)) {
				p.setProperty(CONN_PROP_USER, Utils.readTextGUI("username for '"+papp.getProperty(propsPrefix+SUFFIX_URL)+"': "));
			}

			if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD)) {
				p.setProperty(CONN_PROP_PASSWORD, Utils.readPassword("password [user="+p.getProperty(CONN_PROP_USER)+"]: "));
			}
			else if(Utils.getPropBool(papp, propsPrefix+SUFFIX_ASKFORPASSWD_GUI)) {
				p.setProperty(CONN_PROP_PASSWORD, Utils.readPasswordGUI("password [user="+p.getProperty(CONN_PROP_USER)+"]: "));
			}

			return DriverManager.getConnection(dbUrl, p);
		}
		
		// see: http://www.tomcatexpert.com/blog/2010/04/01/configuring-jdbc-pool-high-concurrency
		//XXX: prop for initial context lookup? like "java:/comp/env"...
		static Connection getConnectionFromDataSource(String dataSource) throws SQLException, NamingException {
			log.debug("getting connection from datasource: "+dataSource);
			Context initContext = new InitialContext();
			Context envContext  = (Context) initContext.lookup("java:/comp/env");
			DataSource datasource = (DataSource) envContext.lookup(dataSource);
			return datasource.getConnection();
		}
		
		public static void showDBInfo(DatabaseMetaData dbmd) {
			try {
				log.info("database info: "+dbmd.getDatabaseProductName()+"; "+dbmd.getDatabaseProductVersion()+" ["+dbmd.getDatabaseMajorVersion()+"."+dbmd.getDatabaseMinorVersion()+"]");
				log.info("jdbc driver info: "+dbmd.getDriverName()+"; "+dbmd.getDriverVersion()+" ["+dbmd.getDriverMajorVersion()+"."+dbmd.getDriverMinorVersion()+"]");
				log.debug("jdbc version: "+dbmd.getJDBCMajorVersion()+"."+dbmd.getJDBCMinorVersion());
			} catch (Exception e) {
				log.warn("error grabbing database/jdbc driver info: "+e);
				//e.printStackTrace();
			} catch (AbstractMethodError e) {
				log.warn("error grabbing database/jdbc driver info: "+e);
			}
		}
	}

	static Log log = LogFactory.getLog(SQLUtils.class);
	static StringBuffer sbTmp = new StringBuffer();

	public static String getRowFromRS(ResultSet rs, int numCol, String table) throws SQLException {
		return getRowFromRS(rs, numCol, table, ";");
	}
	
	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter) throws SQLException {
		return getRowFromRS(rs, numCol, table, delimiter, ""); //"'"?
	}

	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter, String enclosing) throws SQLException {
		sbTmp.setLength(0);
		for(int i=1;i<=numCol;i++) {
			String value = rs.getString(i);
			sbTmp.append(enclosing+value+enclosing);
			sbTmp.append(delimiter);
		}
		return sbTmp.toString();
	}

	public static List<String> getRowListFromRS(ResultSet rs, int numCol) throws SQLException {
		List<String> ls = new ArrayList<String>();
		for(int i=1;i<=numCol;i++) {
			String value = rs.getString(i);
			ls.add(value);
		}
		return ls;
	}
	
	static boolean isInt(double d) {
		long l = (long) d*1000;
		return (l==Math.round(d*1000));
	}

	static boolean resultSetGetObjectExceptionWarned = false;
	
	public static List<Object> getRowObjectListFromRS(ResultSet rs, List<Class<?>> colTypes, int numCol) throws SQLException {
		return getRowObjectListFromRS(rs, colTypes, numCol, false);
	}
	
	public static List<Object> getRowObjectListFromRS(ResultSet rs, List<Class<?>> colTypes, int numCol, boolean canReturnResultSet) throws SQLException {
		List<Object> ls = new ArrayList<Object>();
		for(int i=1;i<=numCol;i++) {
			Object value = null;
			Class<?> coltype = null;
			try {
				coltype = colTypes.get(i-1);
			}
			catch(IndexOutOfBoundsException e) {
				e.printStackTrace();
				coltype = String.class;
			}
			
			if(coltype.equals(Blob.class)) {
				//XXX: do not dump Blobs this way
				//value = null; //Blob and ResultSet should be tested first? yes!
			}
			else if(coltype.equals(ResultSet.class) || coltype.equals(Array.class)) {
				if(canReturnResultSet) {
					try {
						value = rs.getObject(i);
						if(value instanceof Array) {
							value = ((Array)value).getResultSet();
						}
					}
					catch(SQLException e) {
						if(!resultSetGetObjectExceptionWarned) {
							log.warn("error loading ResultSet: "+e+" (you might not use multiple ResultSet-able dumpers when dumping ResultSet/cursors)");
							log.info("error loading ResultSet (you might not use multiple ResultSet-able dumpers when dumping ResultSet/cursors)", e);
							resultSetGetObjectExceptionWarned = true;
						}
					}
				}
				//log.info("obj/resultset: "+rs.getObject(i));
			}
			else {

			value = rs.getString(i);
			if(value==null) {}
			else if(coltype.equals(String.class)) {
				//value = rs.getString(i);
				//do nothing (value already setted)
			}
			else if(coltype.equals(Integer.class)) {
				value = rs.getDouble(i);
				Double dValue = (Double) value;
				if(isInt(dValue)) {
					//log.warn("long type: "+i+"/"+coltype+"/"+rs.getLong(i)+"/"+rs.getDouble(i));
					value = rs.getLong(i);
				}
				else { 
					//log.warn("double type: "+i+"/"+coltype+"/"+rs.getDouble(i));
					//value = rs.getDouble(i); //no need for doing again
				}
			}
			else if(coltype.equals(Double.class)) {
				value = rs.getDouble(i);
				Double dValue = (Double) value;
				if(isInt(dValue)) {
					//log.warn("long type: "+i+"/"+coltype+"/"+rs.getLong(i));
					value = rs.getLong(i);
				}
				else { value = rs.getDouble(i); }
			}
			else if(coltype.equals(Date.class)) {
				//TODOne: how to format Date value?
				value = rs.getDate(i);
			}
			else if(coltype.equals(Object.class)) {
				value = rs.getObject(i);
				//XXX: show log.info on 1st time only?
				log.info("generic type ["+value.getClass().getSimpleName()+"/"+coltype.getSimpleName()+"] grabbed");
				if(canReturnResultSet && ResultSet.class.isAssignableFrom(value.getClass())) {
					log.warn("setting column type ["+coltype.getSimpleName()+"] as ResultSet type - you may not use multiple dumpers for this");
					colTypes.set(i-1, ResultSet.class);					
				}
			}
			else {
				//XXX: show log.warn on 1st time only?
				log.warn("unknown type ["+coltype+"], defaulting to String");
				//value = rs.getString(i); // no need to get value again
			}
			
			}
			
			ls.add(value);
		}
		return ls;
	}
	
	static Set<Integer> unknownSQLTypes = new HashSet<Integer>(); 
	//TODOne: Date class type for dump?
	public static Class<?> getClassFromSqlType(int type, int precision, int scale) {
		//log.debug("type: "+type);
		switch(type) {
			case Types.TINYINT: 
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
				return Integer.class;
			case Types.DECIMAL:
			case Types.NUMERIC:
				return (scale>0)?Double.class:((precision==0)&&(scale==0))?Double.class:Integer.class; //XXX: doesnt seems to work
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
				return Double.class;
			case Types.DATE:
			case Types.TIMESTAMP:
				return Date.class;
			case Types.CHAR:
			case Types.VARCHAR:
				return String.class;
			case Types.LONGVARBINARY:
				return Blob.class;
			case Types.ARRAY:
				//return Array.class;
			case -10: //XXX: ResultSet/Cursor (Oracle)?
				return ResultSet.class;
			case Types.OTHER:
				return Object.class;
			default:
				//convert to Sring? http://www.java2s.com/Code/Java/Database-SQL-JDBC/convertingajavasqlTypesintegervalueintoaprintablename.htm
				if(!unknownSQLTypes.contains(type)) {
					log.warn("unknown SQL type ["+type+"], defaulting to String");
					unknownSQLTypes.add(type);
				}
				return String.class;
		}
	}
	
	public static void dumpRS(ResultSet rs) throws SQLException {
		dumpRS(rs, rs.getMetaData(), System.out);
	}

	public static void dumpRS(ResultSet rs, PrintStream out) throws SQLException {
		dumpRS(rs, rs.getMetaData(), out);
	}

	public static void dumpRS(ResultSet rs, ResultSetMetaData rsmd, PrintStream out) throws SQLException {
		int ncol = rsmd.getColumnCount();
		StringBuffer sb = new StringBuffer();
		//System.out.println(ncol);
		//System.out.println();
		for(int i=1;i<=ncol;i++) {
			//System.out.println(rsmd.getColumnName(i)+" | ");
			sb.append(rsmd.getColumnLabel(i)+" | ");
		}
		sb.append("\n");
		while(rs.next()) {
			for(int i=1; i<= rsmd.getColumnCount(); i++) {
				sb.append(rs.getString(i) + " | ");
			}
			sb.append("\n");
		}
		out.println("\n"+sb.toString()+"\n");
	}
	
	public static String getColumnNames(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<String> lsColNames = new ArrayList<String>();
		//List<Class> lsColTypes = new ArrayList<Class>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		//for(int i=0;i<numCol;i++) {
		//	lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1)));
		//}
		return Utils.join(lsColNames, ", ");
	}
	
	static List<String> getColumnValues(ResultSet rs, String colName) throws SQLException {
		List<String> colvals = new ArrayList<String>();
		while(rs.next()) {
			colvals.add(rs.getString(colName));
		}
		return colvals;
	}

	public static List<String> getSchemaNames(DatabaseMetaData dbmd) throws SQLException {
		return getColumnValues(dbmd.getSchemas(), "table_schem");
	}

}
