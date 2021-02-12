package tbrugz.sqldump.util;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Blob;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBType;

public class SQLUtils {
	
	static final Log log = LogFactory.getLog(SQLUtils.class);
	
	final static String COL_TABLE_CAT = "TABLE_CAT"; //"table_cat"
	final static String COL_TABLE_SCHEM = "TABLE_SCHEM"; //"table_schem"
	
	static final String PROP_STRANGE_PRECISION_NUMERIC_AS_INT = "sqldump.sqlutils.strangePrecisionNumericAsInt";
	static final String PROP_DEFAULT_TYPE_IS_STRING = "sqldump.sqlutils.defaultTypeIsString";
	static final String PROP_CLOB_TYPE_IS_STRING = "sqldump.sqlutils.clobTypeIsString";
	
	static final String BLOB_NOTNULL_PLACEHOLDER = "[blob]"; //""
	
	static boolean strangePrecisionNumericAsInt = false;
	static boolean defaultTypeIsString = true;
	static boolean clobTypeIsString = true;
	public static boolean failOnError = false;
	static boolean arrayTypeIsArray = true;
	
	public static void setProperties(Properties prop) {
		if(prop==null) { return; }
		else {
			strangePrecisionNumericAsInt = Utils.getPropBool(prop, PROP_STRANGE_PRECISION_NUMERIC_AS_INT, strangePrecisionNumericAsInt);
			defaultTypeIsString = Utils.getPropBool(prop, PROP_DEFAULT_TYPE_IS_STRING, defaultTypeIsString);
			clobTypeIsString = Utils.getPropBool(prop, PROP_CLOB_TYPE_IS_STRING, clobTypeIsString);
		}
	}

	public static String getRowFromRS(ResultSet rs, int numCol, String table) throws SQLException {
		return getRowFromRS(rs, numCol, table, ";");
	}
	
	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter) throws SQLException {
		return getRowFromRS(rs, numCol, table, delimiter, ""); //"'"?
	}

	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter, String enclosing) throws SQLException {
		StringBuilder sbTmp = new StringBuilder();
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

	static Set<String> genericObjectsWarned = new HashSet<String>();
	
	static {
		genericObjectsWarned.add(String.class.getName()); // ignoring String class
	}
	
	public static List<Object> getRowObjectListFromRS(ResultSet rs, List<Class<?>> colTypes, int numCol) throws SQLException {
		return getRowObjectListFromRS(rs, colTypes, numCol, false);
	}
	
	//static String errorGettingValueValue = "###";
	static final String errorGettingValueValue = null;
	static final String errorGettingValueStringValue = null; //"\ufffd";
	static int errorGettingValueWarnCount = 0;
	static final int errorGettingValueWarnMaxCount = 10;
	
	static boolean is1stRow = true;
	static Map<Class<?>, Class<?>> colTypeMapper = null;
	
	public static void setupForNewQuery() {
		/*if(numCol>0) {
			errorGettingValueWarnMaxCount = numCol;
			log.info("setupForNewQuery: numCol = "+numCol);
		}*/
		errorGettingValueWarnCount = 0;
		is1stRow = true;
	}

	public static void setupColumnTypeMapper(Map<Class<?>, Class<?>> columnTypeMapper) {
		colTypeMapper = columnTypeMapper;
	}
	
	public static List<Object> getRowObjectListFromRS(ResultSet rs, List<Class<?>> colTypes, int numCol, boolean canReturnResultSet) throws SQLException {
		if(rs.getMetaData().getColumnCount()!=numCol) {
			log.debug("getMetaData().getColumnCount() ["+rs.getMetaData().getColumnCount()+"] != numCol ["+numCol+"]");
		}
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
			
			if(colTypeMapper!=null) {
				Class<?> cTmp = colTypeMapper.get(coltype);
				if(cTmp!=null) {
					coltype = cTmp;
				}
			}
			
			try {

			if(coltype.equals(Blob.class)) {
				// http://docs.oracle.com/javase/7/docs/api/java/sql/Types.html#BLOB
				// http://docs.oracle.com/javase/7/docs/api/java/sql/Blob.html
				// https://docs.oracle.com/javase/tutorial/jdbc/basics/blob.html
				//long initTime = System.currentTimeMillis();
				value = rs.getObject(i);
				boolean isNull = rs.wasNull();
				if(!isNull) {
					value = BLOB_NOTNULL_PLACEHOLDER;
				}
				//log.info("wasNull["+i+";"+coltype+"]: "+isNull+" - "+(System.currentTimeMillis()-initTime)+" val:"+value);
				
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
				value = rs.getTimestamp(i);
				//XXX value = rs.getDate(i) //?
			}
			else if(coltype.equals(Boolean.class)) {
				value = rs.getBoolean(i);
			}
			else if(coltype.equals(Object.class)) {
				value = rs.getObject(i);
				if(value!=null) {
					Class<?> clazz = value.getClass();
					String objectClassName = clazz.getName();
					if(!genericObjectsWarned.contains(objectClassName)) {
						if(value instanceof DBType) {
							log.debug("DBType type ["+objectClassName+";"+i+"] grabbed");
						}
						else {
							log.warn("generic type ["+objectClassName+";"+i+"] grabbed");
						}
						genericObjectsWarned.add(objectClassName);
					}
					if(canReturnResultSet && ResultSet.class.isAssignableFrom(clazz)) {
						log.warn("setting column type ["+coltype.getSimpleName()+"] as ResultSet type - you may not use multiple dumpers for this");
						colTypes.set(i-1, ResultSet.class);
					}
				}
			}
			else {
				//XXX: show log.warn on 1st time only?
				log.warn("getRow: unknown type ["+coltype+"], defaulting to "+(defaultTypeIsString?"String":"Object"));
				if(!defaultTypeIsString) {
					value = rs.getObject(i);
				}
				//value = rs.getString(i); // no need to get value again
			}
			
			}
			
			}
			catch(ArrayIndexOutOfBoundsException e) {
				value = coltype.equals(String.class)?errorGettingValueStringValue:errorGettingValueValue;
				errorGettingValueWarnCount++;
				if( (errorGettingValueWarnCount <= errorGettingValueWarnMaxCount) ) {
					log.warn("error getting value [col="+i+", type="+coltype.getSimpleName()+"; numCol="+numCol+"; rs.columnCount="+rs.getMetaData().getColumnCount()+"]: "+e);
					//log.info("error getting value...", e);
				}
				if(failOnError) { throw e; }
			}
			catch(SQLException e) {
				value = coltype.equals(String.class)?errorGettingValueStringValue:errorGettingValueValue;
				errorGettingValueWarnCount++;
				if( (errorGettingValueWarnCount <= errorGettingValueWarnMaxCount) ) {  //|| is1stRow
					log.warn("error getting value [col="+i+", type="+coltype.getSimpleName()
							+", count="+errorGettingValueWarnCount
							+(errorGettingValueWarnCount==errorGettingValueWarnMaxCount?"; max warn count ["+errorGettingValueWarnMaxCount+"] reached":"")
							+"]: "+e);
					log.debug("error getting value [col="+i+"]", e);
				}
				if(failOnError) { throw e; }
			}
			
			ls.add(value);
		}
		is1stRow = false;
		return ls;
	}
	
	static Set<Integer> unknownSQLTypes = new HashSet<Integer>(); 
	
	//XXX: add BigDecimal (for NUMERIC & DECIMAL), TIMESTAMP (for TIMESTAMP)
	// see: http://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
	public static Class<?> getClassFromSqlType(int type, int precision, int scale) {
		//log.info("type: "+type);
		switch(type) {
			case Types.TINYINT:  // -6
			case Types.SMALLINT: //  5
			case Types.INTEGER:  //  4
			case Types.BIGINT:   // -5
				return Integer.class;
			case Types.DECIMAL:  //  3
			case Types.NUMERIC:  //  2
				/*
				 * XXX numeric with precision == 0 (& scale <= 0) ?
				 * http://stackoverflow.com/questions/1410267/oracle-resultsetmetadata-getprecision-getscale
				 * 
				 * oracle: maybe set sys prop: oracle.jdbc.J2EE13Compliant=true
				 *   http://docs.oracle.com/cd/E19798-01/821-1751/beamw/index.html
				 *   http://ora-jdbc-source.googlecode.com/svn/trunk/OracleJDBC/src/oracle/jdbc/driver/OracleResultSetMetaData.java
				 */
				return
					( scale>0 ) ? Double.class:
					//( scale!=0 ) ? Double.class:
					( (precision>0)&&(scale<0) ) ? Double.class: // might work better for oracle if J2EE13Compliant=false -- should it be (precision != 0) && (scale == -127)) ?
					( precision>0 ) ? Integer.class:
					//( (precision==0)&&(scale==0) ) ? Double.class:
					// assuming Integer? (if actual data is not, dump syntax will hopefully take care)
					// ( precision <= 0 && scale <= 0 ) ==>
					(strangePrecisionNumericAsInt?Integer.class:Double.class); //"strange" precision: less or equal zero 
			case Types.REAL:     //  7
			case Types.FLOAT:    //  6
			case Types.DOUBLE:   //  8
				return Double.class;
			case Types.DATE:     // 91
				return Date.class;
			case Types.TIMESTAMP:// 93
			//case Types.TIMESTAMP_WITH_TIMEZONE: //java8
			case 2014: //TIMESTAMP_WITH_TIMEZONE  //java8 - https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html#TIMESTAMP_WITH_TIMEZONE
			case -101: //oracle TIMESTAMP WITH TIMEZONE
			case -102: //oracle TIMESTAMP WITH LOCAL TIMEZONE
			//pgsql - http://www.postgresql.org/docs/current/static/datatype-datetime.html
				return Date.class; //return Timestamp.class;
			case Types.CHAR:     //    1
			case Types.VARCHAR:  //   12
			//case Types.CLOB:   // 2005
				return String.class;
			case Types.BOOLEAN:
				return Boolean.class;
			case Types.BINARY:        //   -2  // postgresql BYTEA
			case Types.VARBINARY:     //   -3  // mysql BLOB
			case Types.LONGVARBINARY: //   -4
			case Types.BLOB:          // 2004
				return Blob.class;
			case Types.ARRAY:         // 2003
				if(arrayTypeIsArray) {
					return Array.class;
				}
			//case Types.REF_CURSOR:    // 2012
			case -10: //XXX: ResultSet/Cursor/Refcursor (Oracle)?
				return ResultSet.class;
			case Types.JAVA_OBJECT:   // 2000
				return Object.class;
			case Types.SQLXML:        // 2009
			case 2007:                // Oracle XMLTYPE?
				if(!unknownSQLTypes.contains(type)) {
					log.warn("unknown (XML) SQL type ["+type+"], defaulting to "+(defaultTypeIsString?"String":"Object"));
					unknownSQLTypes.add(type);
				}
				return defaultTypeIsString?String.class:Object.class;
			case Types.OTHER:         // 1111
				// postgresql: row, refcursor
				return Object.class;
			default:
				if(clobTypeIsString && type==Types.CLOB) {  // 2005
					return String.class;
				}
				//convert to Sring? http://www.java2s.com/Code/Java/Database-SQL-JDBC/convertingajavasqlTypesintegervalueintoaprintablename.htm
				if(!unknownSQLTypes.contains(type)) {
					log.warn("unknown SQL type ["+type+"], defaulting to "+(defaultTypeIsString?"String":"Object"));
					unknownSQLTypes.add(type);
				}
				return defaultTypeIsString?String.class:Object.class;
		}
	}
	
	//XXX: remove all dumpRS() ?
	
	public static void dumpRS(ResultSet rs) throws SQLException {
		dumpRS(rs, rs.getMetaData(), System.out);
	}

	public static void dumpRS(ResultSet rs, PrintStream out) throws SQLException {
		dumpRS(rs, rs.getMetaData(), out);
	}

	public static void dumpRS(ResultSet rs, Writer w) throws SQLException, IOException {
		dumpRS(rs, rs.getMetaData(), w);
	}
	
	static StringBuilder dumpRS2StringBuilder(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
		int ncol = rsmd.getColumnCount();
		StringBuilder sb = new StringBuilder();
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
		return sb;
	}
	
	public static void dumpRS(ResultSet rs, ResultSetMetaData rsmd, PrintStream out) throws SQLException {
		StringBuilder sb = dumpRS2StringBuilder(rs, rsmd);
		out.println(sb.toString());
	}

	public static void dumpRS(ResultSet rs, ResultSetMetaData rsmd, Writer w) throws SQLException, IOException {
		StringBuilder sb = dumpRS2StringBuilder(rs, rsmd);
		w.write(sb.toString());
	}
	
	public static String getColumnNames(ResultSetMetaData md) throws SQLException {
		return Utils.join(getColumnNamesAsList(md), ", ");
	}
	
	public static List<String> getColumnNamesAsList(ResultSetMetaData md) throws SQLException {
		int numCol = md.getColumnCount();
		List<String> lsColNames = new ArrayList<String>();
		//List<Class> lsColTypes = new ArrayList<Class>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnLabel(i+1));
		}
		//for(int i=0;i<numCol;i++) {
		//	lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1)));
		//}
		return lsColNames;
	}
	
	static List<String> getColumnValues(ResultSet rs, String colName) throws SQLException {
		List<String> colvals = new ArrayList<String>();
		while(rs.next()) {
			colvals.add(rs.getString(colName));
		}
		return colvals;
	}

	public static List<String> getSchemaNames(DatabaseMetaData dbmd) throws SQLException {
		List<String> ret = null;
		ResultSet rsSchemas = dbmd.getSchemas();
		ret = getColumnValues(rsSchemas, COL_TABLE_SCHEM);
		if(ret.size()==0) { //XXX: remove?
			log.info("no schemas found, getting schemas from catalog names...");
			ResultSet rsCatalogs = dbmd.getCatalogs();
			ret = getColumnValues(rsCatalogs, COL_TABLE_CAT);
			if(ret.size()==0) {
				ret.add("");
			}
		}
		log.debug("schemas: "+ret);
		return ret;
	}

	public static List<String> getCatalogNames(DatabaseMetaData dbmd) throws SQLException {
		List<String> ret = null;
		ResultSet rsCatalogs = dbmd.getCatalogs();
		try {
			ret = getColumnValues(rsCatalogs, COL_TABLE_CAT);
		}
		catch(SQLException e) {
			log.warn("getCatalogNames: can't get column '"+COL_TABLE_CAT+"': "+e);
			log.info("getCatalogs's columns: "+SQLUtils.getColumnNames(rsCatalogs.getMetaData()));
			throw e;
		}
		//log.debug("catalogs: "+ret);
		return ret;
	}
	
	//XXX: props for setting pk(i)NamePatterns?
	public static final String pkNamePattern = "${tablename}_pk";
	public static final String pkiNamePattern = "${tablename}_pki";
	
	public static String newNameFromTableName(String tableName, String pattern) {
		return pattern.replaceAll("\\$\\{tablename\\}", Matcher.quoteReplacement(tableName) );
	}
	
	/*
	 * see:
	 * http://docs.oracle.com/javase/tutorial/jdbc/basics/sqlexception.html
	 * http://docs.oracle.com/javase/6/docs/api/java/sql/SQLSyntaxErrorException.html
	 */
	public static void logWarnings(SQLWarning warning, Log logger)
			throws SQLException {
		/*while (warning != null) {
			logger.debug("SQLWarning: message: " + warning.getMessage()
					+"; state: " + warning.getSQLState()
					+"; error code: "+warning.getErrorCode());
			warning = warning.getNextWarning();
		}*/
		List<String> warnings = getLogWarnings(warning);
		for(String s: warnings) {
			logger.debug(s);
		}
	}

	public static void logWarningsInfo(SQLWarning warning, Log logger)
			throws SQLException {
		List<String> warnings = getLogWarnings(warning);
		for(String s: warnings) {
			logger.info(s);
		}
	}
	
	public static List<String> getLogWarnings(SQLWarning warning) throws SQLException {
		List<String> ret = new ArrayList<String>();
		while (warning != null) {
			ret.add("SQLWarning: message: " + warning.getMessage()
					+"; state: " + warning.getSQLState()
					+"; error code: "+warning.getErrorCode());
			warning = warning.getNextWarning();
		}
		return ret;
	}
	
	public static void xtraLogSQLException(SQLException se, Log logger) {
		logger.info("SQLException: state: "+se.getSQLState()+" ; errorCode: "+se.getErrorCode());
		if(se.iterator()!=null) {
			Iterator<Throwable> it = se.iterator();
			while(it.hasNext()) {
				Throwable t = it.next();
				logger.info("inner SQLException: "+StringUtils.exceptionTrimmed(t));
			}
		}
		se = se.getNextException();
		while(se!=null) {
			logger.info("next SQLException: "+se);
			se = se.getNextException();
		} 
	}
	
	public static String getTypeName(int type) throws IllegalArgumentException, IllegalAccessException {
		Field[] fields = Types.class.getDeclaredFields();
		for(Field f: fields) {
			int v = f.getInt(null);
			if(type==v) {
				return f.getName();
			}
		}
		return null;
	}
	
	public static int getSqlTypeFromClass(Class<?> clazz) {
		if(clazz==null) { return Types.VARCHAR; }
		
		if(clazz.equals(String.class)) {
			return Types.VARCHAR;
		}
		if(clazz.equals(Integer.class) ||
			clazz.equals(Integer.TYPE) ||
			clazz.equals(Long.class) ||
			clazz.equals(Long.TYPE)) {
			return Types.INTEGER;
		}
		if(clazz.equals(Double.class) ||
			clazz.equals(Float.class) ||
			clazz.equals(Double.TYPE) ||
			clazz.equals(Float.TYPE)) {
			return Types.DOUBLE;
		}
		if(clazz.equals(Date.class) ||
			clazz.equals(Timestamp.class)) {
			return Types.TIMESTAMP;
		}
		if(clazz.equals(Boolean.class) ||
			clazz.equals(Boolean.TYPE)) {
			return Types.BOOLEAN;
		}
		if(clazz.isArray() || Collection.class.isAssignableFrom(clazz)) {
			return Types.ARRAY;
		}
		if(clazz.isEnum()) {
			//log.info("enum type: "+clazz.getName());
			return Types.VARCHAR;
		}
		if(DBType.class.isAssignableFrom(clazz)) {
			//log.info("DBType type: "+clazz.getName());
			return Types.VARCHAR;
		}
		/*if(clazz.equals(Class.class)) {
			log.info("class type: "+clazz.getName());
			return Types.VARCHAR;
		}*/
		
		log.warn("unknown class: "+clazz.getName()+" [defaulting to VARCHAR]");
		return Types.VARCHAR;
	}

}
