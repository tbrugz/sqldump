package tbrugz.sqldump.util;

import java.io.PrintStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SQLUtils {
	
	static final Log log = LogFactory.getLog(SQLUtils.class);
	
	public static String getRowFromRS(ResultSet rs, int numCol, String table) throws SQLException {
		return getRowFromRS(rs, numCol, table, ";");
	}
	
	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter) throws SQLException {
		return getRowFromRS(rs, numCol, table, delimiter, ""); //"'"?
	}

	public static String getRowFromRS(ResultSet rs, int numCol, String table, String delimiter, String enclosing) throws SQLException {
		StringBuffer sbTmp = new StringBuffer();
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
	
	//static String errorGettingValueValue = "###";
	static final String errorGettingValueValue = null;
	static int errorGettingValueWarnCount = 0;
	static final int errorGettingValueWarnMaxCount = 10;
	
	static boolean is1stRow = true;
	static Map<Class<?>, Class<?>> colTypeMapper = null;
	
	public static void setupForNewQuery(int numCol) {
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
				//XXX value = rs.getDate(i) ?
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
			
			}
			catch(SQLException e) {
				value = errorGettingValueValue;
				errorGettingValueWarnCount++;
				if( (errorGettingValueWarnCount <= errorGettingValueWarnMaxCount) ) {  //|| is1stRow
					log.warn("error getting value [col="+i+", type="+coltype.getSimpleName()
							+", count = "+errorGettingValueWarnCount
							+(errorGettingValueWarnCount==errorGettingValueWarnMaxCount?"; max warn count ["+errorGettingValueWarnMaxCount+"] reached":"")
							+"]: "+e);
					log.debug("error getting value [col="+i+"]", e);
				}
			}
			
			ls.add(value);
		}
		is1stRow = false;
		return ls;
	}
	
	static Set<Integer> unknownSQLTypes = new HashSet<Integer>(); 
	//TODOne: Date class type for dump?
	//XXX: add BigDecimal (for NUMERIC & DECIMAL), TIMESTAMP (for TIMESTAMP)
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
	
	//XXX: remove all dumpRS() ?
	
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
		List<String> ret = null;
		ResultSet rsSchemas = dbmd.getSchemas();
		ret = getColumnValues(rsSchemas, "table_schem");
		if(ret.size()==0) { //XXX: remove?
			log.info("no schemas found, getting schemas from catalog names...");
			ResultSet rsCatalogs = dbmd.getCatalogs();
			ret = getColumnValues(rsCatalogs, "table_cat");
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
		ret = getColumnValues(rsCatalogs, "table_cat");
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
		while (warning != null) {
			logger.debug("SQLWarning: message: " + warning.getMessage()
					+"; state: " + warning.getSQLState()
					+"; error code: "+warning.getErrorCode());
			warning = warning.getNextWarning();
		}
	}
	
	public static void xtraLogSQLException(SQLException se, Log logger) {
		logger.info("SQLException: state: "+se.getSQLState()+" ; errorCode: "+se.getErrorCode());
		if(se.iterator()!=null) {
			Iterator<Throwable> it = se.iterator();
			while(it.hasNext()) {
				Throwable t = it.next();
				logger.info("inner SQLException: "+t);
			}
		}
		se = se.getNextException();
		while(se!=null) {
			logger.info("next SQLException: "+se);
			se = se.getNextException();
		} 
	}

}
