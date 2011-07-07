package tbrugz.sqldump;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SQLUtils {

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

	public static List getRowObjectListFromRS(ResultSet rs, List<Class> colTypes, int numCol) throws SQLException {
		List ls = new ArrayList();
		for(int i=1;i<=numCol;i++) {
			Object value = null;
			Class coltype = colTypes.get(i-1);
			if(coltype.equals(String.class)) {
				value = rs.getString(i);
			}
			else if(coltype.equals(Integer.class)) {
				value = rs.getInt(i);
			}
			else if(coltype.equals(Double.class)) {
				value = rs.getDouble(i);
			}
			else if(coltype.equals(Date.class)) {
				//TODO: how to Date value?
				value = rs.getString(i);
				//value = rs.getDate(i);
			}
			else {
				value = rs.getString(i);
			}
			ls.add(value);
		}
		return ls;
	}

	//TODO: Date class type for dump?
	static Class getClassFromSqlType(int type) {
		//log.debug("type: "+type);
		switch(type) {
			case Types.TINYINT: 
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.DECIMAL: //??
			case Types.NUMERIC: //??
				return Integer.class;
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
				return Double.class;
			case Types.DATE:
			case Types.TIMESTAMP:
				return Date.class;
			case Types.CHAR:
			case Types.VARCHAR:
			default:
				return String.class;
		}
	}
	
	static void dumpRS(ResultSet rs) throws SQLException {
		dumpRS(rs, rs.getMetaData());
	}

	static void dumpRS(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
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
		System.out.println("\n"+sb.toString()+"\n");
	}
	
}
