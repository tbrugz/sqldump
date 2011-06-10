package tbrugz.sqldump;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
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
			sbTmp.append(enclosing+rs.getString(i)+enclosing);
			sbTmp.append(delimiter);
		}
		return sbTmp.toString();
	}

	public static List<String> getRowListFromRS(ResultSet rs, int numCol) throws SQLException {
		List<String> ls = new ArrayList<String>();
		for(int i=1;i<=numCol;i++) {
			ls.add(rs.getString(i));
		}
		return ls;
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
