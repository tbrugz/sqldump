package tbrugz.sqldump;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLUtils {

	static StringBuffer sbTmp = new StringBuffer();

	public static String getRowFromRS(ResultSet rs, int numCol, String table) throws SQLException {
		sbTmp.setLength(0);
		for(int i=1;i<=numCol;i++) {
			sbTmp.append(rs.getString(i));
			sbTmp.append(";");
		}
		return sbTmp.toString();
	}

}
