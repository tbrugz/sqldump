package tbrugz.sqlmigrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MigrationDao {

	static final Log log = LogFactory.getLog(MigrationDao.class);

	String schemaName;
	String tableName;
	
	public MigrationDao(String schemaName, String tableName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	public List<Migration> listMigrations(Connection conn) throws SQLException {
		List<Migration> ret = new ArrayList<>();
		String sql = "select script, version, crc32 " + 
				"from "+ (schemaName!=null?schemaName+".":"") + tableName + " " +
				"order by version";
		log.debug("sql: "+sql);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		while(rs.next()) {
			Migration m = new Migration(rs.getString(1), rs.getString(2), rs.getLong(3));
			ret.add(m);
		}
		return ret;
	}
	
}
