package tbrugz.sqlmigrate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;
import tbrugz.sqlmigrate.util.DmlUtils;

public class MigrationDao {

	static final Log log = LogFactory.getLog(MigrationDao.class);

	static final String[] MIG_COLUMNS = { "version", "script", "crc32" };
	static final String MIG_COLUMNS_STR = Utils.join(Arrays.asList(MIG_COLUMNS), ", ");

	String schemaName;
	String tableName;
	
	public MigrationDao(String schemaName, String tableName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	public List<Migration> listMigrations(Connection conn) throws SQLException {
		List<Migration> ret = new ArrayList<>();
		String sql = "select " + MIG_COLUMNS_STR + " " +
				"from "+ (schemaName!=null?schemaName+".":"") + tableName + " " +
				"order by version";
		log.debug("list.sql: "+sql);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		while(rs.next()) {
			Migration m = new Migration(rs.getString(1), rs.getString(2), rs.getLong(3));
			ret.add(m);
		}
		return ret;
	}

	public void save(Migration mig, Connection conn) throws SQLException {
		String sql = DmlUtils.createInsert(schemaName, tableName, Arrays.asList(MIG_COLUMNS));
		log.debug("save.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, mig.getVersion().asVersionString());
		st.setString(2, mig.script);
		if(mig.crc32!=null) {
			st.setLong(3, mig.crc32);
		}
		else {
			st.setObject(3, null);
		}
		st.execute();
	}

	static final String[] MIG_VERSION = { "version" };
	
	public int removeByVersion(Migration mig, Connection conn) throws SQLException {
		String sql = DmlUtils.createDelete(schemaName, tableName, Arrays.asList(MIG_VERSION));
		log.debug("remove.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, mig.getVersion().asVersionString());
		st.execute();
		return st.getUpdateCount();
	}
	
}
