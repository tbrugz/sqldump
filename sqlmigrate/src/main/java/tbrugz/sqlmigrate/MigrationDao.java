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

	public List<Migration> listVersionedMigrations(Connection conn) throws SQLException {
		return listMigrations(conn, true);
	}

	public List<Migration> listUnversionedMigrations(Connection conn) throws SQLException {
		return listMigrations(conn, false);
	}
	
	List<Migration> listMigrations(Connection conn, boolean versioned) throws SQLException {
		List<Migration> ret = new ArrayList<>();
		String sql = "select " + MIG_COLUMNS_STR + " " +
				"from "+ (schemaName!=null?schemaName+".":"") + tableName + " " +
				"where version " + (versioned?"is not null ":"is null ") +
				"order by " + (versioned?"version":"script");
		log.debug("list.sql: "+sql);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		while(rs.next()) {
			Migration m = new Migration(rs.getString(1), rs.getString(2), rs.getLong(3));
			ret.add(m);
		}
		return ret;
	}

	public int save(Migration mig, Connection conn) throws SQLException {
		String sql = DmlUtils.createInsert(schemaName, tableName, Arrays.asList(MIG_COLUMNS));
		log.debug("save.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		if(mig.getVersion()!=null) {
			st.setString(1, mig.getVersion().asVersionString());
		}
		else {
			st.setObject(1, null);
		}
		st.setString(2, mig.getScript());
		if(mig.getCrc32()!=null) {
			st.setLong(3, mig.getCrc32());
		}
		else {
			st.setObject(3, null);
		}
		st.execute();
		return st.getUpdateCount();
	}

	public int updateChecksumByScript(Migration mig, Long crc32, Connection conn) throws SQLException {
		String sql = DmlUtils.createUpdate(schemaName, tableName, Arrays.asList(new String[]{"crc32"}), Arrays.asList(new String[]{"script"}));
		log.debug("updateChecksumByScript.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setLong(1, crc32);
		st.setString(2, mig.getScript());
		st.execute();
		return st.getUpdateCount();
	}
	
	static final String[] MIG_VERSION = { "version" };
	static final String[] MIG_SCRIPT = { "script" };
	
	public int removeByVersion(Migration mig, Connection conn) throws SQLException {
		String sql = DmlUtils.createDelete(schemaName, tableName, Arrays.asList(MIG_VERSION));
		log.debug("removeByVersion.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, mig.getVersion().asVersionString());
		st.execute();
		return st.getUpdateCount();
	}

	public int removeByScript(Migration mig, Connection conn) throws SQLException {
		String sql = DmlUtils.createDelete(schemaName, tableName, Arrays.asList(MIG_SCRIPT));
		log.debug("removeByScript.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, mig.getScript());
		st.execute();
		return st.getUpdateCount();
	}
	
	public int removeAllUnversioned(Connection conn) throws SQLException {
		String sql = DmlUtils.createDelete(schemaName, tableName, "version is null");
		log.debug("removeAllUnversioned.sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.execute();
		return st.getUpdateCount();
	}
	
}
