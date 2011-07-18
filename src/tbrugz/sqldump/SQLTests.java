package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.apache.log4j.Logger;

/*
 * note: was part of SQLDump.java
 */
public class SQLTests {

	static Logger log = Logger.getLogger(SQLTests.class);
	
	public static void tests(Connection conn) {
		try {
			testsInternal(conn);
		} catch (Exception e) {
			log.warn("SQL error: "+e);
			//e.printStackTrace();
		}
	}

	static void testsInternal(Connection conn) throws Exception {
		log.info("some tests...");
		DatabaseMetaData dbmd = conn.getMetaData();

		//log.info("test: catalogs...");
		//SQLUtils.dumpRS(dbmd.getCatalogs());

		//log.info("test: table types...");
		//SQLUtils.dumpRS(dbmd.getTableTypes());

		//log.info("test: tables...");
		//SQLUtils.dumpRS(dbmd.getTables(null, null, null, null));

		//log.info("test: columns...");
		//SQLUtils.dumpRS(dbmd.getColumns(null, "schema", "table", null));
		
		//log.info("test: fks...");
		//SQLUtils.dumpRS(dbmd.getImportedKeys(null, "schema", "table"));

		//log.info("test: grants...");
		//SQLUtils.dumpRS(dbmd.getTablePrivileges(null, "schema", "table"));
		
		//log.info("test: indexes...");
		//SQLUtils.dumpRS(dbmd.getIndexInfo(null, "schema", "table", false, false));
		
		//String sql = "select * from xxx";
		//Statement st = conn.createStatement();
		//ResultSet rs = st.executeQuery(sql);
		//SQLUtils.dumpRS(rs);
	}
	
}
