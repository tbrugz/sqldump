package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;

public class SQLTests extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(SQLTests.class);
	
	public static void tests(Connection conn) {
		try {
			testsInternal(conn);
		} catch (Exception e) {
			log.warn("SQL error: "+e);
			log.info("stack...",e);
		}
	}

	@SuppressWarnings("unused")
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

		//log.info("test: exported fks...");
		//SQLUtils.dumpRS(dbmd.getExportedKeys(null, "schema", null));
		//!exportedkeys
		
		//log.info("test: grants...");
		//SQLUtils.dumpRS(dbmd.getTablePrivileges(null, "schema", "table"));
		
		//log.info("test: indexes...");
		//SQLUtils.dumpRS(dbmd.getIndexInfo(null, "schema", "table", false, false));
		
		//String sql = "select * from xxx";
		//Statement st = conn.createStatement();
		//ResultSet rs = st.executeQuery(sql);
		//SQLUtils.dumpRS(rs);
	}
	
	@SuppressWarnings("deprecation")
	static void testFeatures(Connection conn) throws SQLException {
		DBMSResources.instance().updateMetaData(conn.getMetaData(), true);
		DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		
		String sql = "SELECT * from table order by column";
		
		//conn.createStatement().execute("create table xyz (col1 integer, col2 varchar)");
		//String sql = "SELECT * from xyz order by col1";
		
		ResultSet rs = feats.explainPlan(sql, conn);
		SQLUtils.dumpRS(rs);
	}

	@Override
	public void process() {
		try {
			//tests(conn);
			testFeatures(conn);
		}
		catch(SQLException e) {
			log.warn("Exception: "+e.getMessage(), e);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, NamingException {
		if(args.length!=2) {
			String message = "should have 2 arguments: properties file & connection properties prefix";
			log.warn(message);
			throw new RuntimeException(message);
		}
		SQLTests st = new SQLTests();
		st.doMain(args[0], args[1]);
	}
	
	void doMain(String fileName, String connPrefix) throws ClassNotFoundException, SQLException, NamingException, IOException {
		log.info("loading props: "+fileName);
		File f = new File(fileName);

		ParametrizedProperties p = new ParametrizedProperties();
		p.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, f.getParent());
		p.load(new FileInputStream(f));
		log.info("initing conn: "+connPrefix);
		Connection conn = ConnectionUtil.initDBConnection(connPrefix, p);
		
		setConnection(conn);
		process();
	}
	
}
