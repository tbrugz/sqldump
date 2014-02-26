package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractSQLProc;
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

	static void testsInternal(Connection conn) throws Exception {
		log.info("some tests...");
		@SuppressWarnings("unused")
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

	@Override
	public void process() {
		tests(conn);
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
	
	void doMain(String fileName, String connPrefix) throws ClassNotFoundException, SQLException, NamingException, FileNotFoundException, IOException {
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
