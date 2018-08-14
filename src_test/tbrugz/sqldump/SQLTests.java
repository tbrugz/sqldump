package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.FK;
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
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures("oracle");
		dbmd = feat.getMetadataDecorator(dbmd);
		
		//DBMSFeatures featz = new OracleFeaturesLite();
		//featz.setId("oracle");
		//dbmd = featz.getMetadataDecorator(dbmd);
		
		log.info("dbmd: "+dbmd);
		log.info("conn info: "+dbmd.getUserName()+" @ "+dbmd.getURL());

		String schema = "schema";
		String relation = "relation";
		long initTime = System.currentTimeMillis();

		//log.info("test: catalogs...");
		//SQLUtils.dumpRS(dbmd.getCatalogs());

		//log.info("test: table types...");
		//SQLUtils.dumpRS(dbmd.getTableTypes());

		//log.info("test: tables...");
		//SQLUtils.dumpRS(dbmd.getTables(null, schema, null, null));

		//log.info("test: columns...");
		//SQLUtils.dumpRS(dbmd.getColumns(null, schema, relation, null));
		
		//log.info("test: pks...");
		//SQLUtils.dumpRS(dbmd.getPrimaryKeys(null, schema, relation));
		
		//log.info("test: fks...");
		//SQLUtils.dumpRS(dbmd.getImportedKeys(null, schema, relation));

		//log.info("test: exported fks...");
		//SQLUtils.dumpRS(dbmd.getExportedKeys(null, schema, null));
		//!exportedkeys
		
		//log.info("test: grants...");
		//SQLUtils.dumpRS(dbmd.getTablePrivileges(null, schema, relation));
		
		//log.info("test: indexes...");
		//SQLUtils.dumpRS(dbmd.getIndexInfo(null, schema, relation, false, false));

		//log.info("test: getProcedures...");
		//SQLUtils.dumpRS(dbmd.getProcedures(null, schema, null));

		//log.info("test: getProcedureColumns...");
		//SQLUtils.dumpRS(dbmd.getProcedureColumns(null, schema, null, null));
		
		//log.info("test: getFunctions...");
		//SQLUtils.dumpRS(dbmd.getFunctions(null, schema, null));

		//log.info("test: getFunctionColumns...");
		//SQLUtils.dumpRS(dbmd.getFunctionColumns(null, schema, null, null));
		
		//String sql = "select * from xxx";
		//Statement st = conn.createStatement();
		//ResultSet rs = st.executeQuery(sql);
		//SQLUtils.dumpRS(rs);
		
		dumpFk();
		
		log.info("elapsed: "+(System.currentTimeMillis()-initTime)+"ms");
		//Thread.sleep(60000);
	}
	
	static void testFeatures(Connection conn) throws SQLException {
		DBMSFeatures feats = DBMSResources.instance().getSpecificFeatures(conn.getMetaData());
		
		String sql = "select * from table order by column";
		
		//conn.createStatement().execute("create table xyz (col1 integer, col2 varchar)");
		//String sql = "select * from xyz order by col1";
		ResultSet rs = feats.explainPlan(sql, null, conn);
		//conn.createStatement().execute("drop table xyz");
		
		SQLUtils.dumpRS(rs);
	}
	
	static int countRsRows(ResultSet rs) throws SQLException {
		int count = 0;
		while(rs.next()) {
			count++;
		}
		return count;
	}

	@Override
	public void process() {
		try {
			tests(conn);
			//testFeatures(conn);
		}
		catch(Exception e) {
			log.warn("Exception: "+e.getMessage(), e);
		}
	}
	
	static void dumpFk() {
		FK fk = new FK();
		fk.setPkTable("pktable");
		fk.setFkTable("fktable");
		fk.setPkColumns(Arrays.asList(new String[]{"p1", "p2"}));
		fk.setFkColumns(Arrays.asList(new String[]{"f1", "f2"}));
		//fk.setName("fkname");
		
		String s = fk.getDefinition(true);
		System.out.println("fk: "+s);
		
		String alt = fk.fkScriptWithAlterTable(true, true);
		System.out.println("alt: "+alt);
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, NamingException {
		dumpFk();
		
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
