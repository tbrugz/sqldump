package tbrugz.sqldump.sqlrun;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class SQLRunAndDumpTest {
	
	public static final String[] NULL_PARAMS = {};

	public String dbpath = "mem:SQLRunAndDumpTest";
	
	/*Connection setupConnection(String prefix, Properties prop) throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = SQLUtils.ConnectionUtil.initDBConnection(prefix, prop);
		return conn;
	}*/
	
	public static void setupModel(Connection conn) throws ClassNotFoundException, IOException, SQLException, NamingException {
		String[] vmparamsRun = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				};
		SQLRun sqlr = new SQLRun();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(NULL_PARAMS, p, conn);
	}
		
	@Test
	public void doRunAndDumpModel() throws Exception {
		String[] vmparamsRun = {
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		Connection conn = TestUtil.getConn(p, "sqlrun");
		setupModel(conn);
		
		testForTwoTables(conn);
		
		System.out.println("conn: "+conn);
		System.out.println("conn.isClosed(): "+conn.isClosed());
		
		String[] vmparamsDump = {
					"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.schemadump.dumpclasses=JAXBSchemaXMLSerializer",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/empdept.jaxb.xml",
					/*"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+dbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"*/
					};
		SQLDump sqld = new SQLDump();
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(NULL_PARAMS, p, conn);
	}
	
	void testForTwoTables(Connection conn) throws ClassNotFoundException, SQLException, NamingException {
		SchemaModelGrabber schemaGrabber = new JDBCSchemaGrabber();
		/*Properties jdbcPropOrig = new Properties();
		String[] vmparams = {
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		TestUtil.setProperties(jdbcPropOrig, vmparams);
		schemaGrabber.procProperties(jdbcPropOrig);
		schemaGrabber.setConnection(TestUtil.getConn(jdbcPropOrig, "sqldump"));*/
		schemaGrabber.procProperties(new Properties());
		schemaGrabber.setConnection(conn);
		SchemaModel smOrig = schemaGrabber.grabSchema();
		System.out.println("smOrig: "+smOrig);
		System.out.println("smOrig.getTables(): "+smOrig.getTables());
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());
	}

	@Test
	public void doRunImportAndDumpModel() throws Exception {
		SQLDump sqld = new SQLDump();
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src_test/tbrugz/sqldump/sqlrun/emp.csv",
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		String[] params = {};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		//setSystemProperties(vmparams);
		sqld.doMain(params, p, null);
		
		String[] vmparamsDump = {
					"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.schemadump.dumpclasses=JAXBSchemaXMLSerializer, SchemaModelScriptDumper",
					"-Dsqldump.processingclasses=DataDump",
					"-Dsqldump.mainoutputfilepattern=work/output/dbobjects.sql",
					"-Dsqldump.schemadump.dumpdropstatements=true",
					"-Dsqldump.datadump.dumpsyntaxes=insertinto",
					"-Dsqldump.datadump.outfilepattern=work/output/${tablename}.${syntaxfileext}",
					"-Dsqldump.datadump.writebom=false",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/empdept.jaxb.xml",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+dbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		//setSystemProperties(vmparamsDump);
		sqld.doMain(params, p, null);
	}
	
}
