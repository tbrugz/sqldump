package tbrugz.sqldump.sqlrun;

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
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.IOUtil;

public class SQLRunAndDumpTest {
	
	public static final String[] NULL_PARAMS = null;

	public String dbpath = "mem:SQLRunAndDumpTest";
	
	/*Connection setupConnection(String prefix, Properties prop) throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = SQLUtils.ConnectionUtil.initDBConnection(prefix, prop);
		return conn;
	}*/
	
	public static void setupModel(Connection conn) throws Exception {
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
		
		testForTables(conn);
		
		System.out.println("conn: "+conn);
		System.out.println("conn.isClosed(): "+conn.isClosed());
		
		String[] vmparamsDump = {
					"-Dsqldump.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.processingclasses=JAXBSchemaXMLSerializer, JSONSchemaSerializer",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/empdept.jaxb.xml",
					"-Dsqldump.jsonserialization.outfile=work/output/empdept.json",
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
	
	void testForTables(Connection conn) throws ClassNotFoundException, SQLException, NamingException {
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
		schemaGrabber.setProperties(new Properties());
		schemaGrabber.setConnection(conn);
		SchemaModel smOrig = schemaGrabber.grabSchema();
		System.out.println("smOrig: "+smOrig);
		System.out.println("smOrig.getTables(): "+smOrig.getTables());
		Assert.assertEquals("should have grabbed 3 tables", 3, smOrig.getTables().size());
	}

	@Test
	public void doRunImportAndDumpModel() throws Exception {
		String mydbpath = dbpath+"-import;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src_test/tbrugz/sqldump/sqlrun/emp.csv",
				//"-Dsqlrun.exec.05.emptystringasnull=true",
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		//setSystemProperties(vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
		
		String[] vmparamsDump = {
					"-Dsqldump.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.processingclasses=DataDump, JAXBSchemaXMLSerializer, SchemaModelScriptDumper",
					"-Dsqldump.schemadump.outputfilepattern=work/output/SQLRunAndDumpTest/dbobjects.sql",
					"-Dsqldump.schemadump.quoteallsqlidentifiers=true",
					"-Dsqldump.schemadump.dumpdropstatements=true",
					"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
					"-Dsqldump.datadump.outfilepattern=work/output/SQLRunAndDumpTest/data_[tablename].[syntaxfileext]",
					"-Dsqldump.datadump.writebom=false",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/SQLRunAndDumpTest/empdept.jaxb.xml",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+mydbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		SQLDump sqld = new SQLDump();
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		//setSystemProperties(vmparamsDump);
		sqld.doMain(null, p, null);
		
		String csvDept = IOUtil.readFromFilename("work/output/SQLRunAndDumpTest/data_DEPT.csv");
		String expected = "ID,NAME,PARENT_ID\r\n0,CEO,0\r\n1,HR,0\r\n2,Engineering,0\r\n";
		Assert.assertEquals(expected, csvDept);
		
		String sqlEmp = IOUtil.readFromFilename("work/output/SQLRunAndDumpTest/data_EMP.sql");
		expected = "insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);";
		Assert.assertEquals(expected, sqlEmp.substring(0, expected.length()));
	}

	@Test
	public void doRunAndAssertModelEqOk() throws Exception {
		String mydbpath = dbpath+"-assert1;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.assert.03.sql=select * from dept",
				"-Dsqlrun.assert.03.row-count.eq=3",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelEqError() throws Exception {
		String mydbpath = dbpath+"-assert2;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.assert.03.sql=select * from emp",
				"-Dsqlrun.assert.03.row-count.eq=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelGtError() throws Exception {
		String mydbpath = dbpath+"-assert3;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.assert.03.sql=select * from dept",
				"-Dsqlrun.assert.03.row-count.gt=3",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelLtError() throws Exception {
		String mydbpath = dbpath+"-assert4;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.assert.03.sql=select * from dept",
				"-Dsqlrun.assert.03.row-count.lt=3",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
	}
	
}
