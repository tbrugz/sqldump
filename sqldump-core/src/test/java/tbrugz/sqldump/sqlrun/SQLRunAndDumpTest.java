package tbrugz.sqldump.sqlrun;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Assume;
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
	public String singleUseDbPath = "mem:";
	
	final static boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

	/*Connection setupConnection(String prefix, Properties prop) throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = SQLUtils.ConnectionUtil.initDBConnection(prefix, prop);
		return conn;
	}*/
	
	public static void setupModel(Connection conn) throws Exception {
		String[] vmparamsRun = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				};
		SQLRun sqlr = new SQLRun();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(NULL_PARAMS, p, conn);
	}
		
	static void execSqlRun(String[] vmparams) throws Exception {
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
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
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src/test/resources/tbrugz/sqldump/sqlrun/emp.csv",
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
		sqlr.doMain(null, p);
		
		String[] vmparamsDump = {
					"-Dsqldump.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.processingclasses=DataDump, JAXBSchemaXMLSerializer, SchemaModelScriptDumper",
					"-Dsqldump.schemadump.outputfilepattern=work/output/SQLRunAndDumpTest/dbobjects.sql",
					"-Dsqldump.schemadump.quoteallsqlidentifiers=true",
					"-Dsqldump.schemadump.dumpdropstatements=true",
					"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
					"-Dsqldump.datadump.outfilepattern=target/work/output/SQLRunAndDumpTest/data_[tablename].[syntaxfileext]",
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
		sqld.doMain(null, p);
		
		String csvDept = IOUtil.readFromFilename("target/work/output/SQLRunAndDumpTest/data_DEPT.csv");
		String expected = "ID,NAME,PARENT_ID\r\n0,CEO,0\r\n1,HR,0\r\n2,Engineering,0\r\n";
		Assert.assertEquals(expected, csvDept);
		
		String sqlEmp = IOUtil.readFromFilename("target/work/output/SQLRunAndDumpTest/data_EMP.sql");
		expected = "insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);";
		Assert.assertEquals(expected, sqlEmp.substring(0, expected.length()));
	}

	@Test
	public void doRunAndAssertModelEqOk() throws Exception {
		String mydbpath = dbpath+"-assert1;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
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
		sqlr.doMain(null, p);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelEqError() throws Exception {
		String mydbpath = dbpath+"-assert2;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
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
		sqlr.doMain(null, p);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelGtError() throws Exception {
		String mydbpath = dbpath+"-assert3;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
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
		sqlr.doMain(null, p);
	}

	@Test(expected = ProcessingException.class)
	public void doRunAndAssertModelLtError() throws Exception {
		String mydbpath = dbpath+"-assert4;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
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
		sqlr.doMain(null, p);
	}

	@Test
	public void doImportCsvWithMappedCol() throws Exception {
		String mydbpath = dbpath+"-csv-mapped;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.05.statement=create table ins_dept (ID integer, NAME varchar, DBLID integer)",
				"-Dsqlrun.exec.10.import=csv",
				"-Dsqlrun.exec.10.insertsql=insert into ins_dept (id, name, dblid) values (${0}, ${1}, cast(${0} as integer) + cast(${0} as integer))",
				"-Dsqlrun.exec.10.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.10.skipnlines=1",
				"-Dsqlrun.assert.20.sql=select * from ins_dept",
				"-Dsqlrun.assert.20.row-count.eq=3",
				"-Dsqlrun.assert.25.sql=select * from ins_dept where DBLID = 4",
				"-Dsqlrun.assert.25.row-count.eq=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
	}

	@Test
	public void testAssert() throws Exception {
		String query = "select 1 as a, 1 as b, 1 as c\n" +
				"union all\n" +
				"select 1, 2, 3";
		String[] vmparams = {
				"-Dsqlrun.assert.10.sql="+query,
				"-Dsqlrun.assert.10.row-count.eq=2",
				"-Dsqlrun.dburl=jdbc:h2:"+singleUseDbPath,
				};
		execSqlRun(vmparams);
	}

	@Test(expected = ProcessingException.class)
	public void testAssertError() throws Exception {
		String query = "select 1 as a, 1 as b, 1 as c\n" +
				"union all\n" +
				"select 1, 2, 3";
		String[] vmparams = {
				"-Dsqlrun.assert.10.sql="+query,
				"-Dsqlrun.assert.10.row-count.eq=1",
				"-Dsqlrun.dburl=jdbc:h2:"+singleUseDbPath,
				};
		execSqlRun(vmparams);
	}

	@Test
	public void testAssertRowColOk() throws Exception {
		String query = "select 1 as a, 1 as b, 1 as c\n" +
				"union all\n" +
				"select 1, 2, 3";
		String[] vmparams = {
				"-Dsqlrun.assert.10.sql="+query,
				"-Dsqlrun.assert.10.row@1.col@A.eq=1",
				"-Dsqlrun.dburl=jdbc:h2:"+singleUseDbPath,
				};
		execSqlRun(vmparams);
	}

	@Test(expected = ProcessingException.class)
	public void testAssertRowColError() throws Exception {
		String query = "select 1 as a, 1 as b, 1 as c\n" +
				"union all\n" +
				"select 1, 2, 3";
		String[] vmparams = {
				"-Dsqlrun.assert.10.sql="+query,
				"-Dsqlrun.assert.10.row@2.col@B.eq=1",
				"-Dsqlrun.dburl=jdbc:h2:"+singleUseDbPath,
				};
		execSqlRun(vmparams);
	}

	@Test
	public void doRunImportFiles() throws Exception {
		Assume.assumeFalse(isWindows);

		String mydbpath = dbpath+"-importfiles;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=dept",
				"-Dsqlrun.exec.05.importfiles.glob=../test/data/**/dept*.csv",
				//"-Dsqlrun.exec.05.importfiles.glob="+System.getProperty("user.dir")+"/test/data/**/dept*.csv", // absolute path
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		String[] vmparamsDump = {
					"-Dsqldump.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.processingclasses=DataDump",
					"-Dsqldump.datadump.dumpsyntaxes=csv",
					//"-Dsqldump.datadump.csv.columnnamesheader=false",
					"-Dsqldump.datadump.outfilepattern=../work/output/SQLRunAndDumpTest/data-import_[tablename].[syntaxfileext]",
					"-Dsqldump.datadump.writebom=false",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+mydbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		SQLDump sqld = new SQLDump();
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(null, p);
		
		String csvDept = IOUtil.readFromFilename("../work/output/SQLRunAndDumpTest/data-import_DEPT.csv");
		//System.out.println(csvDept);
		int count = TestUtil.countLines(csvDept);
		Assert.assertEquals(5+1, count);
	}

	@Test
	public void doRunImportXlsFiles() throws Exception {
		Assume.assumeFalse(isWindows);

		String mydbpath = dbpath+"-importfiles-xls;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.05.import=xls",
				"-Dsqlrun.exec.05.inserttable=dept",
				"-Dsqlrun.exec.05.importfiles.glob=../test/data/**/dept*.xlsx",
				//"-Dsqlrun.exec.05.importfiles.glob="+System.getProperty("user.dir")+"/test/data/**/dept*.csv", // absolute path
				//"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+mydbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		String[] vmparamsDump = {
					"-Dsqldump.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.processingclasses=DataDump",
					"-Dsqldump.datadump.dumpsyntaxes=csv",
					//"-Dsqldump.datadump.csv.columnnamesheader=false",
					"-Dsqldump.datadump.outfilepattern=../work/output/SQLRunAndDumpTest/data-import-xls_[tablename].[syntaxfileext]",
					"-Dsqldump.datadump.writebom=false",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+mydbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		SQLDump sqld = new SQLDump();
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(null, p);
		
		String csvDept = IOUtil.readFromFilename("../work/output/SQLRunAndDumpTest/data-import-xls_DEPT.csv");
		//System.out.println(csvDept);
		int count = TestUtil.countLines(csvDept);
		Assert.assertEquals(6+1, count);
	}
	
}
