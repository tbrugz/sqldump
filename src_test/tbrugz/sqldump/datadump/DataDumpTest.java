package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

//http://docs.oracle.com/javase/1.4.2/docs/guide/intl/encoding.doc.html
public class DataDumpTest {

	static Log log = LogFactory.getLog(DataDumpTest.class);

	static String DIR_OUT = "work/output/DataDumpTest/";
	static String dbpath = "mem:DataDumpTest;DB_CLOSE_DELAY=-1";
	static String[] params = {};
	
	@Test
	public void testEncoding() throws IOException {
		//DataDump dd = new DataDump();
		Map<String, Writer> map = new HashMap<String, Writer>();
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-utf8.txt", "", DataDumpUtils.CHARSET_UTF8, null, false);
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-iso8859.txt", "", DataDumpUtils.CHARSET_ISO_8859_1, null, false); //ISO8859_1
		for(String s: map.keySet()) {
			map.get(s).write("Pôrto Alégre");
			map.get(s).close();
		}
	}
	
	@BeforeClass
	public static void setupDB() throws Exception {
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
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(params, p, null);
	}
	
	@Before
	public void beforeTest() {
		Utils.deleteDirRegularContents(DIR_OUT);
	}
	
	@Test
	public void dump1() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				//"-Dsqldump.schemadump.dumpclasses=SchemaModelScriptDumper",
				//"-Dsqldump.mainoutputfilepattern=work/output/dbobjects.sql",
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		SQLDump sqld = new SQLDump();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(params, p, null);
		
		String csvDept = IOUtil.readFromFilename(DIR_OUT+"/data_DEPT.csv");
		String expected = "ID,NAME,PARENT_ID\n0,CEO,0\n1,HR,0\n2,Engineering,0\n";
		Assert.assertEquals(expected, csvDept);
		
		String sqlEmp = IOUtil.readFromFilename(DIR_OUT+"/data_EMP.sql");
		expected = "insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);";
		Assert.assertTrue(sqlEmp.startsWith(expected));
	}

	@Test
	public void dumpPartitioned() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=_[col:SUPERVISOR_ID]",
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n";
		Assert.assertEquals(expected, csvEmpS2);
	}

	@Test
	public void dumpPartitioned2Patterns() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=p2|p2_[col:SUPERVISOR_ID]", //this line changes it!
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n";
		Assert.assertEquals(expected, csvEmpS2);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpAll);
	}
	
	@Test
	public void dumpWithRowNumber() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.datadump.dumpsyntaxes=rncsv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,1,john,1,1,2000\n"+"1,2,mary,2,2,2000\n"+"2,3,jane,2,2,1000\n"+"3,4,lucas,2,2,1200\n"+"4,5,wilson,1,1,1000\n"+"5\n";
		Assert.assertEquals(expected, csvEmpAll);
	}
	
	@Test
	public void dumpPartitionedWithRowNumber() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=p2_[col:SUPERVISOR_ID]",
				"-Dsqldump.datadump.dumpsyntaxes=rncsv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,1,john,1,1,2000\n"+"1,5,wilson,1,1,1000\n"+"2\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.rn.csv");
		expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,2,mary,2,2,2000\n"+"1,3,jane,2,2,1000\n"+"2,4,lucas,2,2,1200\n"+"3\n";
		Assert.assertEquals(expected, csvEmpS2);
	}
	
}
