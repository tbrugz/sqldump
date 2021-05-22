package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.importers.XlsImporter;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class XlsImportTest {
	
	@Test
	public void doImportXls() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.02.statement=create table ins_xls (ID integer, NAME varchar, SUPERVISOR_ID integer, DEPARTMENT_ID integer, SALARY integer)\n"+
			"";
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}
	
	@Test
	public void doImportXlsx() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.02.statement=create table ins_xls (ID integer, NAME varchar, SUPERVISOR_ID integer, DEPARTMENT_ID integer, SALARY integer)\n"+
			"sqlrun.exec.20.importfile=${basedir}/src_test/tbrugz/sqldump/sqlrun/emp.xlsx\n";
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}

	@Test
	public void doImportXlsWithCreateTable() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.20.do-create-table=true\n"+
			"";
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}

	@Test
	public void doImportXlsxColMapper() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.02.statement=create table ins_xls (ID integer, NAME varchar, DBLSALARY integer)\n"+
			"sqlrun.exec.20.importfile=${basedir}/src_test/tbrugz/sqldump/sqlrun/emp.xlsx\n"+
			"sqlrun.exec.20.insertsql=insert into ins_xls (id, name, dblsalary) values (${0}, ${1}, ${4}+${4})\n"
			;
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}

	@Test
	public void useXslImporter() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		String execId = "1";
		Properties p = new Properties();
		
		p.setProperty("sqlrun.exec."+execId+".inserttable", "ins_xls2");
		p.setProperty("sqlrun.exec."+execId+".do-create-table", "true");
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = new XlsImporter();
		imp.setConnection(conn);
		imp.setExecId(execId);
		imp.setProperties(p);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls2"));
	}

}
