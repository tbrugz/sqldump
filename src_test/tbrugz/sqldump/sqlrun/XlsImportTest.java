package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.importers.ImporterHelper;
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
		conn.close();
	}

	@Test
	public void useXslImporterWithHelper() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls2");
		p.setProperty(".do-create-table", "true");
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = new XlsImporter();
		ImporterHelper.setImporterPlainProperties(imp, p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls2"));
		conn.close();
	}

	@Test
	public void useXslImporterWithHelper2() throws Exception {
		// setup
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		
		// read xlsx
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls2");
		p.setProperty(".do-create-table", "true");
		Importer imp = ImporterHelper.getImporterByFileExt("xlsx", p);
		imp.setConnection(conn);
		imp.importStream(is);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls2"));

		// read xls
		is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xls");
		p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls2");
		imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);
		Assert.assertEquals(10, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls2"));
		
		// close
		conn.close();
	}

	@Test
	public void useXslImporterWithHelperAndLimit() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls2");
		p.setProperty(".do-create-table", "true");
		p.setProperty(Constants.SUFFIX_LIMIT_LINES, "3");
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(3, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls2"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls2");
		Assert.assertEquals(5, rs.getMetaData().getColumnCount());
		conn.close();
	}

	@Test
	public void useXslImporterColTypes() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls");
		p.setProperty(Constants.SUFFIX_DO_CREATE_TABLE, "true");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "int,string,int");
		
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls");
		Assert.assertEquals(3, rs.getMetaData().getColumnCount());
		conn.close();
	}

	@Test
	public void importWith1stLineAsColNames() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls");
		p.setProperty(Constants.SUFFIX_DO_CREATE_TABLE, "true");
		p.setProperty(Constants.SUFFIX_1ST_LINE_AS_COLUMN_NAMES, "true");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "int,string,int,int,int");
		
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls");
		Assert.assertEquals(5, rs.getMetaData().getColumnCount());
		Assert.assertEquals("ID", rs.getMetaData().getColumnName(1));
		Assert.assertEquals("NAME", rs.getMetaData().getColumnName(2));
		Assert.assertEquals("SALARY", rs.getMetaData().getColumnName(5));
		conn.close();
	}

	@Test
	public void importWith1stLineAsColNamesAndSkip() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls");
		p.setProperty(Constants.SUFFIX_DO_CREATE_TABLE, "true");
		p.setProperty(Constants.SUFFIX_1ST_LINE_AS_COLUMN_NAMES, "true");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "int,string,-,-,int");
		
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls");
		Assert.assertEquals(3, rs.getMetaData().getColumnCount());
		Assert.assertEquals("ID", rs.getMetaData().getColumnName(1));
		Assert.assertEquals("NAME", rs.getMetaData().getColumnName(2));
		Assert.assertEquals("SALARY", rs.getMetaData().getColumnName(3));
		conn.close();
	}
	
	@Test
	public void importSkipCol() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls");
		p.setProperty(Constants.SUFFIX_DO_CREATE_TABLE, "true");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "-,string,-,-,int");
		
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls");
		Assert.assertEquals(2, rs.getMetaData().getColumnCount());
		conn.close();
	}

	@Test
	public void importWithColNamesProperty() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_xls");
		p.setProperty(Constants.SUFFIX_DO_CREATE_TABLE, "true");
		p.setProperty(Constants.SUFFIX_COLUMN_NAMES, "id, name, supervisor_id, dept_id, salary");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "int,string,int,int,int");
		//p.setProperty(Constants.SUFFIX_SKIP_N, "1"); // skips header line

		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.xlsx");
		
		Importer imp = ImporterHelper.getImporterByFileExt("xls", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_xls");
		Assert.assertEquals(5, rs.getMetaData().getColumnCount());
		Assert.assertEquals("ID", rs.getMetaData().getColumnName(1));
		Assert.assertEquals("NAME", rs.getMetaData().getColumnName(2));
		Assert.assertEquals("SUPERVISOR_ID", rs.getMetaData().getColumnName(3));
		Assert.assertEquals("DEPT_ID", rs.getMetaData().getColumnName(4));
		Assert.assertEquals("SALARY", rs.getMetaData().getColumnName(5));
		conn.close();
	}

}
