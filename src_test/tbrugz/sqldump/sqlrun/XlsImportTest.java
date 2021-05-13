package tbrugz.sqldump.sqlrun;

import java.sql.Connection;
import java.util.Properties;

import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class XlsImportTest {
	
	@Test
	public void doImportXls() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.02.statement=create table ins_xls (ID integer, NAME varchar, SUPERVISOR_ID integer, DEPARTMENT_ID integer, SALARY integer)\n"+
			"";
		StringInputStream sis = new StringInputStream(propsStr);

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
		StringInputStream sis = new StringInputStream(propsStr);

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
		StringInputStream sis = new StringInputStream(propsStr);

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
		StringInputStream sis = new StringInputStream(propsStr);

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}

}
