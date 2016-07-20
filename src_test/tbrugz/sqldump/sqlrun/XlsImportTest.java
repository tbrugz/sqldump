package tbrugz.sqldump.sqlrun;

import java.io.FileInputStream;
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
		String[] params = {"-propfile=test/sqlrun-h2-xls.properties"};
		SQLRun.main(params);
		
		Properties p = new Properties();
		p.load(new FileInputStream("test/sqlrun-h2-xls.properties"));
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}
	
	@Test
	public void doImportXlsx() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-xls.properties\n"+
			"sqlrun.exec.20.importfile=${basedir}/src_test/tbrugz/sqldump/sqlrun/emp.xlsx";
		StringInputStream sis = new StringInputStream(propsStr);

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p, null);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(5, CSVImportTest.get1stValue(conn, "select count(*) from ins_xls"));
	}

}
