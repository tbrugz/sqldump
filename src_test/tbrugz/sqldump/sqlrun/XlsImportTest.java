package tbrugz.sqldump.sqlrun;

import java.util.Properties;

import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ParametrizedProperties;

public class XlsImportTest {
	
	@Test
	public void doImportXls() throws Exception {
		String[] params = {"-propfile=test/sqlrun-h2-xls.properties"};
		SQLRun.main(params);
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
	}

}
