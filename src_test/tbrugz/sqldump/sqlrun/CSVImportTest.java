package tbrugz.sqldump.sqlrun;

import java.net.URL; 
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class CSVImportTest {
	
	/*public static final String H2_JAR_PATH = "lib/h2-1.3.168.jar";
	
	@Test @Ignore
	public void doImportWithNewClassLoader() throws Exception {
		String userdir = System.getProperty("user.dir");
		//ClassLoader loader = loadJar(new URL("file://D:/proj/sqldump/"+H2_JAR_PATH));
		String url = "file://"+userdir+"/"+H2_JAR_PATH;
		System.out.println("user.dir: "+userdir+" ; url: "+url);
		ClassLoader loader = loadJar(new URL(url));
		Class<?> clazz = Class.forName("org.h2.Driver", true, loader);
		
		String[] params = {"-propfile=test/sqlrun-h2-csv.properties"};
		SQLRun.main(params);
	}*/

	@Test
	public void doImport() throws Exception {
		String[] params = {"-propfile=test/sqlrun-h2-csv.properties"};
		SQLRun.main(params);
	}

	@Test(expected = ProcessingException.class)
	public void doImportWithError() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-csv.properties\n"+
			"sqlrun.exec.00.statement=drop table unexistent";
		StringInputStream sis = new StringInputStream(propsStr);

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
	}

	@Test
	public void doImportWithLimit() throws Exception {
		String propsStr = 
			"@includes=test/sqlrun-h2-csv.properties\n"+
			"sqlrun.exec.20.limit=10";
		StringInputStream sis = new StringInputStream(propsStr);

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(10, get1stValue(conn, "select count(*) from ins_csv"));
	}
	
	static ClassLoader loadJar(URL url) {
		ClassLoader loader = URLClassLoader.newInstance(
			new URL[] { url },
			CSVImportTest.class.getClassLoader()
		);
		return loader;
	}
	
	static int get1stValue(Connection conn, String sql) throws SQLException {
		ResultSet rs = conn.createStatement().executeQuery(sql);
		rs.next();
		return rs.getInt(1);
	}
}
