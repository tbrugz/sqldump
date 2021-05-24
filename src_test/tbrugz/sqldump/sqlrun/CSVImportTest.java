package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL; 
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.importers.AbstractImporter;
import tbrugz.sqldump.sqlrun.importers.CSVImporter;
import tbrugz.sqldump.sqlrun.importers.ImporterHelper;
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
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

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
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

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
	
	@Test
	public void useCsvImporter() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_csv2 (ID_TSE integer, SIGLA varchar, NOME varchar, DEFERIMENTO varchar, PRESIDENTE_NACIONAL varchar, NUMERO integer)");
		stmt.execute();
		String execId = "1";
		Properties p = new Properties();
		
		p.setProperty("sqlrun.exec."+execId+".inserttable", "ins_csv2");
		p.setProperty("sqlrun.exec."+execId+".columndelimiter", ";");
		p.setProperty("sqlrun.exec."+execId+".skipnlines", "1");
		//p.setProperty("sqlrun.exec."+execId+".do-create-table", "true"); //TODO: add '.do-create-table' to AbstractImporter
		InputStream is = new FileInputStream("test/data/tse_partidos.csv");
		
		Importer imp = new CSVImporter();
		imp.setConnection(conn);
		imp.setExecId(execId);
		imp.setProperties(p);
		imp.importStream(is);

		Assert.assertEquals(29, get1stValue(conn, "select count(*) from ins_csv2"));
		conn.close();
	}

	@Test
	public void useCsvImporterWithHelper() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_csv2 (ID_TSE integer, SIGLA varchar, NOME varchar, DEFERIMENTO varchar, PRESIDENTE_NACIONAL varchar, NUMERO integer)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_csv2");
		p.setProperty(".columndelimiter", ";");
		p.setProperty(Constants.SUFFIX_SKIP_N, "1");
		//p.setProperty(".do-create-table", "true"); //TODO: add '.do-create-table' to AbstractImporter
		InputStream is = new FileInputStream("test/data/tse_partidos.csv");
		
		Importer imp = new CSVImporter();
		ImporterHelper.setImporterPlainProperties(imp, p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(29, get1stValue(conn, "select count(*) from ins_csv2"));
		conn.close();
	}
	
	@Test
	public void useCsvImporterWithHelper2() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_csv2 (ID_TSE integer, SIGLA varchar, NOME varchar, DEFERIMENTO varchar, PRESIDENTE_NACIONAL varchar, NUMERO integer)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_csv2");
		p.setProperty(".columndelimiter", ";");
		p.setProperty(Constants.SUFFIX_SKIP_N, "1");
		//p.setProperty(".do-create-table", "true"); //TODO: add '.do-create-table' to AbstractImporter
		InputStream is = new FileInputStream("test/data/tse_partidos.csv");
		
		Importer imp = ImporterHelper.getImporterByFileExt("csv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(29, get1stValue(conn, "select count(*) from ins_csv2"));
		conn.close();
	}

	@Test
	public void useScsvImporterHelper() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_scsv (id integer, name varchar, supervisor_id integer, department_id integer, salary float)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_scsv");
		p.setProperty(Constants.SUFFIX_SKIP_N, "1");
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.scsv");
		
		Importer imp = ImporterHelper.getImporterByFileExt("scsv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, get1stValue(conn, "select count(*) from ins_scsv"));
		conn.close();
	}

	@Test
	public void usePsvImporterHelper() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_psv (id integer, name varchar, supervisor_id integer, department_id integer, salary float)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_psv");
		p.setProperty(Constants.SUFFIX_SKIP_N, "1");
		
		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.psv");
		Importer imp = ImporterHelper.getImporterByFileExt("psv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, get1stValue(conn, "select count(*) from ins_psv"));
		conn.close();
	}

	@Test
	public void useTsvImporterHelper() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_tsv (id integer, name varchar, supervisor_id integer, department_id integer, salary float)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_tsv");
		p.setProperty(Constants.SUFFIX_SKIP_N, "2");

		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.tsv");
		Importer imp = ImporterHelper.getImporterByFileExt("tsv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(4, get1stValue(conn, "select count(*) from ins_tsv"));
		conn.close();
	}

	/*
	@Test
	public void useTsvImporterHelper2() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_tsv2 (id integer, name varchar, supervisor_id integer, department_id integer, salary float)");
		stmt.execute();

		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.tsv");
		
		Importer imp = ImporterHelper.getImporterByFileExt("tsv");
		imp.setConnection(conn);
		ImporterHelper.setPlainProperty(imp, Constants.SUFFIX_INSERTTABLE, "ins_tsv2");
		ImporterHelper.setPlainProperty(imp, Constants.SUFFIX_SKIP_N, "2");
		imp.importStream(is);

		Assert.assertEquals(4, get1stValue(conn, "select count(*) from ins_tsv2"));
		conn.close();
	}
	*/

	@Test
	public void useTsvImporterHelperWithLimit() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_tsv (id integer, name varchar, supervisor_id integer, department_id integer, salary float)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_tsv");
		p.setProperty(Constants.SUFFIX_SKIP_N, "2");
		p.setProperty(Constants.SUFFIX_LIMIT_LINES, "2");

		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.tsv");
		Importer imp = ImporterHelper.getImporterByFileExt("tsv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(2, get1stValue(conn, "select count(*) from ins_tsv"));
		conn.close();
	}

	@Test
	public void useTsvImporterHelperColTypes() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
		PreparedStatement stmt = conn.prepareStatement("create table ins_tsv (id integer, name varchar, supervisor_id integer)");
		stmt.execute();

		Properties p = new Properties();
		p.setProperty(Constants.SUFFIX_INSERTTABLE, "ins_tsv");
		p.setProperty(Constants.SUFFIX_SKIP_N, "1");
		p.setProperty(Constants.SUFFIX_COLUMN_TYPES, "int,string,int");

		InputStream is = new FileInputStream("src_test/tbrugz/sqldump/sqlrun/emp.tsv");
		Importer imp = ImporterHelper.getImporterByFileExt("tsv", p);
		imp.setConnection(conn);
		imp.importStream(is);

		Assert.assertEquals(5, get1stValue(conn, "select count(*) from ins_tsv"));
		ResultSet rs = conn.createStatement().executeQuery("select * from ins_tsv");
		Assert.assertEquals(3, rs.getMetaData().getColumnCount());
		conn.close();
	}

}
