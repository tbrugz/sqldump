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

public class SQLRunAndDumpXlsTest {
	
	public static final String[] NULL_PARAMS = null;

	public String dbpath = "mem:SQLRunAndDumpXlsTest";
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
	public void doRunImportXlsFiles() throws Exception {
		Assume.assumeFalse(isWindows);

		String mydbpath = dbpath+"-importfiles-xls;DB_CLOSE_DELAY=-1";
		
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.05.import=xls",
				"-Dsqlrun.exec.05.inserttable=dept",
				"-Dsqlrun.exec.05.importfiles.glob=src/test/resources/data/**/dept*.xlsx",
				//"-Dsqlrun.exec.05.importfiles.glob="+System.getProperty("user.dir")+"/src/test/resources/data/**/dept*.csv", // absolute path
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
					"-Dsqldump.datadump.outfilepattern=target/work/output/SQLRunAndDumpTest/data-import-xls_[tablename].[syntaxfileext]",
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
		
		String csvDept = IOUtil.readFromFilename("target/work/output/SQLRunAndDumpTest/data-import-xls_DEPT.csv");
		//System.out.println(csvDept);
		int count = TestUtil.countLines(csvDept);
		Assert.assertEquals(6+1, count);
	}
	
}
