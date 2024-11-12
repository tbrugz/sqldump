package tbrugz.sqldump.sqlrun;

import java.sql.Connection;
import java.util.Properties;

import org.junit.Test;

import tbrugz.sqldump.TestUtil;

public class StmtExecTest {

	public static final String[] NULL_PARAMS = null;
	public String dbpath = "mem:StmtExecTest";

	@Test
	public void doSetupAndLoadData() throws Exception {
		String[] vmparamsRun = {
				"-Dsqlrun.exec.00.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-drop.sql",
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-load.sql",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		Connection conn = TestUtil.getConn(p, "sqlrun");

		SQLRun sqlr = new SQLRun();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(NULL_PARAMS, p, conn);
	}

	@Test
	public void doSetupAndLoadMoreData() throws Exception {
		String[] vmparamsRun = {
				"-Dsqlrun.exec.00.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-drop.sql",
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-load.sql",
				"-Dsqlrun.exec.03.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-load2.sql",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		Connection conn = TestUtil.getConn(p, "sqlrun");

		SQLRun sqlr = new SQLRun();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(NULL_PARAMS, p, conn);
	}

	@Test
	public void doSetupAndLoadMoreDataStmtTokenizer() throws Exception {
		String[] vmparamsRun = {
				"-Dsqlrun.exec.00.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-drop.sql",
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-load.sql",
				"-Dsqlrun.exec.03.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept-load2.sql",
				"-Dsqlrun.sqltokenizerclass=SQLStmtTokenizer",
				//"-Dsqlrun.sqltokenizerclass=SQLStmtScanner|SQLStmtTokenizer|StringSpliter",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		Connection conn = TestUtil.getConn(p, "sqlrun");

		SQLRun sqlr = new SQLRun();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(NULL_PARAMS, p, conn);
	}
	
}
