package tbrugz.sqldump.sqlrun;

import org.junit.Test;

public class FailoverTest {
	@Test
	public void testAccessLog() throws Exception {
		String[] params = {
				"-propfile=src/test/resources/tbrugz/sqldump/sqlrun/failover-access_logs.properties",
				"-usesysprop=false"
		};
		SQLRun.main(params);
	}

	@Test
	public void testSimple() throws Exception {
		String[] params = {
				"-propfile=src/test/resources/tbrugz/sqldump/sqlrun/failover-simple.properties",
				"-usesysprop=false"
		};
		SQLRun.main(params);
	}

	@Test
	public void testSimpleNoFailover() throws Exception {
		String[] params = {
				"-propfile=src/test/resources/tbrugz/sqldump/sqlrun/failover-simple-no.properties",
				"-usesysprop=false"
		};
		SQLRun.main(params);
	}
}
