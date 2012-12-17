package tbrugz.sqldump.sqlrun;

import org.junit.Test;

public class FailoverTest {
	@Test
	public void doRun() throws Exception {
		String[] params = {
				"-propfile=src_test/tbrugz/sqldump/sqlrun/failover-access_logs.properties"
		};
		SQLRun.main(params);
	}
}
