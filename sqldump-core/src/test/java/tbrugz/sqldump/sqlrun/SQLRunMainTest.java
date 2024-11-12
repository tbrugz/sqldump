package tbrugz.sqldump.sqlrun;

import java.util.Properties;

import org.junit.Test;

import tbrugz.sqldump.util.ParametrizedProperties;

public class SQLRunMainTest {

	@Test(expected = IllegalStateException.class)
	public void testArguments() throws Exception {
		Properties p = new ParametrizedProperties();
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(new String[]{"aa","ab"}, p);
	}
}
