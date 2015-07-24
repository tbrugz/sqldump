package tbrugz.sqldump.sqlrun;

import java.util.Properties;

import org.junit.Test;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.ParametrizedProperties;

public class SQLRunMainTest {

	@Test(expected = ProcessingException.class)
	public void testIt() throws Exception {
		Properties p = new ParametrizedProperties();
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(new String[]{"aa","ab"}, p, null);
	}
}
