package tbrugz.sqldump.sqlrun;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class StmtProcTest {

	@Test
	public void testReplaceParameters() throws SQLException {
		StmtProc proc = new StmtProc();
		Properties prop = new Properties();
		prop.setProperty("sqlrun.exec.a.param.1", "x");
		//prop.setProperty("_procid", "a");
		proc.setProperties(prop);
		String replaced = proc.replaceParameters("abc ? cde", "a");
		Assert.assertEquals("abc x cde", replaced);
	}
}
