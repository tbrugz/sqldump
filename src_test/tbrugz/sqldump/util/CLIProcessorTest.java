package tbrugz.sqldump.util;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class CLIProcessorTest {

	@Test
	public void testOneFile() throws IOException {
		String[] args = { CLIProcessor.PARAM_PROPERTIES_FILENAME+"src_test/tbrugz/sqldump/util/p3.properties" };
		Properties p = new Properties();
		CLIProcessor.init("cli", args, "", p);
		
		Assert.assertEquals("value2", p.getProperty("id1"));
	}

	@Test
	public void testTwoFiles() throws IOException {
		String[] args = {
			CLIProcessor.PARAM_PROPERTIES_FILENAME+"src_test/tbrugz/sqldump/util/p3.properties",
			CLIProcessor.PARAM_PROPERTIES_FILENAME+"src_test/tbrugz/sqldump/util/p-xtra.properties"
		};
		Properties p = new Properties();
		CLIProcessor.init("cli", args, "", p);
		
		Assert.assertEquals("xtra", p.getProperty("id-xtra"));
		Assert.assertEquals("value2", p.getProperty("id1"));
	}

	@Test
	public void testOneFileOneResource() throws IOException {
		String[] args = {
			CLIProcessor.PARAM_PROPERTIES_FILENAME+"src_test/tbrugz/sqldump/util/p3.properties",
			CLIProcessor.PARAM_PROPERTIES_RESOURCE+"/tbrugz/sqldump/util/p-xtra.properties"
		};
		Properties p = new Properties();
		CLIProcessor.init("cli", args, "", p);
		
		Assert.assertEquals("xtra", p.getProperty("id-xtra"));
		Assert.assertEquals("value2", p.getProperty("id1"));
	}

	@Test
	public void testDefaultPropFile() throws IOException {
		String[] args = {};
		Properties p = new Properties();
		CLIProcessor.init("cli", args, "src_test/tbrugz/sqldump/util/p3.properties", p);
		
		Assert.assertEquals("value2", p.getProperty("id1"));
		Assert.assertEquals(null, p.getProperty("id-xtra"));
	}
}
