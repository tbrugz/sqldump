package tbrugz.sqldump.processors;

import java.util.Properties;

import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.util.ParametrizedProperties;

public class SQLRunProcessorTest {

	@Test
	public void testProcessor() throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("sqlrun-processor.properties"));
		SQLDump sqld = new SQLDump();
		sqld.doMain(null, p);
	}

}
