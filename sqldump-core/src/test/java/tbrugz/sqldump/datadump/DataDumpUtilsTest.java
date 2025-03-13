package tbrugz.sqldump.datadump;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.util.Utils;

public class DataDumpUtilsTest {

	@Test
	public void testGuessPivotColValues() throws Exception {
		String s = "count_ABC|||YEAR:::2024|||MONTH:::12";
		List<String> vals = DataDumpUtils.guessPivotColValues(s);
		String ret = Utils.join(vals, "/");
		//System.out.println(csv);
		Assert.assertEquals("2024/12", ret);
	}
	
}
