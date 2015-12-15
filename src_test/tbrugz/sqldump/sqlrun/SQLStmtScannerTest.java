package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

public class SQLStmtScannerTest {

	SQLStmtScanner scanner;
	
	{
		String str = "";
		InputStream is = new ByteArrayInputStream(str.getBytes());
		scanner = new SQLStmtScanner(is, "UTF-8", false);
	}
	
	@Test
	public void testFind3Apos() {
		String test = "abc'def''ghi";
		Assert.assertEquals(3, scanner.countApos(test));
	}

	@Test
	public void testFind1Apos1() {
		String test = "abc'defghi";
		Assert.assertEquals(1, scanner.countApos(test));
	}
	

}
