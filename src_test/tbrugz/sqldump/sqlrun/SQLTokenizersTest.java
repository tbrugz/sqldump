package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

public class SQLTokenizersTest {

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
	
	@Test
	public void testSQLStmtTokenizer() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc;cde'';eee';'");
		
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("abc", p.next());
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("cde''", p.next());
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
}
