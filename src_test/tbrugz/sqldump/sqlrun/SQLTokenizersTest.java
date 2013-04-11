package tbrugz.sqldump.sqlrun;

import org.junit.Assert;
import org.junit.Test;

public class SQLTokenizersTest {

	@Test
	public void testFind3Apos() {
		String test = "abc'def''ghi";
		Assert.assertEquals(3, SQLStmtScanner.countApos(test));
	}

	@Test
	public void testFind1Apos1() {
		String test = "abc'defghi";
		Assert.assertEquals(1, SQLStmtScanner.countApos(test));
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
