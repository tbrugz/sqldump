package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

public class SQLTokenizersTest {

	Iterator<String> scanner;
	
	public SQLTokenizersTest() {
		String str = "";
		InputStream is = new ByteArrayInputStream(str.getBytes());
		scanner = new SQLStmtScanner(is, "UTF-8", false);
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

	@Test
	public void testTokenComment() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc--;\ncde;eee';'");
		
		Assert.assertEquals("abc--;\ncde", p.next());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenCommentExtraNl() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc--;\n;cde;eee");
		
		Assert.assertEquals("abc--;\n", p.next());
		Assert.assertEquals("cde", p.next());
		Assert.assertEquals("eee", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
	@Test
	public void testTokenBlockComment() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc/*;*/cde'';eee';'");
		
		Assert.assertEquals("abc/*;*/cde''", p.next());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
	@Test
	public void testTokenBlockComment2() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc/*aa*/cde;eee");
		
		Assert.assertEquals("abc/*aa*/cde", p.next());
		Assert.assertEquals("eee", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
}
