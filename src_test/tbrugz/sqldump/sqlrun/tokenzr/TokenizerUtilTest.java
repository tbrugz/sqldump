package tbrugz.sqldump.sqlrun.tokenzr;

import org.junit.Assert;
import org.junit.Test;

public class TokenizerUtilTest {

	@Test
	public void testTrue() {
		//System.out.println("=== testTrue() ===");
		String test = null;
		
		test = "select /*";
		Assert.assertTrue(TokenizerUtil.containsSqlStatmement(test));
		test = "/* select */a";
		Assert.assertTrue(TokenizerUtil.containsSqlStatmement(test));
		test = "-- select\nselect";
		Assert.assertTrue(TokenizerUtil.containsSqlStatmement(test));
	}

	@Test
	public void testFalse() {
		//System.out.println("=== testFalse() ===");
		String test = null;
		
		test = "/* select /*";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
		test = "/* select";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
		test = "/* select */";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
		test = "/* select */ /* alter table... */";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
		test = "-- select";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
		test = "  -- select\n  ";
		Assert.assertFalse(TokenizerUtil.containsSqlStatmement(test));
	}

	@Test
	public void testRemoveComments() {
		//System.out.println("=== testRemoveComments() ===");
		String test = null;
		
		test = "select /**/ 1";
		Assert.assertEquals("select  1", TokenizerUtil.removeSqlComents(test));

		test = "select -- - \n 2";
		Assert.assertEquals("select \n 2", TokenizerUtil.removeSqlComents(test));
	}

	@Test
	public void testRemoveCommentsQuotes() {
		//System.out.println("=== testRemoveCommentsQuotes() ===");
		String test = null;
		
		test = "select '/**/' 1";
		Assert.assertEquals("select '/**/' 1", TokenizerUtil.removeSqlComents(test));

		test = "select '--' 1";
		Assert.assertEquals("select '--' 1", TokenizerUtil.removeSqlComents(test));
	}

	@Test
	public void testRemoveCommentsDQuote() {
		//System.out.println("=== testRemoveCommentsDQuote() ===");
		String test = null;
		
		test = "select \"/**/\" 1";
		Assert.assertEquals("select \"/**/\" 1", TokenizerUtil.removeSqlComents(test));

		test = "select \" -- \n\" 2";
		Assert.assertEquals("select \" -- \n\" 2", TokenizerUtil.removeSqlComents(test));
	}

}
