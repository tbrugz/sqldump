package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.tokenzr.TokenizerUtil.QueryParameter;

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

	@Test
	public void testNamedParameters() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc ";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);

		test = "select :abc";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);
	}

	@Test
	public void test2NamedParameters() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(2, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);
		Assert.assertEquals("def", qpl.get(1).name);
		Assert.assertEquals(12, qpl.get(1).position);

		test = "select :defg :defg";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(2, qpl.size());
		Assert.assertEquals("defg", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);
		Assert.assertEquals("defg", qpl.get(1).name);
		Assert.assertEquals(13, qpl.get(1).position);
	}

	@Test
	public void testNamedParametersInComm() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc --:def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);

		test = "select --:abc \n :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("def", qpl.get(0).name);
		Assert.assertEquals(16, qpl.get(0).position);
	}

	@Test
	public void testNamedParametersInBlockComm() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc /* :def */";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);

		test = "select /*:abc*/  :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("def", qpl.get(0).name);
		Assert.assertEquals(17, qpl.get(0).position);
	}

	@Test
	public void testNamedParametersInString() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc ':def' ";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);

		test = "select ':abc'  :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("def", qpl.get(0).name);
		Assert.assertEquals(15, qpl.get(0).position);
	}

	@Test
	public void testNamedParametersDQuotes() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc \":def\" ";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);

		test = "select \":abc\"  :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("def", qpl.get(0).name);
		Assert.assertEquals(15, qpl.get(0).position);
	}

	@Test
	public void testReplace2NamedParameters() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc :def";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(2, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);
		Assert.assertEquals("def", qpl.get(1).name);
		Assert.assertEquals(12, qpl.get(1).position);

		String replaced = TokenizerUtil.replaceNamedParameters(test, qpl);
		Assert.assertEquals("select ?    ?   ", replaced);
	}
	
	@Test
	public void testReplaceNamedParametersInBlockComm() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc /* :def */";
		qpl = TokenizerUtil.getNamedParameters(test);
		Assert.assertEquals(1, qpl.size());
		Assert.assertEquals("abc", qpl.get(0).name);
		Assert.assertEquals(7, qpl.get(0).position);

		String replaced = TokenizerUtil.replaceNamedParameters(test, qpl);
		Assert.assertEquals("select ?    /* :def */", replaced);
	}

	@Test(expected=RuntimeException.class)
	public void testNamedAndPositionalParameters() {
		String test = null;
		List<QueryParameter> qpl = null;

		test = "select :abc and ?";
		TokenizerUtil.getNamedParameters(test);
	}

}
