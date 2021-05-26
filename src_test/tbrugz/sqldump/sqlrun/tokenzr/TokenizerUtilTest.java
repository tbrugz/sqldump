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
	
}
