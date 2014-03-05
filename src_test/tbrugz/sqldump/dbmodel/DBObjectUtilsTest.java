package tbrugz.sqldump.dbmodel;

import org.junit.Assert;
import org.junit.Test;

public class DBObjectUtilsTest {
	
	@Test
	public void testEqualsEachLineOk() {
		String s1 = " ab \n z";
		String s2 = "ab\nz";
		Assert.assertTrue(DBObjectUtils.equalsIgnoreWhitespacesEachLine(s1, s2));
	}

	@Test
	public void testEqualsEachLineError() {
		String s1 = " ab \n z";
		String s2 = "ab\nzb";
		Assert.assertFalse(DBObjectUtils.equalsIgnoreWhitespacesEachLine(s1, s2));
	}

	@Test
	public void testEqualsTrimOk() {
		String s1 = " ab ";
		String s2 = "ab";
		Assert.assertTrue(DBObjectUtils.equalsWithTrim(s1, s2));
	}
	
	@Test
	public void testEqualsTrimError() {
		String s1 = " ab c ";
		String s2 = " ab  c ";
		Assert.assertFalse(DBObjectUtils.equalsWithTrim(s1, s2));
	}
	
}
