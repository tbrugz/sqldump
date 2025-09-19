package tbrugz.sqldump.util;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.WhitespaceIgnoreType;

public class StringUtilsTest {

	String s1 = " \t \n abc \t \n ";

	@Test
	public void testRTrim() {
		Assert.assertEquals(" \t \n abc", StringUtils.rtrim(s1));
	}

	@Test
	public void testLTrim() {
		Assert.assertEquals("abc \t \n ", StringUtils.ltrim(s1));
	}

	@Test
	public void testLRTrim() {
		Assert.assertEquals("abc", StringUtils.lrtrim(s1));
	}

	@Test
	public void testEqualsIgnoreWhitespaces() {
		Assert.assertTrue(StringUtils.equalsIgnoreWhitespacesEachLine("abc\n123 \n", "abc\n123\n", WhitespaceIgnoreType.EOL));
		Assert.assertFalse(StringUtils.equalsIgnoreWhitespacesEachLine("abc\n123 \n", "abc\n123\n", WhitespaceIgnoreType.NONE));
		//Assert.assertTrue(StringUtils.equalsIgnoreWhitespacesEachLine("abc\n123\n", "abc\n123\r\n", WhitespaceIgnoreType.NONE));

		Assert.assertTrue(StringUtils.equalsIgnoreWhitespacesEachLine("abc\n123 \r\n", "abc\n123\n", WhitespaceIgnoreType.EOL));
		Assert.assertTrue(StringUtils.equalsIgnoreWhitespacesEachLine("abc\n123 \n", "abc\n123\r\n", WhitespaceIgnoreType.EOL));
		Assert.assertTrue(StringUtils.equalsIgnoreWhitespacesEachLine("abc\r\n123", "abc\r\n123", WhitespaceIgnoreType.EOL));
	}

}
