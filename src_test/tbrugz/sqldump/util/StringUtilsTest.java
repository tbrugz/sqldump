package tbrugz.sqldump.util;

import org.junit.Assert;
import org.junit.Test;

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

}
