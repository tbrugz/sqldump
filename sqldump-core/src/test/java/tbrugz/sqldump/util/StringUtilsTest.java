package tbrugz.sqldump.util;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.WhitespaceIgnoreType;
import tbrugz.sqldump.processors.ContentNormalizerProcessor;

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

	@Test
	public void testMatchPtrnCreate() {
		String s = "create or replace procedure abc.123";
		Matcher m = ContentNormalizerProcessor.PTRN_CREATE_EX.matcher(s);
		//System.out.println(s+" ; "+m.matches());
		Assert.assertTrue(m.matches());

		s = "create procedure abc.123";
		m = ContentNormalizerProcessor.PTRN_CREATE_EX.matcher(s);
		//System.out.println(s+" ; "+m.matches());
		Assert.assertTrue(m.matches());

		s = " create or replace package body 123";
		m = ContentNormalizerProcessor.PTRN_CREATE_EX.matcher(s);
		//System.out.println(s+" ; "+m.matches());
		Assert.assertTrue(m.matches());
	}

	@Test
	public void testNormalizeDeclaration() {
		String s = "create or replace   procedure  ABC.123 as begin end;";
		String ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("create or replace procedure ABC.123 as begin end;", ns);

		s = " create    package BODY  ABC.123 as begin end;";
		ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("create package body ABC.123 as begin end;", ns);

		s = " create package  body abc.123 as begin end;";
		ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("create package body ABC.123 as begin end;", ns);

		s = " create    package BODY  \"ABC\".\"123\" as begin end;";
		ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("create package body ABC.123 as begin end;", ns);

		s = " create    package BODY  \"ABC\".\"DEF_123\" as begin end;";
		ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("create package body ABC.DEF_123 as begin end;", ns);

		s = " function  \"ABC\".\"DEF_123\" as begin end;";
		ns = ContentNormalizerProcessor.normalizeDeclaration(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("function ABC.DEF_123 as begin end;", ns);
	}

	@Test
	public void normalizeTriggerDescription() {
		String s = "  \"ABC\".\"DEF_123\" as begin end;";
		String ns = ContentNormalizerProcessor.normalizeTriggerDescription(s);
		//System.out.println(s+" ; "+ns);
		Assert.assertEquals("ABC.DEF_123 as begin end;", ns);
		
	}

}
