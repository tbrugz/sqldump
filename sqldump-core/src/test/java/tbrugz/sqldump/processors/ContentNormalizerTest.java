package tbrugz.sqldump.processors;

import org.junit.Assert;
import org.junit.Test;

public class ContentNormalizerTest {

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

}
