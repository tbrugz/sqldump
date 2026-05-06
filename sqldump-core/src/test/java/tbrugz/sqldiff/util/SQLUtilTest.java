package tbrugz.sqldiff.util;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.util.SQLUtils;

public class SQLUtilTest {

	static void shouldValidate(String id) {
		SQLUtils.validateSqlIdentifier(id);
		System.out.println("valid: ["+id+"]");
	}

	static void shouldFail(String id) {
		try {
			SQLUtils.validateSqlIdentifier(id);
			Assert.fail("should have failed: id="+id);
		}
		catch(IllegalArgumentException e) {
			System.out.println("failed ok: ["+id+"]");
		}
	}
	
	static void shouldValidateDataType(String id) {
		SQLUtils.validateSqlDataType(id);
		System.out.println("valid: ["+id+"]");
	}

	static void shouldFailDataType(String id) {
		try {
			SQLUtils.validateSqlDataType(id);
			Assert.fail("should have failed: id="+id);
		}
		catch(IllegalArgumentException e) {
			System.out.println("failed ok: ["+id+"]");
		}
	}
	
	@Test
	public void testValidateSqlId() {
		shouldValidate("abc");
		shouldValidate("AbC123");
		shouldFail("1ABC");
		shouldFail("=ABC");
		shouldValidate("AbC=");
		shouldFail("[ABC]");
		shouldFail("ABC'");
		shouldValidate(null);
	}
	
	@Test
	public void testValidateSqlDataType() {
		shouldValidateDataType("abc");
		shouldValidateDataType("varchar2");
		shouldValidateDataType("DOUBLE PRECISION");
		shouldFailDataType("1ABC");
		shouldFailDataType("=ABC");
		shouldFailDataType("AbC=");
		shouldFailDataType("[ABC]");
		shouldFailDataType("ABC'");
		shouldValidateDataType(null);
	}

}
