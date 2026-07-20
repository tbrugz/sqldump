package tbrugz.sqldiff.util;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.util.SQLUtils;

public class SQLUtilTest {

	static void shouldValidate(String id) {
		SQLUtils.validateSqlIdentifier(id);
		debug("valid: ["+id+"]");
	}

	static void shouldFail(String id) {
		try {
			SQLUtils.validateSqlIdentifier(id);
			Assert.fail("should have failed: id="+id);
		}
		catch(IllegalArgumentException e) {
			debug("failed ok: ["+id+"]");
		}
	}
	
	static void shouldValidateSchemaName(String id) {
		SQLUtils.validateSchemaName(id);
		debug("valid: ["+id+"]");
	}

	static void shouldFailSchemaName(String id) {
		try {
			SQLUtils.validateSchemaName(id);
			Assert.fail("should have failed: id="+id);
		}
		catch(IllegalArgumentException e) {
			debug("failed ok: ["+id+"]");
		}
	}
	
	static void shouldValidateDataType(String id) {
		SQLUtils.validateSqlDataType(id);
		debug("valid: ["+id+"]");
	}

	static void shouldFailDataType(String id) {
		try {
			SQLUtils.validateSqlDataType(id);
			Assert.fail("should have failed: id="+id);
		}
		catch(IllegalArgumentException e) {
			debug("failed ok: ["+id+"]");
		}
	}

	static void debug (String s) {
		//System.out.println(s);
	}
	
	@Test
	public void testValidateSchemaName() {
		shouldValidateSchemaName("abc");
		shouldValidateSchemaName("AbC123");
		shouldFailSchemaName("1ABC");
		shouldFailSchemaName("=ABC");
		shouldFailSchemaName("AbC=");
		shouldFailSchemaName("Abcdê");
		shouldValidateSchemaName("_C123");
		shouldValidateSchemaName("_C123-II");
		shouldValidate(null);
	}

	@Test
	public void testValidateSqlId() {
		shouldValidate("abc");
		shouldValidate("AbC123");
		shouldValidate("1ABC");
		shouldValidate("ABC#");
		shouldFail("=ABC");
		shouldValidate("AbC=");
		shouldValidate("AbC Cd=");
		shouldValidate("org/hash/XYZ");
		shouldValidate("package-info");
		shouldFail("[ABC]");
		shouldFail("ABC'");
		shouldValidate("Abcdê");
		shouldValidate(null);
		shouldValidate("Abcd?");
		shouldValidate("SSS (ZZ)");
	}
	
	@Test
	public void testValidateSqlDataType() {
		shouldValidateDataType("abc");
		shouldValidateDataType("varchar2");
		shouldValidateDataType("DOUBLE PRECISION");
		shouldValidateDataType("TIMESTAMP(6)");
		shouldValidateDataType("PL/SQL BOOLEAN");
		shouldFailDataType("1ABC");
		shouldFailDataType("=ABC");
		shouldFailDataType("AbC=");
		shouldFailDataType("[ABC]");
		shouldFailDataType("ABC'");
		shouldFailDataType("Abcdê");
		shouldValidateDataType(null);
	}

}
