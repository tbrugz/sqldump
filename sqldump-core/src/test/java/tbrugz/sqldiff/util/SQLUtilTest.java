package tbrugz.sqldiff.util;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.util.SQLUtils;

public class SQLUtilTest {

	static void shouldValidade(String id) {
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
	
	@Test
	public void testValidateSqlId() {
		shouldValidade("abc");
		shouldValidade("AbC123");
		shouldFail("1ABC");
		shouldFail("=ABC");
		shouldValidade("AbC=");
		shouldFail("[ABC]");
		shouldFail("ABC'");
		shouldValidade(null);
	}
	
}
