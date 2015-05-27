package tbrugz.sqldump.dbmodel;

import org.junit.Assert;
import org.junit.Test;

public class GrantTest {

	@Test
	public void testParse() {
		Grant gr = new Grant("USER_X", PrivilegeType.SELECT, "USER_Y");
		String grStr = gr.toString();
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(gr, gr2);
	}
	
	@Test
	public void testParseBlank() {
		String grStr = "";
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(null, gr2);
	}
	
	@Test
	public void testParseNull() {
		String grStr = null;
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(null, gr2);
	}

	@Test
	public void testParseError() {
		String grStr = "[]";
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(null, gr2);
	}

	@Test
	public void testParseWithColumn() {
		Grant gr = new Grant("USER_X", "COLUMN_Z", PrivilegeType.SELECT, "USER_Y", false);
		String grStr = gr.toString();
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(gr, gr2);
	}

	@Test
	public void testParseWithGrantOption() {
		Grant gr = new Grant("USER_X", null, PrivilegeType.SELECT, "USER_Y", true);
		String grStr = gr.toString();
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(gr, gr2);
	}

}
