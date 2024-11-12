package tbrugz.sqldump.dbmodel;

import java.util.Arrays;
import java.util.List;

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
		Grant gr = new Grant("USER_X", Arrays.asList("COLUMN_Z"), PrivilegeType.SELECT, "USER_Y", false);
		String grStr = gr.toString();
		Grant gr2 = Grant.parseGrant(grStr);
		//System.out.println(gr+" // "+gr2);
		Assert.assertEquals(gr, gr2);
	}

	@Test
	public void testParseWithGrantOption() {
		Grant gr = new Grant("USER_X", (List<String>) null, PrivilegeType.SELECT, "USER_Y", true);
		String grStr = gr.toString();
		Grant gr2 = Grant.parseGrant(grStr);
		Assert.assertEquals(gr, gr2);
	}

}
