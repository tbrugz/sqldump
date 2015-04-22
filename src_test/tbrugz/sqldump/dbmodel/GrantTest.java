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

}
