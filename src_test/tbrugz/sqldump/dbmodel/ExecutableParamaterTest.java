package tbrugz.sqldump.dbmodel;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutableParamaterTest {

	@Test
	public void testEquals() {
		ExecutableParameter ep1 = new ExecutableParameter();
		ExecutableParameter ep2 = new ExecutableParameter();
		ep1.setName("PAR1");
		ep2.setName("PAR1");
		Assert.assertTrue(ep1.equals(ep2));
	}

	@Test
	public void testHashCodes() {
		ExecutableParameter ep1 = new ExecutableParameter();
		ExecutableParameter ep2 = new ExecutableParameter();
		ep1.setName("PAR1");
		ep2.setName("PAR1");
		Assert.assertTrue(ep1.hashCode()==ep2.hashCode());
	}

}
