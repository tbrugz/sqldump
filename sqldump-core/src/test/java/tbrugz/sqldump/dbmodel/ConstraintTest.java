package tbrugz.sqldump.dbmodel;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.util.SQLIdentifierDecorator;

public class ConstraintTest {

	@BeforeClass
	public static void beforeClass() {
		SQLIdentifierDecorator.dumpQuoteAll = false;
	}

	@Test
	public void testUniqueConstraint() {
		Constraint c = new Constraint();
		c.setName("UNIQUE_ABC");
		c.setType(ConstraintType.UNIQUE);
		c.setUniqueColumns(Arrays.asList(new String[]{"COL1", "COL2"}));
		Assert.assertEquals("constraint UNIQUE_ABC unique (COL1, COL2)", c.getDefinition(false));
	}

	@Test
	public void testCheckConstraint() {
		Constraint c = new Constraint();
		c.setName("CHECK_ABC");
		c.setType(ConstraintType.CHECK);
		c.setCheckDescription("ABC > 2");
		Assert.assertEquals("constraint CHECK_ABC check (ABC > 2)", c.getDefinition(false));
	}

}
