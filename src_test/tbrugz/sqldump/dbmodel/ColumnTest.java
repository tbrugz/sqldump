package tbrugz.sqldump.dbmodel;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.SQLIdentifierDecorator;

public class ColumnTest {

	@BeforeClass
	public static void before() {
		DBMSResources.instance();
	}
	
	Column getColumn() {
		Column c = new Column();
		c.setName("one");
		c.setType("numeric");
		c.setColumnSize(3);
		c.setNullable(true);
		c.setAutoIncrement(true);
		return c;
	}
	
	@Test
	public void testColumnAutoIncH2() {
		Column c = getColumn();
		SQLIdentifierDecorator.dumpQuoteAll = false;
		ColTypeUtilTest.updateDbId("h2");
		//Column.useAutoIncrement = true;
		Assert.assertEquals("one numeric(3) auto_increment", c.getDefinition());
	}

	@Test
	public void testColumnAutoIncPgSql() {
		Column c = getColumn();
		SQLIdentifierDecorator.dumpQuoteAll = true;
		ColTypeUtilTest.updateDbId("pgsql");
		Assert.assertEquals("\"one\" numeric(3)", c.getDefinition());
	}

	@Test
	public void testColumnAutoIncMysql() {
		Column c = getColumn();
		SQLIdentifierDecorator.dumpQuoteAll = true;
		ColTypeUtilTest.updateDbId("mysql");
		Assert.assertEquals("`one` numeric(3) auto_increment", c.getDefinition());
	}
	
}
