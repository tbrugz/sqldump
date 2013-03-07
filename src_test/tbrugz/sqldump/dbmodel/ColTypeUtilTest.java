package tbrugz.sqldump.dbmodel;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.def.DBMSResources;

public class ColTypeUtilTest {
	
	void assertIt(String type, boolean shouldUsePrecision) {
		Assert.assertEquals("Column type "+type.toUpperCase()+" should "+(shouldUsePrecision?"":"NOT ")+"use precision",
				shouldUsePrecision,
				Column.ColTypeUtil.usePrecision(type));
	}
	
	@Test
	public void testColTypes() {
		DBMSResources.instance().updateDbId("derby");
		DBMSResources.instance().updateDbId(null);
		assertIt("numeric", true);
		
		assertIt("long raw", false);
		assertIt("blob", false);
		assertIt("clob", false);
		assertIt("text", false);
		assertIt("timestamp", false);
		assertIt("date", false);

		assertIt("integer", true);
	}

	@Test
	public void testDerbyColTypes() {
		DBMSResources.instance().updateDbId("derby");
		assertIt("numeric", true);
		assertIt("long raw", false);
		assertIt("blob", false);
		
		assertIt("integer", false);
	}

	@Test
	public void testHSQLDBColTypes() {
		DBMSResources.instance().updateDbId("hsqldb");
		assertIt("blob", false);
		assertIt("varchar", true);
		
		assertIt("integer", false);
		assertIt("smallint", false);
	}
}
