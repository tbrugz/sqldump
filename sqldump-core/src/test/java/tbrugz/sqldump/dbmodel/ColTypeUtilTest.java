package tbrugz.sqldump.dbmodel;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ColTypeUtilTest {
	
	static final Log log = LogFactory.getLog(ColTypeUtilTest.class);
	
	void assertIt(String type, boolean shouldUsePrecision) {
		Assert.assertEquals("Column type "+type.toUpperCase()+" should "+(shouldUsePrecision?"":"NOT ")+"use precision",
				shouldUsePrecision,
				Column.ColTypeUtil.usePrecision(type));
	}
	
	public static void updateDbId(String id) {
		//DBMSResources.instance().updateDbId(id);
		Column.ColTypeUtil.setDbId(id);
		//DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(id);
		//log.info("feat.getIdentifierQuoteString: "+feat.getIdentifierQuoteString()+" / "+SQLIdentifierDecorator.dumpQuoteAll);
		//SQLIdentifierDecorator.dumpIdentifierQuoteString = features.getIdentifierQuoteString();
	}
	
	@Test
	public void testColTypes() {
		updateDbId("derby");
		updateDbId(null);
		assertIt("numeric", true);
		
		assertIt("long raw", false);
		assertIt("blob", false);
		assertIt("clob", false);
		assertIt("text", false);
		assertIt("timestamp", false);
		assertIt("date", false);
		assertIt("integer", false);
		assertIt("bit", true);
		
		//XXXxx ansi-sql92 compatible
		assertIt("real", false);
	}

	@Test
	public void testDerbyColTypes() {
		updateDbId("derby");
		assertIt("numeric", true);
		assertIt("long raw", false);
		assertIt("blob", false);
		
		assertIt("integer", false);
	}

	@Test
	public void testHSQLDBColTypes() {
		updateDbId("hsqldb");
		assertIt("blob", false);
		assertIt("varchar", true);
		
		assertIt("integer", false);
		assertIt("smallint", false);
	}
	
	@Test
	public void testMSAccessColTypes() {
		updateDbId("msaccess");
		assertIt("varchar", true);
		
		//ms-access specific
		assertIt("BIT", false);
		assertIt("real", false);
		assertIt("CURRENCY", false);
	}
	
	@Test
	public void testSQLTypesProps() {
		updateDbId(null);
		
		Properties p = new Properties();
		p.setProperty("sqldump.sqltypes.ignoreprecision", "VARCHAR,varchar2");
		p.setProperty("sqldump.sqltypes.useprecision", "blob");
		Column.ColTypeUtil.setProperties(p);
		
		assertIt("numeric", true);
		assertIt("long raw", false);
		
		//should use precision
		assertIt("blob", true);

		//should NOT use precision
		assertIt("varchar", false);
		assertIt("varchar2", false);
	}
	
	@Test
	public void testStandardTypes() {
		updateDbId(null);
		Assert.assertTrue(Column.ColTypeUtil.isBinary("BLOB"));
		Assert.assertTrue(Column.ColTypeUtil.isBoolean("boolean"));
		Assert.assertTrue(Column.ColTypeUtil.isCharacter("varchar2"));
		Assert.assertTrue(Column.ColTypeUtil.isDatetime("TIMESTAMP"));
		Assert.assertTrue(Column.ColTypeUtil.isInteger("integer"));
		Assert.assertTrue(Column.ColTypeUtil.isNumeric("float"));
		Assert.assertTrue(Column.ColTypeUtil.isNumeric("int4"));
		
		Assert.assertFalse(Column.ColTypeUtil.isBoolean("boole"));
	}
	
	@AfterClass
	public static void tearDown() {
		log.info("tearDown");
		Column.ColTypeUtil.setProperties(null);
	}
}
