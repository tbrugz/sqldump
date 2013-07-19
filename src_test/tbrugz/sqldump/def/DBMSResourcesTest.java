package tbrugz.sqldump.def;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

public class DBMSResourcesTest {
	
	class TestDBMD extends AbstractDatabaseMetaDataDecorator {
		final String dbProduct;
		final int major;
		final int minor;
		
		public TestDBMD(String dbProduct, int major, int minor) {
			this.dbProduct = dbProduct;
			this.major = major;
			this.minor = minor;
		}
		
		@Override
		public String getDatabaseProductName() throws SQLException {
			return dbProduct;
		}
		
		@Override
		public int getDatabaseMajorVersion() throws SQLException {
			return major;
		}
		
		@Override
		public int getDatabaseMinorVersion() throws SQLException {
			return minor;
		}
	}

	@Test
	public void testDetectDbPgSQL() {
		TestDBMD dbmd = null;
		DBMSResources res = DBMSResources.instance();
		
		dbmd = new TestDBMD("PostgreSQL", 9, 1);
		Assert.assertEquals("pgsql", res.detectDbId(dbmd, false));

		dbmd = new TestDBMD("PostgreSQL", 9, 0);
		Assert.assertEquals("pgsql90-", res.detectDbId(dbmd, false));
	}
		
	@Test
	public void testDetectDbVirtuoso() {
		TestDBMD dbmd = null;
		DBMSResources res = DBMSResources.instance();
		
		dbmd = new TestDBMD("Virtuoso", 0, 0);
		Assert.assertEquals(null, res.detectDbId(dbmd, false));

		dbmd = new TestDBMD(" Virtuoso ", 0, 0);
		Assert.assertEquals("virtuoso", res.detectDbId(dbmd, false));

		dbmd = new TestDBMD("abc Virtuoso 123", 0, 0);
		Assert.assertEquals("virtuoso", res.detectDbId(dbmd, false));
	}
}
