package tbrugz.sqldump.dbmsfeatures;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.sqlrun.QueryDumper;

public class H2FeaturesTest {

	static final Log log = LogFactory.getLog(H2FeaturesTest.class);
	static final String PROP_FILE = "H2FeaturesTest.properties";
	//static final String DIR_OUT = "target/work/output/H2FeaturesTest/";

	Properties prop = new Properties();

	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new Properties();
		p.load(H2FeaturesTest.class.getResourceAsStream(PROP_FILE));
		String[] vmparams = {
				"-Dsqlrun.exec.01.statement=create table test1 ( id varchar(20) )",
				};
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
	}

	@Before
	public void before() throws IOException {
		prop.load(H2FeaturesTest.class.getResourceAsStream(PROP_FILE));
	}
	
	@Test
	public void testExplain() throws Exception {
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", prop);
		DBMSResources res = DBMSResources.instance();
		DBMSFeatures feat = res.getSpecificFeatures(conn.getMetaData());
		ResultSet rs = feat.explainPlan("select * from test1", null, conn);
		//Assert.assertTrue(rs.first());
		//Assert.assertEquals("2000", rs.getString(5));
		//Assert.assertEquals("1", rs.getString(3));
		QueryDumper.simplerRSDump(rs);
	}

}
