package tbrugz.sqldump.dbmsfeatures;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Index;
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
				"-Dsqlrun.exec.02.statement=create table test2 ( id varchar(20) primary key, name integer, email varchar(30), address varchar(50), constraint name_uk unique (name) )",
				"-Dsqlrun.exec.03.statement=create index idx_customer_email on test2 (email)",
				"-Dsqlrun.exec.04.statement=create index idx_xyz on test2 (email, address)"
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
	
	@Test
	public void testGrabIndexes() throws Exception {
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", prop);
		DBMSResources res = DBMSResources.instance();
		DBMSFeatures feat = res.getSpecificFeatures(conn.getMetaData());
		Assert.assertEquals(true, feat.supportsGrabIndexes());
		Collection<Index> idxs = new ArrayList<>();
		feat.grabDBIndexes(idxs, "PUBLIC", null, null, conn);
		//System.out.println(idxs);
		Assert.assertEquals(3, idxs.size());
		Iterator<Index> it = idxs.iterator();
		Index idx = it.next();
		//[[Index:PUBLIC.IDX_CUSTOMER_EMAIL,table=TEST2,unique=false,cols=[EMAIL]], [Index:PUBLIC.IDX_XYZ,table=TEST2,unique=false,cols=[EMAIL, ADDRESS]], [Index:PUBLIC.NAME_UK_INDEX_4,table=TEST2,unique=true,cols=[NAME]]]
		Assert.assertEquals("IDX_CUSTOMER_EMAIL", idx.getName());
		Assert.assertEquals("EMAIL", idx.getColumns().get(0));
		idx = it.next();
		Assert.assertEquals("IDX_XYZ", idx.getName());
		Assert.assertEquals("EMAIL", idx.getColumns().get(0));
		Assert.assertEquals("ADDRESS", idx.getColumns().get(1));
	}

}
