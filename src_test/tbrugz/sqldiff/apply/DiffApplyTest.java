package tbrugz.sqldiff.apply;

import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ParametrizedProperties;

public class DiffApplyTest {

	String OUTDIR = "work/output/DiffApplyTest";
	
	@BeforeClass
	public static void setupDB() throws Exception {
		//db1
		Properties p = new ParametrizedProperties();
		p.load(DiffApplyTest.class.getResourceAsStream("setupdb.properties"));
		p.load(DiffApplyTest.class.getResourceAsStream("db1.properties"));
		p.setProperty("sqlrun.connpropprefix","sqlrun.db1");
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p, null);

		//db2
		p = new ParametrizedProperties();
		p.load(DiffApplyTest.class.getResourceAsStream("setupdb.properties"));
		p.load(DiffApplyTest.class.getResourceAsStream("db2.properties"));
		p.setProperty("sqlrun.connpropprefix","sqlrun.db2");
		sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p, null);
	}
	
	@Test
	public void testDiffAddColumn() throws Exception {
		//exec DDL
		Properties p = new ParametrizedProperties();
		p.load(DiffApplyTest.class.getResourceAsStream("db2.properties"));
		p.setProperty("sqlrun.exec.50.statement", "alter table emp add column abc varchar(10)");
		p.setProperty("sqlrun.connpropprefix","sqlrun.db2");
		Executor sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
		
		//run diff
		SQLDiff sqldiff = new SQLDiff();
		Properties pd = new ParametrizedProperties();
		pd.load(DiffApplyTest.class.getResourceAsStream("diff1.properties"));
		pd.load(DiffApplyTest.class.getResourceAsStream("db1.properties"));
		pd.load(DiffApplyTest.class.getResourceAsStream("db2.properties"));
		sqldiff.doMain(TestUtil.NULL_PARAMS, pd);
		Assert.assertEquals(1, sqldiff.getLastDiffCount());

		//run diff 2nd time - should have no difference
		sqldiff.doMain(TestUtil.NULL_PARAMS, pd);
		Assert.assertEquals(0, sqldiff.getLastDiffCount());
	}
	
}
