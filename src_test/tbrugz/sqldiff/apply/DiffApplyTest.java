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
		execDDLId2("alter table emp add column abc varchar(10)");
		
		//run diff
		SQLDiff sqldiff = new SQLDiff();
		runDiff(sqldiff);
		Assert.assertEquals(1, sqldiff.getLastDiffCount());

		//run diff 2nd time - should have no difference
		runDiff(sqldiff);
		Assert.assertEquals(0, sqldiff.getLastDiffCount());
	}
	
	@Test
	public void testDiffAlterColumnType() throws Exception {
		//exec DDL
		execDDLId2("alter table emp alter column name varchar(200)"); //was 100
		
		//run diff
		SQLDiff sqldiff = new SQLDiff();
		runDiff(sqldiff);
		Assert.assertEquals(1, sqldiff.getLastDiffCount());

		//run diff 2nd time - should have no difference
		runDiff(sqldiff);
		Assert.assertEquals(0, sqldiff.getLastDiffCount());
	}

	@Test
	public void testDiffAlterColumnNullable() throws Exception {
		//exec DDL
		execDDLId2("alter table emp alter column department_id set not null"); //was null
		//execDDLId2("alter table emp alter column name set null"); //was not null
		
		//run diff
		SQLDiff sqldiff = new SQLDiff();
		runDiff(sqldiff);
		Assert.assertEquals(1, sqldiff.getLastDiffCount());

		//run diff 2nd time - should have no difference
		runDiff(sqldiff);
		Assert.assertEquals(0, sqldiff.getLastDiffCount());
	}
	
	@Test
	public void testDiffDropColumn() throws Exception {
		//exec DDL
		execDDLId2("alter table emp drop column salary");
		
		//run diff
		SQLDiff sqldiff = new SQLDiff();
		runDiff(sqldiff);
		Assert.assertEquals(1, sqldiff.getLastDiffCount());

		//run diff 2nd time - should have no difference
		runDiff(sqldiff);
		Assert.assertEquals(0, sqldiff.getLastDiffCount());
	}
	
	void execDDLId2(String ddl) throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(DiffApplyTest.class.getResourceAsStream("db2.properties"));
		p.setProperty("sqlrun.exec.50.statement", ddl);
		p.setProperty("sqlrun.connpropprefix","sqlrun.db2");
		Executor sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
	}
	
	void runDiff(SQLDiff sqldiff) throws Exception {
		Properties pd = new ParametrizedProperties();
		pd.load(DiffApplyTest.class.getResourceAsStream("diff1.properties"));
		pd.load(DiffApplyTest.class.getResourceAsStream("db1.properties"));
		pd.load(DiffApplyTest.class.getResourceAsStream("db2.properties"));
		sqldiff.doMain(TestUtil.NULL_PARAMS, pd);
	}
	
}
