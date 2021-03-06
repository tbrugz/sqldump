package tbrugz.sqldump.processors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ParametrizedProperties;

public class CascadingDataDumpTest {

	String OUTDIR = "work/output/CDD";
	
	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("setupdb.properties"));
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
	}
	
	@Test
	public void dumpCascadingFromEmpWithDept2() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("cdd1.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		
		//added "2" to linecount: header line + last linefeed
		
		int countE = TestUtil.countLines(OUTDIR+"/t1/data_EMP.csv");
		Assert.assertEquals(3+2, countE); //3 EMPs

		int countD = TestUtil.countLines(OUTDIR+"/t1/data_DEPT.csv");
		Assert.assertEquals(1+2, countD); //1 DEPT
	}

	@Test
	public void dumpCascadingFromEmpWithDept2AndEmpSalaryLessThan1500() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("cdd2.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		
		int countE = TestUtil.countLines(OUTDIR+"/t2/data_EMP.csv");
		Assert.assertEquals(2+2, countE); //2 EMPs

		int countD = TestUtil.countLines(OUTDIR+"/t2/data_DEPT.csv");
		Assert.assertEquals(1+2, countD); //1 DEPT
	}

	@Test
	public void dumpCascadingFromEmp1AndProj3() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("cdd3.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		
		int countE = TestUtil.countLines(OUTDIR+"/t3/data_DEPT.csv");
		Assert.assertEquals(2+2, countE); //2 DEPTs
	}
	
}
