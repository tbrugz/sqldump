package tbrugz.sqldump.mondrianschema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.processors.CascadingDataDump;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ParametrizedProperties;

public class MondrianTests {

	String OUTDIR = "work/output/mondrian";
	
	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(CascadingDataDump.class.getResourceAsStream("setupdb.properties"));
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
	}
	
	@Test
	public void dumpSchemaAndValidate() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(MondrianTests.class.getResourceAsStream("mondrian1.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
	}

	@Test
	public void dumpOlapMDXQuery() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(MondrianTests.class.getResourceAsStream("mondrian2.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
	}
	
}
