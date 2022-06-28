package tbrugz.sqldump.mondrianschema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ParametrizedProperties;

public class MondrianTests {

	static final String[] NULL_PARAMS = null;
	static final String OUTDIR = "target/test-output/mondrian";
	
	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new ParametrizedProperties();
		//System.out.println("current dir == "+System.getProperty("user.dir"));
		p.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, ".");
		p.load(MondrianTests.class.getResourceAsStream("/setupdb.properties"));
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(NULL_PARAMS, p);
	}
	
	@Test
	public void dumpSchemaAndValidate() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(MondrianTests.class.getResourceAsStream("mondrian1.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(NULL_PARAMS, p);
	}

	@Test
	public void dumpOlapMDXQuery() throws IOException, ClassNotFoundException, SQLException, NamingException {
		Properties p = new ParametrizedProperties();
		p.load(MondrianTests.class.getResourceAsStream("mondrian2.properties"));
		p.setProperty("baseoutdir", OUTDIR);
		SQLDump sqld = new SQLDump();
		sqld.doMain(NULL_PARAMS, p);
	}
	
}
