package tbrugz.sqldiff;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.naming.NamingException;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.junit.Test;

import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class SQLDiffMainTest {

	String OUTDIR = "work/output/SQLDiffMainTest";
	
	@Test
	public void testMain1() throws IOException, ClassNotFoundException, SQLException, NamingException, JAXBException, XMLStreamException, InterruptedException, ExecutionException {
		Properties p = new ParametrizedProperties();
		p.load(SQLDiffMainTest.class.getResourceAsStream("diff1.properties"));
		p.setProperty("outputdir", OUTDIR);
		SQLDiff sqld = new SQLDiff();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
	}

}
