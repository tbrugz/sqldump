package tbrugz.sqldump.dbmsfeatures;

import java.io.IOException;
import java.sql.Connection;
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
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ConnectionUtil;

public class FunctionTest {

	static final Log log = LogFactory.getLog(FunctionTest.class);
	static final String PROP_FILE = "FunctionTest.properties"; 
	static final String DIR_OUT = "work/output/FunctionTest/";

	Properties prop = new Properties();

	static final String DDL_MY_SQRT = "create alias MY_SQRT for \"java.lang.Math.sqrt\";";
	static final String DDL_REVERSE = "create alias REVERSE as $$ String reverse(String s) { return new StringBuilder(s).reverse().toString(); } $$;";
	static final String DDL_AGG_COUNT = "create aggregate SIMPLE_COUNT for \"tbrugz.sqldump.dbmsfeatures.H2CountAggregate\";";

	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new Properties();
		p.load(TriggerTest.class.getResourceAsStream(PROP_FILE));
		String[] vmparams = {
				"-Dsqlrun.exec.01.statement="+DDL_MY_SQRT,
				"-Dsqlrun.exec.02.statement="+DDL_REVERSE,
				"-Dsqlrun.exec.03.statement="+DDL_AGG_COUNT,
				};
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, p);
	}

	@Before
	public void before() throws IOException {
		prop.load(TriggerTest.class.getResourceAsStream(PROP_FILE));
	}
	
	@Test
	public void testQuery() throws Exception {
		String[] params = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries, JAXBSchemaXMLSerializer, SchemaModelScriptDumper",
				"-Dsqldump.schemagrab.db-specific-features=true",
				"-Dsqldump.queries=q1, q2, q3, c1, sq1, sq2",
				"-Dsqldump.query.q1.sql=select count(c1), simple_count(c1) from (select 1 as c1 union all select 1 as c1) a",
				"-Dsqldump.query.q2.sql=select reverse('abcd')",
				"-Dsqldump.query.q3.sql=select my_sqrt(9)",
				"-Dsqldump.query.c1.sql=call my_sqrt(9)",
				"-Dsqldump.query.sq1.sql=select * from information_schema.tables",
				"-Dsqldump.query.sq2.sql=select * from information_schema.views",
				"-Dsqldump.datadump.dumpsyntaxes=csv, ffc",
				"-Dsqldump.datadump.ffc.nullvalue=<null>", 
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename].[syntaxfileext]",
				"-Dsqldump.xmlserialization.jaxb.outfile="+DIR_OUT+"/FunctionTestModel.jaxb.xml",
				"-Dsqldump.schemadump.outputfilepattern="+DIR_OUT+"/FunctionTestModel.sql",
				};
		TestUtil.setProperties(prop, params);
		SQLDump sqld = new SQLDump();
		sqld.doMain(TestUtil.NULL_PARAMS, prop);
	}
	
	@Test
	public void testGetSchema() throws ClassNotFoundException, SQLException, NamingException {
		String[] params = {
				"-Dsqldump.schemagrab.db-specific-features=true",
				};
		TestUtil.setProperties(prop, params);

		JDBCSchemaGrabber sg = new JDBCSchemaGrabber();
		sg.setProperties(prop);
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", prop);
		sg.setConnection(conn);
		SchemaModel sm = sg.grabSchema();
		Set<ExecutableObject> execs = sm.getExecutables();
		Assert.assertEquals(3, execs.size());
		
		Iterator<ExecutableObject> it = execs.iterator();
		
		int count = 1;
		{
			ExecutableObject eo = it.next();
			Assert.assertEquals("my_sqrt", eo.getName().toLowerCase());
			log.info(">> exec["+count+"]:\n"+eo.getDefinition(true));
			Assert.assertEquals(DDL_MY_SQRT, eo.getDefinition(true));
			count++;
		}

		{
			ExecutableObject eo = it.next();
			Assert.assertEquals("reverse", eo.getName().toLowerCase());
			log.info(">> exec["+count+"]:\n"+eo.getDefinition(true));
			Assert.assertEquals(DDL_REVERSE, eo.getDefinition(true));
			count++;
		}

		{
			ExecutableObject eo = it.next();
			Assert.assertEquals("simple_count", eo.getName().toLowerCase());
			log.info(">> exec["+count+"]:\n"+eo.getDefinition(true));
			Assert.assertEquals(DDL_AGG_COUNT, eo.getDefinition(true));
			count++;
		}
	}

}
