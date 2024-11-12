package tbrugz.sqldump.dbmsfeatures;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.ConnectionUtil;

public class TriggerTest {

	static final Log log = LogFactory.getLog(TriggerTest.class);
	static final String PROP_FILE = "TriggerTest.properties"; 

	Properties prop = new Properties();
	
	@BeforeClass
	public static void setupDB() throws Exception {
		Properties p = new Properties();
		p.load(TriggerTest.class.getResourceAsStream(PROP_FILE));
		String[] vmparams = {
				"-Dsqlrun.exec.01.statement=create table test1 ( id varchar(20) )",
				"-Dsqlrun.exec.02.statement=create trigger trig_ins before insert on test1 for each row call \"tbrugz.sqldump.dbmsfeatures.H2SimpleTrigger\"",
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
	public void testOK() throws Exception {
		String[] params = {
				"-Dsqlrun.exec.01.statement=insert into test1 values ('OK')",
				};
		TestUtil.setProperties(prop, params);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(TestUtil.NULL_PARAMS, prop);
	}

	@Test
	public void testError() throws Exception {
		String[] params = {
				"-Dsqlrun.exec.01.statement=insert into test1 values ('ERROR')",
				};
		TestUtil.setProperties(prop, params);
		SQLRun sqlr = new SQLRun();
		try {
			sqlr.doMain(TestUtil.NULL_PARAMS, prop);
		}
		catch(ProcessingException pe) {
			Assert.assertTrue("pe.getCause() should be instance of SQLException", pe.getCause() instanceof SQLException);
		}
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
		Set<Trigger> triggers = sm.getTriggers();
		Assert.assertEquals(1, triggers.size());
		Trigger t = triggers.iterator().next();
		Assert.assertEquals("trig_ins", t.getName().toLowerCase());
	}
	
}
