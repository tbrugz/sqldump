package tbrugz.sqldiff.test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldiff.io.JSONDiffIO;
import tbrugz.sqldiff.io.XMLDiffIO;
import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.util.ConnectionUtil;

public class SQLDiffTest {
	
	//TODO: test with different databases
	//see: http://www.mkyong.com/unittest/junit-4-tutorial-6-parameterized-test/
	
	static String DIR_OUT = "target/work/output/SQLDiffTest";
	
	static String dbURL = null;
	static String dbDriver = null;
	static String dbUser = null;
	static String dbPassword = "h";

	SchemaModelGrabber schemaJdbcGrabber;
	Connection conn;
	SchemaModel smOriginal;
	SchemaDiffer differ = new SchemaDiffer();

	@Before
	public void setupConnProperties() throws SQLException {
		//'jdbc:h2:mem:SQLDiffTest'? no 'name' means a private database
		//see http://www.h2database.com/html/features.html#in_memory_databases
		dbURL = "jdbc:h2:mem:";
		dbDriver = "org.h2.Driver";
		dbUser = "h";
	}
	
	void setup4diff() throws Exception {
		if(conn!=null) {
			conn.close(); //removes database (for H2)
		}
		schemaJdbcGrabber = new JDBCSchemaGrabber();
		Properties jdbcPropNew = new Properties();
		String[] jdbcGrabParams = {
				"-Dsqldump.driverclass="+dbDriver,
				"-Dsqldump.dburl="+dbURL,
				"-Dsqldump.user="+dbUser,
				"-Dsqldump.password="+dbPassword,
				"-Dsqldump.schemagrab.db-specific-features=true",
		};
		TestUtil.setProperties(jdbcPropNew, jdbcGrabParams);
		schemaJdbcGrabber.setProperties(jdbcPropNew);
		
		conn = ConnectionUtil.initDBConnection("sqldump", jdbcPropNew);
		
		SQLRunAndDumpTest.setupModel(conn);
		
		schemaJdbcGrabber.setConnection(conn);
		
		smOriginal = schemaJdbcGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 3 tables", 3, smOriginal.getTables().size());
	}
	
	@Test
	public void testDiffAddColumn() throws Exception {
		setup4diff();
		Statement st = conn.createStatement();
		st.executeUpdate("alter table emp add column email varchar(100)");
		
		List<Diff> diffs = null;
		SchemaDiff schemaDiff = null;
		Diff diff1st = null;
		Diff dinv = null;
		
		//test diff size
		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 1", 1, diffs.size());
		}
		
		//test diff(0) type
		{
		diff1st = diffs.get(0);
		Assert.assertEquals("diff type should be ADD", ChangeType.ADD, diff1st.getChangeType());
		Assert.assertEquals("diff object type should be COLUMN", DBObjectType.COLUMN, diff1st.getObjectType());
		}
		
		//test inverse diff(0)
		{
		dinv = schemaDiff.inverse();
		System.out.println("diff inverse:\n"+dinv.getDiff());
		dinv = diffs.get(0).inverse();
		Assert.assertEquals("diff type should be DROP", ChangeType.DROP, dinv.getChangeType());
		Assert.assertEquals("diff object type should be COLUMN", DBObjectType.COLUMN, dinv.getObjectType());

		//rolling back db changes
		st.executeUpdate(dinv.getDiff());
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //XXX: remove? (restore database should not be the test responsability)
	}

	@Test
	public void testDiffXMLAndJSONOut() throws Exception {
		setup4diff();
		Statement st = conn.createStatement();
		st.executeUpdate("alter table emp add column email varchar(100)");
		
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		SchemaDiff schemaDiff = differ.diffSchemas(smOriginal, sm2);
		
		//test xml & json output
		XMLDiffIO xmlio = new XMLDiffIO();
		xmlio.dumpDiff(schemaDiff, new File(DIR_OUT+"/diff.xml"));
		JSONDiffIO jsonio = new JSONDiffIO();
		jsonio.dumpDiff(schemaDiff, new File(DIR_OUT+"/diff.json"));
	}
	
	@Test
	public void testDiffCreateTable() throws Exception {
		setup4diff();
		Statement st = conn.createStatement();
		//alter schema 
		st.executeUpdate("create table newt (abc integer)");
		SchemaDiff schemaDiff = null;
		List<Diff> diffs = null;
		Diff diff1st = null;
		Diff dinv = null;

		//get diff 
		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 1", 1, diffs.size());
		diff1st = diffs.get(0);
		}
		
		//get inverse diff 
		{
		dinv = schemaDiff.inverse();
		System.out.println("diff inverse:\n"+dinv.getDiff());
		dinv = diffs.get(0).inverse();
		Assert.assertEquals("diff type should be DROP", ChangeType.DROP, dinv.getChangeType());
		Assert.assertEquals("diff object type should be TABLE", DBObjectType.TABLE, dinv.getObjectType());

		//rolling back db changes
		st.executeUpdate(dinv.getDiff());
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //remove?
	}

	@Test
	public void testDiffCreateView() throws Exception {
		setup4diff();
		Statement st = conn.createStatement();
		//alter schema 
		st.executeUpdate("create view emp_view as select id, name from emp");
		SchemaDiff schemaDiff = null;
		List<Diff> diffs = null;
		Diff diff1st = null;
		Diff dinv = null;

		//get diff 
		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		Assert.assertEquals("model should have 1 view", 1, sm2.getViews().size());
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 1", 1, diffs.size());
		diff1st = diffs.get(0);
		}
		
		//get inverse diff 
		{
		dinv = schemaDiff.inverse();
		System.out.println("diff inverse:\n"+dinv.getDiff());
		dinv = diffs.get(0).inverse();
		Assert.assertEquals("diff type should be DROP", ChangeType.DROP, dinv.getChangeType());
		Assert.assertEquals("diff object type should be VIEW", DBObjectType.VIEW, dinv.getObjectType());

		//rolling back db changes
		st.executeUpdate(dinv.getDiff());
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //remove?
	}

	@Test
	public void testNoDiff() throws Exception {
		setup4diff();
		
		List<Diff> diffs = null;
		SchemaDiff schemaDiff = null;

		//test diff size
		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		schemaDiff = differ.diffSchemas(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals(0, diffs.size());
		}
	}
	
}
