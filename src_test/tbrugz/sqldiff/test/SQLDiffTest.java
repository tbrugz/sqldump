package tbrugz.sqldiff.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
	
	static String dbURL = null;
	static String dbDriver = null;
	static String dbUser = null;
	static String dbPassword = "h";

	SchemaModelGrabber schemaJdbcGrabber;
	Connection conn;
	SchemaModel smOriginal;

	@Before
	public void setupConnProperties() throws SQLException {
		//'jdbc:h2:mem:SQLDiffTest'? no 'name' means a private database
		//see http://www.h2database.com/html/features.html#in_memory_databases
		dbURL = "jdbc:h2:mem:";
		dbDriver = "org.h2.Driver";
		dbUser = "h";
	}
	
	void setup4diff() throws ClassNotFoundException, SQLException, NamingException, IOException {
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
				"-Dsqldump.usedbspecificfeatures=true",
		};
		TestUtil.setProperties(jdbcPropNew, jdbcGrabParams);
		schemaJdbcGrabber.setProperties(jdbcPropNew);
		
		conn = ConnectionUtil.initDBConnection("sqldump", jdbcPropNew);
		
		SQLRunAndDumpTest.setupModel(conn);
		
		schemaJdbcGrabber.setConnection(conn);
		
		smOriginal = schemaJdbcGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOriginal.getTables().size());
	}
	
	@Test
	public void testDiffAddColumn() throws ClassNotFoundException, SQLException, NamingException, IOException {
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //XXX: remove? (restore database should not be the test responsability)
	}
		
	@Test
	public void testDiffCreateTable() throws ClassNotFoundException, SQLException, NamingException, IOException {
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //remove?
	}

	@Test
	public void testDiffCreateView() throws ClassNotFoundException, SQLException, NamingException, IOException {
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
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
		schemaDiff = SchemaDiff.diff(smOriginal, sm2);
		System.out.println("diff:\n"+schemaDiff.getDiff());
		diffs = schemaDiff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}
		
		//test if generated diff runs ok on initial model
		st.executeUpdate(diff1st.getDiff());
		st.executeUpdate(dinv.getDiff()); //remove?
	}
}
