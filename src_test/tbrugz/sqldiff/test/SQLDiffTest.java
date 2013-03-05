package tbrugz.sqldiff.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;

public class SQLDiffTest {
	
	static final String dbPath = "mem:testIdenticalModels;DB_CLOSE_DELAY=-1";

	@BeforeClass
	public static void testIt() throws Exception {
		SQLRunAndDumpTest randd = new SQLRunAndDumpTest();
		randd.dbpath = dbPath;
		randd.doRunAndDumpModel();
	}
	
	@Test
	public void testIdenticalModels() {
		//xml serializer input Orig
		SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "work/output/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropOrig);
		SchemaModel smOrig = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());

		//xml serializer input New
		Properties jaxbPropNew = new Properties();
		jaxbPropNew.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "work/output/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropNew);
		SchemaModel smNew = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smNew.getTables().size());
		
		//do diff
		SchemaDiff diff = SchemaDiff.diff(smOrig, smNew);
		System.out.println("diff:\n"+diff.getDiff());
		
		List<Diff> diffs = diff.getChildren();
		Assert.assertEquals("diff size should be zero", 0, diffs.size());
	}
	
	SchemaModelGrabber schemaJdbcGrabber;
	Connection conn;
	SchemaModel smOriginal;
	
	void setup4diff() throws ClassNotFoundException, SQLException, NamingException {
		if(schemaJdbcGrabber==null) {
			schemaJdbcGrabber = new JDBCSchemaGrabber();
			Properties jdbcPropNew = new Properties();
			String[] jdbcGrabParams = {
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+dbPath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h",
					"-Dsqldump.usedbspecificfeatures=true"
			};
			TestUtil.setProperties(jdbcPropNew, jdbcGrabParams);
			schemaJdbcGrabber.procProperties(jdbcPropNew);
			
			conn = SQLUtils.ConnectionUtil.initDBConnection("sqldump", jdbcPropNew);
			DBMSResources.instance().updateMetaData(conn.getMetaData());
			schemaJdbcGrabber.setConnection(conn);
		}
		
		smOriginal = schemaJdbcGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOriginal.getTables().size());
	}
	
	@Test
	public void testDiffAddColumn() throws ClassNotFoundException, SQLException, NamingException {
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
		st.executeUpdate(dinv.getDiff()); //XXX: remove (restore database should not be the test responsability)
	}
		
	@Test
	public void testDiffCreateTable() throws ClassNotFoundException, SQLException, NamingException {
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
		st.executeUpdate(dinv.getDiff()); //remove
	}

	@Test
	public void testDiffCreateView() throws ClassNotFoundException, SQLException, NamingException {
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
		st.executeUpdate(dinv.getDiff()); //remove
	}
}
