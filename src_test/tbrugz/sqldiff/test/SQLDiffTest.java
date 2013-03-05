package tbrugz.sqldiff.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
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
	
	//pgsql: file:///D:/apps/PostgreSQL/9.0/doc/postgresql/html/sql-altertable.html

	//add column, alter column, drop column
	/*
	Set<Table> tables = new TreeSet<Table>();
	Set<Sequence> sequences = new TreeSet<Sequence>(); ??

	Set<FK> foreignKeys = new TreeSet<FK>();
	Set<View> views = new TreeSet<View>();
	Set<Trigger> triggers = new TreeSet<Trigger>();
	Set<ExecutableObject> executables = new TreeSet<ExecutableObject>();
	Set<Synonym> synonyms = new TreeSet<Synonym>();
	Set<Index> indexes = new TreeSet<Index>();
	 */
	String dbPath = "mem:testIdenticalModels;DB_CLOSE_DELAY=-1";

	@Test
	public void testIt() throws Exception {
		//depends on SQLRunAndDumpTest.doRun() ...
		SQLRunAndDumpTest randd = new SQLRunAndDumpTest();
		randd.dbpath = this.dbPath;
		randd.doRunAndDumpModel();
		
		testIdenticalModels();
		testDifferentModels();
	}
	
	void testIdenticalModels() {
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
	
	void testDifferentModels() throws ClassNotFoundException, SQLException, NamingException {
		
		//List<DBObjectType> objtypeList = Arrays.asList(DBObjectType.TABLE, DBObjectType.COLUMN);
		//System.out.println("diff [types:"+objtypeList+"]\n"+diff.getDiffByDBObjectTypes(objtypeList));

		SchemaModelGrabber schemaJdbcGrabber = new JDBCSchemaGrabber();
		Properties jdbcPropNew = new Properties();
		String[] jdbcGrabParams = {
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbPath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
		};
		TestUtil.setProperties(jdbcPropNew, jdbcGrabParams);
		schemaJdbcGrabber.procProperties(jdbcPropNew);
		
		Connection conn = SQLUtils.ConnectionUtil.initDBConnection("sqldump", jdbcPropNew);
		DBMSResources.instance().updateMetaData(conn.getMetaData());
		schemaJdbcGrabber.setConnection(conn);
		
		/*DBMSResources res = DBMSResources.instance();
		DBMSFeatures feat = res.databaseSpecificFeaturesClass();
		System.out.println("conn = "+conn+"; res = "+res+"; feat = "+feat);*/
		
		SchemaModel sm1 = schemaJdbcGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, sm1.getTables().size());
		
		Statement st = conn.createStatement();
		st.executeUpdate("alter table emp add column email varchar(100)");
		//conn.commit();

		List<Diff> diffs = null;
		SchemaDiff diff = null;
		
		//test diff size
		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		diff = SchemaDiff.diff(sm1, sm2);
		System.out.println("diff:\n"+diff.getDiff());
		diffs = diff.getChildren();
		Assert.assertEquals("diff size should be 1", 1, diffs.size());
		}
		
		//test diff(0) type
		{
		Diff d = diffs.get(0);
		Assert.assertEquals("diff type should be ADD", ChangeType.ADD, d.getChangeType());
		Assert.assertEquals("diff object type should be COLUMN", DBObjectType.COLUMN, d.getObjectType());
		}

		//test inverse diff(0)
		{
		Diff dinv = diff.inverse();
		System.out.println("diff inverse:\n"+dinv.getDiff());
		dinv = diffs.get(0).inverse();
		Assert.assertEquals("diff type should be DROP", ChangeType.DROP, dinv.getChangeType());
		Assert.assertEquals("diff object type should be COLUMN", DBObjectType.COLUMN, dinv.getObjectType());

		//rolling back db changes
		st.executeUpdate(dinv.getDiff());
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		diff = SchemaDiff.diff(sm1, sm2);
		System.out.println("diff:\n"+diff.getDiff());
		diffs = diff.getChildren();
		Assert.assertEquals("diff size should be 0", 0, diffs.size());
		}

		//st = conn.createStatement();
		st.executeUpdate("create table newt (abc integer)");
		//conn.commit();

		{
		SchemaModel sm2 = schemaJdbcGrabber.grabSchema();
		diff = SchemaDiff.diff(sm1, sm2);
		System.out.println("diff:\n"+diff.getDiff());
		diffs = diff.getChildren();
		Assert.assertEquals("diff size should be 1", 1, diffs.size());
		}
		
		{
		Diff dinv = diff.inverse();
		System.out.println("diff inverse:\n"+dinv.getDiff());
		dinv = diffs.get(0).inverse();
		Assert.assertEquals("diff type should be DROP", ChangeType.DROP, dinv.getChangeType());
		Assert.assertEquals("diff object type should be TABLE", DBObjectType.TABLE, dinv.getObjectType());
		}
		
	}

}
