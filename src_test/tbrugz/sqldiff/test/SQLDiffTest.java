package tbrugz.sqldiff.test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.dbmodel.SchemaModel;
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

	@Test
	public void testIdenticalModels() throws Exception {
		//depends on SQLRunAndDumpTest.doRun() ...
		SQLRunAndDumpTest randd = new SQLRunAndDumpTest();
		randd.doRunAndDumpModel();
		
		//xml serializer input Orig
		SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "work/output/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropOrig);
		SchemaModel smOrig = schemaSerialGrabber.grabSchema();

		//xml serializer input New
		Properties jaxbPropNew = new Properties();
		jaxbPropNew.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "work/output/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropNew);
		SchemaModel smNew = schemaSerialGrabber.grabSchema();
		
		//do diff
		SchemaDiff diff = SchemaDiff.diff(smOrig, smNew);
		System.out.println("diff:\n"+diff.getDiff());
		
		List<Diff> diffs = diff.getDiffList();
		Assert.assertEquals("diff size should be zero", 0, diffs.size());
		
		//List<DBObjectType> objtypeList = Arrays.asList(DBObjectType.TABLE, DBObjectType.COLUMN);
		//System.out.println("diff [types:"+objtypeList+"]\n"+diff.getDiffByDBObjectTypes(objtypeList));
	}

}
