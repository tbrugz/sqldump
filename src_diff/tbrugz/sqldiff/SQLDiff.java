package tbrugz.sqldiff;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelGrabber;
import tbrugz.sqldump.dbmodel.DBObjectType;

public class SQLDiff {
	
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

	void doIt() throws Exception {
		//SQLDump sdd = new SQLDump();
		//SchemaModel sm = sdd.grabSchema();
		
		//xml serializer input Orig
		SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "output/"+"model1.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropOrig);
		SchemaModel smOrig = schemaSerialGrabber.grabSchema();

		//xml serializer input New
		Properties jaxbPropNew = new Properties();
		jaxbPropNew.setProperty(JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "output/"+"model2.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropNew);
		SchemaModel smNew = schemaSerialGrabber.grabSchema();
		
		//do diff
		SchemaDiff diff = SchemaDiff.diff(smOrig, smNew);
		System.out.println("=========+=========+=========+=========+=========+=========+=========+=========");
		System.out.println("diff:\n"+diff.getDiff());
		System.out.println("=========+=========+=========+=========+=========+=========+=========+=========");
		List<DBObjectType> objtypeList = Arrays.asList(DBObjectType.TABLE, DBObjectType.COLUMN);
		System.out.println("diff [types:"+objtypeList+"]\n"+diff.getDiffByDBObjectTypes(objtypeList));
	}
	
	public static void main(String[] args) throws Exception {
		new SQLDiff().doIt();
	}
}
