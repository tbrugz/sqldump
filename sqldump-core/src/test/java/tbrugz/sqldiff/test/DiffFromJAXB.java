package tbrugz.sqldiff.test;

import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.JSONSchemaSerializer;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class DiffFromJAXB {
	
	static String DIR = "src/test/resources/";
	
	@Test
	public void testIdenticalModelsFromJAXB() {
		//xml serializer input Orig
		SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE,
			DIR+"jaxb/empdept.jaxb.xml");
		schemaSerialGrabber.setProperties(jaxbPropOrig);
		SchemaModel smOrig = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());

		//xml serializer input New
		Properties jaxbPropNew = new Properties();
		jaxbPropNew.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE,
			DIR+"jaxb/empdept.jaxb.xml");
		schemaSerialGrabber.setProperties(jaxbPropNew);
		SchemaModel smNew = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smNew.getTables().size());
		
		//do diff
		SchemaDiffer sd = new SchemaDiffer();
		SchemaDiff diff = sd.diffSchemas(smOrig, smNew);
		System.out.println("diff:\n"+diff.getDiff());
		
		List<Diff> diffs = diff.getChildren();
		Assert.assertEquals("diff size should be zero", 0, diffs.size());
	}

	@Test
	public void testIdenticalModelsFromJAXBAndJSON() {
		//xml serializer input Orig
		SchemaModel smOrig = null;
		{
		SchemaModelGrabber jaxbGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE,
			DIR+"jaxb/empdept.jaxb.xml");
		jaxbGrabber.setProperties(jaxbPropOrig);
		smOrig = jaxbGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());
		}

		//json serializer input New
		SchemaModel smNew = null;
		{
		SchemaModelGrabber jsonGrabber = new JSONSchemaSerializer();
		Properties jsonPropNew = new Properties();
		jsonPropNew.setProperty(JSONSchemaSerializer.JSONSERIALIZATION_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE,
			DIR+"json/empdept.json");
		jsonGrabber.setProperties(jsonPropNew);
		smNew = jsonGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smNew.getTables().size());
		}
		
		//do diff
		SchemaDiffer sd = new SchemaDiffer();
		SchemaDiff diff = sd.diffSchemas(smOrig, smNew);
		System.out.println("diff:\n"+diff.getDiff());
		
		List<Diff> diffs = diff.getChildren();
		Assert.assertEquals("diff size should be zero", 0, diffs.size());
	}
	
}
