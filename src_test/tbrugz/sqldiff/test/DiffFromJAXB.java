package tbrugz.sqldiff.test;

import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class DiffFromJAXB {
	
	@Test
	public void testIdenticalModelsFromJAXB() {
		//xml serializer input Orig
		SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		Properties jaxbPropOrig = new Properties();
		jaxbPropOrig.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "test/jaxb/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropOrig);
		SchemaModel smOrig = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());

		//xml serializer input New
		Properties jaxbPropNew = new Properties();
		jaxbPropNew.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INFILE, "test/jaxb/empdept.jaxb.xml");
		schemaSerialGrabber.procProperties(jaxbPropNew);
		SchemaModel smNew = schemaSerialGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smNew.getTables().size());
		
		//do diff
		SchemaDiff diff = SchemaDiff.diff(smOrig, smNew);
		System.out.println("diff:\n"+diff.getDiff());
		
		List<Diff> diffs = diff.getChildren();
		Assert.assertEquals("diff size should be zero", 0, diffs.size());
	}

}
