package tbrugz.sqldiff.model;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.RenameDetector;
import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.XMLSerializer;

public class SchemaDiffTest {
	
	SchemaModel sm1;
	SchemaDiffer differ = new SchemaDiffer();
	
	@Before
	public void before() {
		Table t = new Table();
		t.setName("a");
		t.getColumns().add(newColumn("c1", "int", 1, 1));
		
		sm1 = new SchemaModel();
		sm1.getTables().add(t);
	}

	@Test
	public void testNoDiff() {
		SchemaDiff sd = differ.diffSchemas(sm1, sm1);
		Assert.assertEquals(0, sd.getChildren().size());
	}

	@Test
	public void testColumnAddDrop() {
		Table t = new Table();
		t.setName("a");
		t.getColumns().add(newColumn("c2", "int", 1, 1));
		
		SchemaModel sm2 = new SchemaModel();
		sm2.getTables().add(t);
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(2, sd.getChildren().size());
	}

	@Test
	public void testColumnTwoAddDrop() {
		SchemaDiff sd = twoColumnsAddDrop();
		Assert.assertEquals(4, sd.getChildren().size());
	}

	@Test
	public void testColumnRenameTwoAddDrop() {
		SchemaDiff sd = twoColumnsAddDrop();
		RenameDetector.detectAndDoColumnRenames(sd.getColumnDiffs(), 0.5);
		SchemaDiff.logInfo(sd);
		Assert.assertEquals(2, sd.getChildren().size());
	}
	
	SchemaDiff twoColumnsAddDrop() {
		sm1.getTables().iterator().next().getColumns().add(newColumn("c1b", "int", 1, 2));
		
		Table t = new Table();
		t.setName("a");
		t.getColumns().add(newColumn("c2", "int", 1, 1));
		t.getColumns().add(newColumn("c2b", "int", 1, 2));
		
		SchemaModel sm2 = new SchemaModel();
		sm2.getTables().add(t);
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		return sd;
	}

	@Test
	public void testColumnRename() {
		Table t = new Table();
		t.setName("a");
		t.getColumns().add(newColumn("c2", "int", 1, 1));
		
		SchemaModel sm2 = new SchemaModel();
		sm2.getTables().add(t);
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		@SuppressWarnings("unused")
		int renameCount = RenameDetector.detectAndDoColumnRenames(sd.getColumnDiffs(), 0.5);
		SchemaDiff.logInfo(sd);
		
		Assert.assertEquals(1, sd.getChildren().size());
	}

	@Test
	public void testJaxbClone() throws JAXBException {
		SchemaModel sm2 = jaxbClone(sm1);
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(0, sd.getChildren().size());
	}

	@Test
	public void testTableRemarks() throws JAXBException {
		SchemaModel sm2 = jaxbClone(sm1);
		Table t = sm2.getTables().iterator().next();
		t.setRemarks("this is table A");
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(1, sd.getChildren().size());
		
		//System.out.println("diff: "+sd.getDiff());
	}

	@Test
	public void testColumnRemarks() throws JAXBException {
		ColumnDiff.updateFeatures(DBMSResources.instance().getSpecificFeatures("pgsql"));

		SchemaModel sm2 = jaxbClone(sm1);
		Table t = sm2.getTables().iterator().next();
		Column c = t.getColumns().get(0);
		c.setRemarks("this is column 'c1'");
		
		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(1, sd.getChildren().size());
		
		//System.out.println("diff: "+sd.getChildren().get(0));
		System.out.println("diff: "+sd.getDiff());

		// --- inverse...
		
		sd = differ.diffSchemas(sm2, sm1);
		Assert.assertEquals(1, sd.getChildren().size());
		
		System.out.println("diff: "+sd.getDiff());
	}

	/* --- */
	
	public static Column newColumn(String name, String type, int precision, int position) {
		Column c = ColumnDiffTest.newColumn(name, type, precision, true);
		c.setOrdinalPosition(position);
		return c;
	}
	
	public static String schema2string(Object model) throws JAXBException {
		XMLSerializer xmlser;
		xmlser = new XMLSerializer(JAXBSchemaXMLSerializer.JAXB_SCHEMA_PACKAGES);
		StringWriter sw = new StringWriter();
		xmlser.marshal(model, sw);
		return sw.toString();
	}

	public static Object string2schema(String model) throws JAXBException {
		XMLSerializer xmlser;
		xmlser = new XMLSerializer(JAXBSchemaXMLSerializer.JAXB_SCHEMA_PACKAGES);
		StringReader sr = new StringReader(model);
		return xmlser.unmarshal(sr);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T jaxbClone(T object) throws JAXBException {
		String s = schema2string(object);
		return (T) string2schema(s);
	}
	
}
