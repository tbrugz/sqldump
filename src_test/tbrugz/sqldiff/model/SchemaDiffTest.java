package tbrugz.sqldiff.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.RenameDetector;
import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

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
		int renameCount = RenameDetector.detectAndDoColumnRenames(sd.getColumnDiffs(), 0.5);
		SchemaDiff.logInfo(sd);
		
		Assert.assertEquals(1, sd.getChildren().size());
	}
	
	public static Column newColumn(String name, String type, int precision, int position) {
		Column c = ColumnDiffTest.newColumn(name, type, precision, true);
		c.ordinalPosition = position;
		return c;
	}
	
}
