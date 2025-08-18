package tbrugz.sqldiff.model;

import java.util.Arrays;

import jakarta.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.FK.UpdateRule;

public class ConstraintDiffTest {

	SchemaModel sm1;
	SchemaDiffer differ = new SchemaDiffer();
	
	@Before
	public void before() {
		Table t = new Table();
		t.setName("a");
		t.getColumns().add(SchemaDiffTest.newColumn("c1", "int", 1, 1));
		
		Constraint c1 = new Constraint();
		c1.setName("unique_abc");
		c1.setType(ConstraintType.UNIQUE);
		c1.setUniqueColumns(Arrays.asList(new String[]{"c1"}));
		t.getConstraints().add(c1);
		
		FK fk1 = new FK();
		fk1.setFkTable("a");
		fk1.setPkTable("p");
		fk1.setFkColumns(Arrays.asList("c1"));
		fk1.setPkColumns(Arrays.asList("c1"));
		fk1.setName("fk1"); //XXX: can't compare FKs without names
		t.getForeignKeys().add(fk1);

		sm1 = new SchemaModel();
		sm1.getTables().add(t);
	}

	@Test
	public void testDiffAddConstraint() throws JAXBException {
		SchemaModel sm2 = SchemaDiffTest.jaxbClone(sm1);

		{
		Constraint c1 = new Constraint();
		c1.setName("unique_abc2");
		c1.setType(ConstraintType.UNIQUE);
		c1.setUniqueColumns(Arrays.asList(new String[]{"c1"}));
		sm2.getTables().iterator().next().getConstraints().add(c1);
		}

		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(1, sd.getChildren().size());
	}

	@Test
	public void testDiffEquals() throws JAXBException {
		SchemaModel sm2 = SchemaDiffTest.jaxbClone(sm1);

		{
		Constraint c1 = new Constraint();
		c1.setName("unique_xyz");
		c1.setType(ConstraintType.UNIQUE);
		c1.setUniqueColumns(Arrays.asList(new String[]{"c1"}));
		sm1.getTables().iterator().next().getConstraints().add(c1);
		}
		
		{
		Constraint c2 = new Constraint();
		c2.setName("unique_xyz");
		c2.setType(ConstraintType.UNIQUE);
		c2.setUniqueColumns(Arrays.asList(new String[]{"c1"}));
		sm2.getTables().iterator().next().getConstraints().add(c2);
		}

		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(0, sd.getChildren().size());
	}

	@Test
	public void testDiffNameChange() throws JAXBException {
		SchemaModel sm2 = SchemaDiffTest.jaxbClone(sm1);
		sm2.getTables().iterator().next().getConstraints().get(0).setName("unique_xyz");

		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(2, sd.getChildren().size());
	}

	@Test
	public void testFKOtherName() throws JAXBException {
		SchemaModel sm2 = SchemaDiffTest.jaxbClone(sm1);
		sm2.getTables().iterator().next().getForeignKeys().get(0).setName("fk2");

		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		Assert.assertEquals(2, sd.getChildren().size());
	}

	@Test
	public void testFKOnDelete() throws JAXBException {
		SchemaModel sm2 = SchemaDiffTest.jaxbClone(sm1);
		sm2.getTables().iterator().next().getForeignKeys().get(0).setDeleteRule(UpdateRule.CASCADE); 

		//System.out.println("sm1.fk="+sm1.getTables().iterator().next().getForeignKeys().get(0).getDefinition(false));
		//System.out.println("sm2.fk="+sm2.getTables().iterator().next().getForeignKeys().get(0).getDefinition(false));

		SchemaDiff sd = differ.diffSchemas(sm1, sm2);
		//System.out.println("diff = "+sd.getDiff());
		Assert.assertEquals(1, sd.getChildren().size());
		Assert.assertEquals(ChangeType.REPLACE, sd.getChildren().get(0).getChangeType());
	}

}
