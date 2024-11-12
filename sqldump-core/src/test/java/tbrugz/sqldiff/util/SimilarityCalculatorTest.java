package tbrugz.sqldiff.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public class SimilarityCalculatorTest {
	
	Table t1 = new Table();
	Table t2 = new Table();

	Column c1 = ColumnDiffTest.newColumn("c", "varchar", 10);
	Column c2 = ColumnDiffTest.newColumn("c", "varchar", 10);
	
	@Before
	public void setup() {
		t1.setName("t");
		t1.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
		t1.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));

		t2.setName("t");
		t2.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
		t2.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));
		
		c1.setOrdinalPosition(1); c2.setOrdinalPosition(1); 
	}
	
	@Test
	public void testTableSimColumnOrder() {
		double sim1 = SimilarityCalculator.instance().similarity(t1, t2);
		System.out.println("sim1 = "+sim1);

		Table t3 = new Table();
		t3.setName("t");
		t3.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));
		t3.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
		
		double sim2 = SimilarityCalculator.instance().similarity(t1, t3);
		System.out.println("sim2 = "+sim2);
		
		Assert.assertTrue(sim1>sim2);
	}

	@Test
	public void testTableSimOtherName() {
		double sim1 = SimilarityCalculator.instance().similarity(t1, t2);
		t2.setName("tx");
		double sim2 = SimilarityCalculator.instance().similarity(t1, t2);
		
		System.out.println("sim1 = "+sim1+" ; sim2 = "+sim2);
		Assert.assertTrue(sim1>sim2);
	}

	@Test
	public void testTableSimOtherSchema() {
		double sim1 = SimilarityCalculator.instance().similarity(t1, t2);
		t2.setSchemaName("abc");
		double sim2 = SimilarityCalculator.instance().similarity(t1, t2);
		
		System.out.println("sim1 = "+sim1+" ; sim2 = "+sim2);
		Assert.assertTrue(sim1>sim2);
	}

	@Test
	public void testColumnSimOtherName() {
		double sim1 = SimilarityCalculator.instance().similarity(c1, c1);
		c2.setName("c2");
		double sim2 = SimilarityCalculator.instance().similarity(c1, c2);
		
		System.out.println("sim1 = "+sim1+" ; sim2 = "+sim2);
		Assert.assertTrue(sim1>sim2);
	}

	@Test
	public void testColumnSimOtherPosition() {
		double sim1 = SimilarityCalculator.instance().similarity(c1, c1);
		c2.setOrdinalPosition(2);
		double sim2 = SimilarityCalculator.instance().similarity(c1, c2);
		
		System.out.println("sim1 = "+sim1+" ; sim2 = "+sim2);
		Assert.assertTrue(sim1>sim2);
	}

	@Test
	public void testColumnSimOtherType() {
		double sim1 = SimilarityCalculator.instance().similarity(c1, c1);
		c1.setType("char");
		double sim2 = SimilarityCalculator.instance().similarity(c1, c2);
		
		System.out.println("sim1 = "+sim1+" ; sim2 = "+sim2);
		Assert.assertTrue(sim1>sim2);
	}
}
