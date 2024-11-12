package tbrugz.sqldiff;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.compare.ExecOrderDiffComparator;
import tbrugz.sqldump.dbmodel.Table;

public class CompareTest {

	@Test
	public void testExecOrder() {
		Table o1 = new Table(); o1.setName("o1");
		Table o2 = new Table(); o2.setName("o2");
		Assert.assertEquals(-1, ExecOrderDiffComparator.compare(o1, o2));

		Table o0 = new Table(); o0.setName("o0");
		Assert.assertEquals(1, ExecOrderDiffComparator.compare(o1, o0));
		
		o0.setSchemaName("schema");
		Assert.assertEquals(-1, ExecOrderDiffComparator.compare(o1, o0));
		Assert.assertEquals(1, ExecOrderDiffComparator.compare(o0, o1));
		Assert.assertEquals(0, ExecOrderDiffComparator.compare(o0, o0));
		Assert.assertEquals(0, ExecOrderDiffComparator.compare(o1, o1));
	}
	
}
