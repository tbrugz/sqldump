package tbrugz.sqldiff.datadiff;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.resultset.ResultSetListAdapter;
import tbrugz.sqldump.resultset.TestBean;
import tbrugz.sqldump.util.CategorizedOut;

public class ResultSetDiffTest {

	static final Log log = LogFactory.getLog(ResultSetDiffTest.class);
	
	List<TestBean> l1;
	List<TestBean> l2;
	
	@Before
	public void init() {
		TestBean b1 = new TestBean(1, "one", "c1", 10);
		TestBean b2 = new TestBean(2, "two", "c1", 20);
		TestBean b3 = new TestBean(3, "three", "c2", 30);
		TestBean b4 = new TestBean(4, "two", "c3", 40);
		TestBean b5 = new TestBean(5, "one", "c3", 50);
		TestBean b6 = new TestBean(6, "three", "c4", 60);
		TestBean b2x = new TestBean(2, "two", "c1", 200);
		
		l1 = new ArrayList<TestBean>();
		l1.add(b1); l1.add(b2); l1.add(b3); l1.add(b4);

		l2 = new ArrayList<TestBean>();
		l2.add(b1); l2.add(b2x); l2.add(b3); l2.add(b5); l2.add(b6);
	}
	
	@Test
	public void testDumpUpdateDeleteAndEqualsCounts() throws IntrospectionException, SQLException, IOException {
		ResultSetListAdapter<TestBean> rsla1 = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		ResultSetListAdapter<TestBean> rsla2 = new ResultSetListAdapter<TestBean>("rsla2", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l2, TestBean.class);
		
		ResultSetDiff rsd = new ResultSetDiff();

		CategorizedOut cout = new CategorizedOut(CategorizedOut.NULL_WRITER);
		
		List<DiffSyntax> ds = DataDiff.getSyntaxes(new Properties(), false);
		
		log.info("s: 1 t: 2");
		rsd.diff(rsla1, rsla2, "table1", TestBean.getUniqueCols(), ds, cout);
		Assert.assertEquals(2, rsd.getDumpCount());
		Assert.assertEquals(1, rsd.getUpdateCount());
		Assert.assertEquals(1, rsd.getDeleteCount());
		Assert.assertEquals(2, rsd.getIdenticalRowsCount());
		
		log.info("s: 2 t: 1");
		rsla1.beforeFirst(); rsla2.beforeFirst();
		rsd.diff(rsla2, rsla1, "table2", TestBean.getUniqueCols(), ds, cout);
		Assert.assertEquals(1, rsd.getDumpCount());
		Assert.assertEquals(1, rsd.getUpdateCount());
		Assert.assertEquals(2, rsd.getDeleteCount());
		Assert.assertEquals(2, rsd.getIdenticalRowsCount());
	}

}
