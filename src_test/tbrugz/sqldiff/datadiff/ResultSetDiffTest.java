package tbrugz.sqldiff.datadiff;

import java.beans.IntrospectionException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.resultset.ResultSetListAdapter;
import tbrugz.sqldump.resultset.TestBean;

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
	public void test01() throws IntrospectionException, SQLException {
		ResultSetListAdapter<TestBean> rsla1 = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		ResultSetListAdapter<TestBean> rsla2 = new ResultSetListAdapter<TestBean>("rsla2", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l2, TestBean.class);
		
		ResultSetDiff rsd = new ResultSetDiff();

		log.info("s: 1 t: 2");
		rsd.diff(rsla1, rsla2, TestBean.getUniqueCols());
		
		log.info("s: 2 t: 1");
		rsla1.beforeFirst(); rsla2.beforeFirst();
		rsd.diff(rsla2, rsla1, TestBean.getUniqueCols());
	}

}
