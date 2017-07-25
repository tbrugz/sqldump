package tbrugz.sqldump.resultset.pivot;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.resultset.ResultSetListAdapter;
import tbrugz.sqldump.resultset.TestBean;
import tbrugz.sqldump.sqlrun.QueryDumper;

@SuppressWarnings("rawtypes")
public class PivotRSTest {

	static final Log log = LogFactory.getLog(PivotRSTest.class);

	List<TestBean> l1;
	
	@Before
	public void init() {
		TestBean b1 = new TestBean(1, "one", "c1", 10);
		TestBean b2 = new TestBean(2, "two", "c1", 20);
		TestBean b3 = new TestBean(3, "three", "c2", 30);
		TestBean b4 = new TestBean(4, "two", "c3", 40);
		
		l1 = new ArrayList<TestBean>();
		l1.add(b1); l1.add(b2); l1.add(b3); l1.add(b4);
	}
	
	@Test
	public void test01() throws SQLException, IntrospectionException, IOException {
		log.info("--> test01()");
		List<String> colsNotToPivot = Arrays.asList(new String[]{"id"});
		final Map<String, Comparable> colsToPivot = new LinkedHashMap<String, Comparable>();
		for(String s: new String[]{"description", "category"}) {
			colsToPivot.put(s, null);
		}

		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		//SQLUtils.dumpRS(rsla); rsla.first();
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot, false);
		prs.process();
		
		log.info("keyColValues: "+prs.keyColValues);
		log.info("nonPivotKeyValues: "+prs.nonPivotKeyValues);
		log.info("measures: "+prs.measureCols);
		
		int colcount = prs.getMetaData().getColumnCount();
		log.info("colcount: "+colcount);
		List<String> cols = new ArrayList<String>();
		for(int i=1;i<=colcount;i++) {
			cols.add(prs.getMetaData().getColumnName(i));
		}
		log.info("cols: "+cols);
		
		QueryDumper.simplerRSDump(prs); prs.beforeFirst();
		
		int rowCounter = 0;
		while(prs.next()) {
			log.info("row:: key="+prs.currentNonPivotKey+" / id="+prs.getString("id")+" / category:c1|description:two="+prs.getString("description"+PivotResultSet.COLVAL_SEP+"two"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c1"));
			rowCounter++;
		}
		log.info("row count: "+rowCounter);
		
		log.info("originalRowCount: "+prs.getOriginalRowCount()+" ; new rowCount: "+prs.getRowCount());

		prs.beforeFirst();
		prs.next();
		Assert.assertEquals("1", prs.getString("id"));
		Assert.assertEquals("10", prs.getString("description"+PivotResultSet.COLVAL_SEP+"one"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals(null, prs.getString("description"+PivotResultSet.COLVAL_SEP+"one"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c3"));
		try {
			String col = "description"+PivotResultSet.COLVAL_SEP+"one"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c4";
			prs.getString(col);
			Assert.fail("column '"+col+"' does not exist");
		}
		catch(SQLException e) {}
		prs.next();
		Assert.assertEquals("2", prs.getString("id"));
		Assert.assertEquals(null, prs.getString("description"+PivotResultSet.COLVAL_SEP+"one"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals("20", prs.getString("description"+PivotResultSet.COLVAL_SEP+"two"+PivotResultSet.COLS_SEP+"category"+PivotResultSet.COLVAL_SEP+"c1"));
	}

	@Test
	public void test02() throws SQLException, IntrospectionException, IOException {
		log.info("--> test02()");
		List<String> colsNotToPivot = Arrays.asList(new String[]{"id", "description"});
		final Map<String, Comparable> colsToPivot = new HashMap<String, Comparable>();
		for(String s: new String[]{"category"}) {
			colsToPivot.put(s, null);
		}

		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot, false);
		prs.process();
		
		int colcount = prs.getMetaData().getColumnCount();
		log.info("colcount: "+colcount);
		List<String> cols = new ArrayList<String>();
		for(int i=1;i<=colcount;i++) {
			cols.add(prs.getMetaData().getColumnName(i));
		}
		log.info("cols: "+cols);
		
		QueryDumper.simplerRSDump(prs); prs.beforeFirst();
		
		prs.next();
		Assert.assertEquals("1", prs.getString("id"));
		Assert.assertEquals("10", prs.getString("category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals(null, prs.getString("category"+PivotResultSet.COLVAL_SEP+"c2"));
		prs.next();
		prs.next();
		Assert.assertEquals("3", prs.getString("id"));
		Assert.assertEquals(null, prs.getString("category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals("30", prs.getString("category"+PivotResultSet.COLVAL_SEP+"c2"));
	}

	@Test
	public void test03() throws SQLException, IntrospectionException, IOException {
		log.info("--> test03()");
		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		List<String> colsNotToPivot = Arrays.asList(new String[]{"id", "category"});
		List<String> colsToPivot = Arrays.asList(new String[]{"description"});
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot, false);
		prs.process();
		
		QueryDumper.simplerRSDump(prs); prs.beforeFirst();

		prs.next();
		Assert.assertEquals("1", prs.getString("id"));
		Assert.assertEquals("10", prs.getString("description"+PivotResultSet.COLVAL_SEP+"one"));
		Assert.assertEquals(null, prs.getString("description"+PivotResultSet.COLVAL_SEP+"three"));
		prs.next();
		prs.next();
		Assert.assertEquals("3", prs.getString("id"));
		Assert.assertEquals(null, prs.getString("description"+PivotResultSet.COLVAL_SEP+"one"));
		Assert.assertEquals("30", prs.getString("description"+PivotResultSet.COLVAL_SEP+"three"));
	}

	@Test
	public void test04() throws SQLException, IntrospectionException, IOException {
		log.info("--> test04()");
		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		List<String> colsNotToPivot = Arrays.asList(new String[]{"description", "id"});
		List<String> colsToPivot = Arrays.asList(new String[]{"category"});
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot, false);
		prs.process();
		
		QueryDumper.simplerRSDump(prs); prs.beforeFirst();

		prs.next();
		Assert.assertEquals("1", prs.getString("id"));
		Assert.assertEquals("10", prs.getString("category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals(null, prs.getString("category"+PivotResultSet.COLVAL_SEP+"c2"));
		prs.next();
		prs.next();
		prs.next();
		Assert.assertEquals("4", prs.getString("id"));
		Assert.assertEquals(null, prs.getString("category"+PivotResultSet.COLVAL_SEP+"c1"));
		Assert.assertEquals("40", prs.getString("category"+PivotResultSet.COLVAL_SEP+"c3"));
	}
}
