package tbrugz.sqldump.resultset.pivot;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.resultset.ResultSetListAdapter;
import tbrugz.sqldump.resultset.TestBean;
import tbrugz.sqldump.sqlrun.QueryDumper;

public class PivotRSTest {

	static final Log log = LogFactory.getLog(PivotRSTest.class);

	//final String[] colsNotToPivotArr = {"id"}; //{"a", "b"};
	//final String[] colsToPivotArr = {"description", "category"}; //"c", "d"};

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
		List<String> colsNotToPivot = Arrays.asList(new String[]{"id"});
		final Map<String, Comparable> colsToPivot = new HashMap<String, Comparable>();
		for(String s: new String[]{"description", "category"}) {
			colsToPivot.put(s, null);
		}

		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		//SQLUtils.dumpRS(rsla); rsla.first();
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot);
		prs.process();
		
		log.info("keyColValues: "+prs.keyColValues);
		log.info("nonPivotKeyValues: "+prs.nonPivotKeyValues);
		log.info("measures: "+prs.measureCols);
		
		/*log.info(prs.currentNonPivotKey);
		prs.next();
		log.info(prs.currentNonPivotKey);
		prs.next();
		log.info(prs.currentNonPivotKey);*/
		
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
			log.info("row:: key="+prs.currentNonPivotKey+" / id="+prs.getString("id")+" / category:c1|description:two="+prs.getString("category:c1|description:two"));
			rowCounter++;
		}
		log.info("row count: "+rowCounter);
		
		log.info("originalRSRowCount: "+prs.originalRSRowCount+" ; new rowCount: "+prs.rowCount);
	}

	@Test
	public void test02() throws SQLException, IntrospectionException, IOException {
		List<String> colsNotToPivot = Arrays.asList(new String[]{"id", "description"});
		final Map<String, Comparable> colsToPivot = new HashMap<String, Comparable>();
		for(String s: new String[]{"category"}) {
			colsToPivot.put(s, null);
		}

		ResultSetListAdapter<TestBean> rsla = new ResultSetListAdapter<TestBean>("testbeanLA", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		
		//SQLUtils.dumpRS(rsla); rsla.first();
		QueryDumper.simplerRSDump(rsla); rsla.beforeFirst();
		
		PivotResultSet prs = new PivotResultSet(rsla, colsNotToPivot, colsToPivot);
		prs.process();
		
		int colcount = prs.getMetaData().getColumnCount();
		log.info("colcount: "+colcount);
		List<String> cols = new ArrayList<String>();
		for(int i=1;i<=colcount;i++) {
			cols.add(prs.getMetaData().getColumnName(i));
		}
		log.info("cols: "+cols);
		
		QueryDumper.simplerRSDump(prs); prs.beforeFirst();
	}
}
