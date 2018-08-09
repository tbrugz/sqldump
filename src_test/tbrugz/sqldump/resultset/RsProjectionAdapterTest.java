package tbrugz.sqldump.resultset;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.datadump.FFCDataDump;

public class RsProjectionAdapterTest {
	
	List<TestBean> l1;
	//List<TestBean> l2;
	
	@Before
	public void init() {
		TestBean b1 = new TestBean(1, "one", "c1", 10);
		TestBean b2 = new TestBean(2, "two", "c1", 20);
		//TestBean b3 = new TestBean(3, "three", "c2", 30);
		//TestBean b4 = new TestBean(4, "two", "c3", 40);
		//TestBean b5 = new TestBean(5, "one", "c3", 50);
		//TestBean b6 = new TestBean(6, "three", "c4", 60);
		//TestBean b2x = new TestBean(2, "two", "c1", 200);
		
		l1 = new ArrayList<TestBean>();
		l1.add(b1); l1.add(b2); //l1.add(b3); l1.add(b4);

		//l2 = new ArrayList<TestBean>();
		//l2.add(b1); l2.add(b2x); l2.add(b3); l2.add(b5); l2.add(b6);
	}

	// 	static String[] uniqueCols = {"id"}; static String[] allCols = {"description", "category", "measure"};
	
	@Test(expected=IllegalArgumentException.class)
	public void testProjectionErr() throws IntrospectionException, SQLException {
		ResultSetListAdapter<TestBean> rs = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		String[] projectedCols = {"category", "z2", "description"};
		
		ResultSet rspd = new ResultSetProjectionDecorator(rs, Arrays.asList(projectedCols));
		rspd.close();
	}
	
	@Test
	public void testProjection() throws IntrospectionException, SQLException, IOException {
		ResultSetListAdapter<TestBean> rs = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		String[] projectedCols = {"description", "category"};
		
		ResultSet rspd = new ResultSetProjectionDecorator(rs, Arrays.asList(projectedCols));
		
		/*StringWriter sw = new StringWriter();
		DataDumpUtils.dumpRS(new FFCDataDump(), rspd, "schema", "table", sw, false);
		System.err.println(sw);*/
		
		rspd.beforeFirst();
		
		ResultSetMetaData rsmd = rspd.getMetaData();
		Assert.assertEquals(2, rsmd.getColumnCount());
		Assert.assertEquals("description", rsmd.getColumnName(1));
		
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("one", rspd.getString(1));
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("c1", rspd.getString(2));
		Assert.assertEquals("c1", rspd.getString("category"));
		Assert.assertFalse(rspd.next());
		
		rspd.close();
	}

	@Test
	public void testProjectionAlteredOrder() throws IntrospectionException, SQLException, IOException {
		ResultSetListAdapter<TestBean> rs = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		String[] projectedCols = {"category", "description"};
		
		ResultSet rspd = new ResultSetProjectionDecorator(rs, Arrays.asList(projectedCols));
		
		ResultSetMetaData rsmd = rspd.getMetaData();
		
		Assert.assertEquals(2, rsmd.getColumnCount());
		Assert.assertEquals("category", rsmd.getColumnName(1));
		
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("c1", rspd.getString(1));
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("two", rspd.getString(2));
		Assert.assertFalse(rspd.next());
		
		rspd.close();
	}
	
	@Test
	public void testProjectionIgnoreInvalidColumns() throws IntrospectionException, SQLException, IOException {
		ResultSetListAdapter<TestBean> rs = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		String[] projectedCols = {"description", "id", "category", "etc"};
		//String[] projectedCols = {"id", "etc2", "etc", "category"};
		
		ResultSet rspd = new ResultSetProjectionDecorator(rs, Arrays.asList(projectedCols), true);
		
		/*StringWriter sw = new StringWriter();
		DataDumpUtils.dumpRS(new FFCDataDump(), rspd, "schema", "table", sw, false);
		System.err.println(sw);*/
		
		ResultSetMetaData rsmd = rspd.getMetaData();
		Assert.assertEquals(4, rsmd.getColumnCount());
		Assert.assertEquals("category", rsmd.getColumnName(3));
		
		rspd.beforeFirst();
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("one", rspd.getString(1));
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("c1", rspd.getString(3));
		Assert.assertEquals("c1", rspd.getString("category"));
		Assert.assertFalse(rspd.next());
		
		rspd.close();
	}
	
	@Test
	public void testProjectionIgnoreInvalidColumns2() throws IntrospectionException, SQLException, IOException {
		ResultSetListAdapter<TestBean> rs = new ResultSetListAdapter<TestBean>("rsla1", 
				TestBean.getUniqueCols(), TestBean.getAllCols(), 
				l1, TestBean.class);
		String[] projectedCols = {"id", "etc2", "etc", "category"};
		
		ResultSet rspd = new ResultSetProjectionDecorator(rs, Arrays.asList(projectedCols), true);
		
		StringWriter sw = new StringWriter();
		DataDumpUtils.dumpRS(new FFCDataDump(), rspd, "schema", "table", sw, false);
		System.err.println(sw);
		
		ResultSetMetaData rsmd = rspd.getMetaData();
		Assert.assertEquals(4, rsmd.getColumnCount());
		Assert.assertEquals("etc", rsmd.getColumnName(3));
		
		rspd.beforeFirst();
		Assert.assertTrue(rspd.next());
		Assert.assertEquals(1, rspd.getInt(1));
		Assert.assertTrue(rspd.next());
		Assert.assertEquals("c1", rspd.getString(4));
		Assert.assertFalse(rspd.next());
		
		rspd.close();
	}

}
