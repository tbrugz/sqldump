package tbrugz.sqldump.pivot;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.resultset.pivot.PivotResultSet;
import tbrugz.sqldump.resultset.pivot.PivotResultSet.Aggregator;
import tbrugz.sqldump.sqlrun.QueryDumper;

public class QueryTest {

	String pivotDriverClass = SQLPivotDriver.class.getName();
	Properties prop = new Properties(); 
	Connection conn;
	
	{
		try {
			prop.load(QueryTest.class.getResourceAsStream("pivot.properties"));
		} catch (IOException e) {
			throw new ExceptionInInitializerError(e);
		}		
	}
	
	@Before
	public void before() throws SQLException, ClassNotFoundException {
		PivotResultSet.showMeasuresInColumns = true;
		PivotResultSet.showMeasuresFirst = true;
		
		Class.forName(pivotDriverClass); //"tbrugz.sqldump.pivot.SQLPivotDriver");
		
		String url = "jdbc:sqlpivot:h2:mem:abc";
		conn = DriverManager.getConnection(url, null);
	}

	@After
	public void after() throws SQLException, ClassNotFoundException {
		conn.close();
	}
	
	Date makeSqlDate(int year, int month, int dayOfMonth) {
		return new java.sql.Date(new GregorianCalendar(year, month-1, dayOfMonth).getTime().getTime());
	}

	@Test
	public void testQ1NonPivot() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q1");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		//QueryDumper.simplerRSDump(rs);
		//rs.absolute(0);
		rs.next();
		Assert.assertEquals("FALSE", rs.getString("A"));
		Assert.assertEquals("FALSE", rs.getString("B"));
		Assert.assertEquals("FALSE", rs.getString("BOOL_OR"));
	}

	@Test
	public void testQ2Pivot() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q2");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(0);
		rs.next();
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:false"));
		Assert.assertEquals("true", rs.getString("B:true"));

		rs.next();
		Assert.assertEquals("true", rs.getString("A"));
		Assert.assertEquals("true", rs.getString("B:false"));
		Assert.assertEquals(true, rs.getObject("B:true"));
	}

	@Test
	public void testQ3Pivot2Cols() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q3");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:false|C:false"));
		Assert.assertEquals("true", rs.getString("B:false|C:true"));
	}
	
	@Test
	public void testQ4Pivot2Measures() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND|B:false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR|B:true"));
		
		PivotResultSet.showMeasuresFirst = false;
		//((PivotResultSet)rs).showMeasuresFirst = false;
		((PivotResultSet)rs).processMetadata();
		//rs.absolute(0);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:false|BOOL_AND"));
		Assert.assertEquals("true", rs.getString("B:true|BOOL_OR"));
	}

	@Test
	public void testQ5Aggregator() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q5");
		
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("true", rs.getString("B:true"));

		PivotResultSet.aggregator = Aggregator.FIRST;
		rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("B:true"));
	}

	@Test
	public void testQ6Integers() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q6");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals(2, rs.getObject("B"));
		Assert.assertEquals(2+1, rs.getObject("A:1"));
		
		rs.next();
		Assert.assertEquals(4, rs.getObject("B"));
		Assert.assertEquals(4+2, rs.getInt("A:2"));
	}

	@Test
	public void testQ7Date() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q7");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals(makeSqlDate(2013, 10, 8), rs.getObject("A"));
		Assert.assertEquals(1, rs.getObject("B:one"));
		Assert.assertEquals(2, rs.getObject("B:two"));
	}

	@Test
	public void testQ7DatePivotted() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q7p");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("one", rs.getObject("B"));
		Assert.assertEquals(1, rs.getObject("A:2013-10-08"));
		Assert.assertEquals(3, rs.getObject("A:2013-10-09"));

		rs.next();
		Assert.assertEquals("two", rs.getObject("B"));
		Assert.assertEquals(2, rs.getObject("A:2013-10-08"));
		Assert.assertEquals(4, rs.getObject("A:2013-10-09"));

		Assert.assertEquals(false, rs.next());
	}
	
	@Test
	public void testQ4MeasuresInColumnsFirst() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND|B:false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR|B:true"));
		
		//((PivotResultSet)rs).showMeasuresInColumns = false;
		PivotResultSet.showMeasuresInColumns = false;
		PivotResultSet.showMeasuresFirst = true; //both ways must be tested
		rs = conn.createStatement().executeQuery(sql);
		//((PivotResultSet)rs).showMeasuresFirst = true; //both ways must be tested
		//((PivotResultSet)rs).processMetadata();
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("BOOL_AND", rs.getObject("Measure"));
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:false"));
		Assert.assertEquals(false, rs.getObject("B:true"));

		rs.absolute(3);
		Assert.assertEquals("BOOL_OR", rs.getString("Measure"));
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:false"));
		Assert.assertEquals(true, rs.getObject("B:true"));
	}

	@Test
	public void testQ4MeasuresInColumnsLast() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND|B:false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR|B:true"));
		
		PivotResultSet.showMeasuresInColumns = false;
		PivotResultSet.showMeasuresFirst = false;
		rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("BOOL_AND", rs.getObject("Measure"));
		Assert.assertEquals("false", rs.getString("B:false"));
		Assert.assertEquals(false, rs.getObject("B:true"));

		rs.absolute(4);
		Assert.assertEquals("true", rs.getString("A"));
		Assert.assertEquals("BOOL_OR", rs.getString("Measure"));
		Assert.assertEquals("true", rs.getString("B:false"));
		Assert.assertEquals(true, rs.getObject("B:true"));
	}
	
}
