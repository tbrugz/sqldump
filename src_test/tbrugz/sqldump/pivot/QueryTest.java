package tbrugz.sqldump.pivot;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
		Class.forName(pivotDriverClass); //"tbrugz.sqldump.pivot.SQLPivotDriver");
		
		String url = "jdbc:sqlpivot:h2:mem:abc";
		conn = DriverManager.getConnection(url, null);
	}

	@After
	public void after() throws SQLException, ClassNotFoundException {
		conn.close();
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
		Assert.assertEquals("FALSE", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:FALSE"));
		Assert.assertEquals("true", rs.getString("B:TRUE"));
	}

	@Test
	public void testQ3Pivot2Cols() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q3");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("FALSE", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:FALSE|C:FALSE"));
		Assert.assertEquals("true", rs.getString("B:FALSE|C:TRUE"));
	}
	
	@Test
	public void testQ4Pivot2Measures() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("FALSE", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND|B:FALSE"));
		Assert.assertEquals("true", rs.getString("BOOL_OR|B:TRUE"));
		
		((PivotResultSet)rs).showMeasuresFirst = false;
		((PivotResultSet)rs).processMetadata();
		rs.absolute(0);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("FALSE", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B:FALSE|BOOL_AND"));
		Assert.assertEquals("true", rs.getString("B:TRUE|BOOL_OR"));
	}

	@Test
	public void testQ5Aggregator() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q5");
		
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("true", rs.getString("B:TRUE"));

		PivotResultSet.aggregator = Aggregator.FIRST;
		rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("B:TRUE"));
	}

	@Test
	public void testQ6Integers() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q6");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("2", rs.getObject("B"));
		Assert.assertEquals(2+1, rs.getObject("A:1"));
		
		rs.next();
		Assert.assertEquals("4", rs.getObject("B"));
		Assert.assertEquals(4+2, rs.getInt("A:2"));
	}
	
}
