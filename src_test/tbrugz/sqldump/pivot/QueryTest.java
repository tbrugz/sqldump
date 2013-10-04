package tbrugz.sqldump.pivot;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
		QueryDumper.simplerRSDump(rs);
	}

	@Test
	//TODO test for values
	public void testQ2Pivot() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q2");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
	}
	
}
