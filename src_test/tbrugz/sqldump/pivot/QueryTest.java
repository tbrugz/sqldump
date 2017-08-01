package tbrugz.sqldump.pivot;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.datadump.HTMLDataDump;
import tbrugz.sqldump.resultset.pivot.PivotResultSet;
import tbrugz.sqldump.resultset.pivot.PivotResultSet.Aggregator;
import tbrugz.sqldump.sqlrun.QueryDumper;
import tbrugz.sqldump.util.Utils;

public class QueryTest {

	static String DIR_OUT = "work/output/PivotTest/";
	
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
		
		File dir = new File(DIR_OUT);
		dir.mkdirs();
		Utils.deleteDirRegularContents(dir);
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
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"true"));

		rs.next();
		Assert.assertEquals("true", rs.getString("A"));
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals(true, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"true"));
	}

	@Test
	public void testQ3Pivot2Cols() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q3");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"+PivotResultSet.COLS_SEP+"C"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"+PivotResultSet.COLS_SEP+"C"+PivotResultSet.COLVAL_SEP+"true"));
	}
	
	@Test
	public void testQ4Pivot2Measures() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		String sql1 = sql + " /* pivot B nonpivot A */";
		ResultSet rs = conn.createStatement().executeQuery(sql1);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"true"));
		
		String sql2 = sql + " /* pivot B nonpivot A +measures-show-last */";
		rs = conn.createStatement().executeQuery(sql2);
		((PivotResultSet)rs).processMetadata();
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"+PivotResultSet.COLS_SEP+"BOOL_AND"));
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"true"+PivotResultSet.COLS_SEP+"BOOL_OR"));
	}

	@Test
	public void testQ5Aggregator() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q5");
		
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"true"));

		PivotResultSet.aggregator = Aggregator.FIRST;
		rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"true"));
	}

	@Test
	public void testQ6Integers() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q6");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals(2, rs.getObject("B"));
		Assert.assertEquals(2+1, rs.getObject("A"+PivotResultSet.COLVAL_SEP+"1"));
		
		rs.next();
		Assert.assertEquals(4, rs.getObject("B"));
		Assert.assertEquals(4+2, rs.getInt("A"+PivotResultSet.COLVAL_SEP+"2"));
	}

	@Test
	public void testQ7Date() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q7");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals(makeSqlDate(2013, 10, 8), rs.getObject("A"));
		Assert.assertEquals(1, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"one"));
		Assert.assertEquals(2, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"two"));
	}

	@Test
	public void testQ7DatePivotted() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q7p");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("one", rs.getObject("B"));
		Assert.assertEquals(1, rs.getObject("A"+PivotResultSet.COLVAL_SEP+"2013-10-08"));
		Assert.assertEquals(3, rs.getObject("A"+PivotResultSet.COLVAL_SEP+"2013-10-09"));

		rs.next();
		Assert.assertEquals("two", rs.getObject("B"));
		Assert.assertEquals(2, rs.getObject("A"+PivotResultSet.COLVAL_SEP+"2013-10-08"));
		Assert.assertEquals(4, rs.getObject("A"+PivotResultSet.COLVAL_SEP+"2013-10-09"));

		Assert.assertEquals(false, rs.next());
	}

	@Test
	public void q7DateSorted() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q7many");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		String[] colsNTP = {"A"};
		String[] colsTP = {"B"};
		rs = new PivotResultSet(rs, Arrays.asList(colsNTP), Arrays.asList(colsTP), true, PivotResultSet.FLAG_SORT_NONPIVOT_KEYS);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals(java.sql.Date.valueOf("2013-10-08"), rs.getObject("A"));
		rs.next();
		Assert.assertEquals(java.sql.Date.valueOf("2013-10-09"), rs.getObject("A"));
		rs.next();
		Assert.assertEquals(java.sql.Date.valueOf("2013-10-10"), rs.getObject("A"));
		rs.next();
		Assert.assertEquals(java.sql.Date.valueOf("2013-10-12"), rs.getObject("A"));
		rs.next();
		Assert.assertEquals(java.sql.Date.valueOf("2013-10-20"), rs.getObject("A"));
	}
	
	@Test
	public void testQ4MeasuresInColumnsFirst() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		String sql1 = sql + " /* pivot B nonpivot A */";
		ResultSet rs = conn.createStatement().executeQuery(sql1);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"true"));
		
		String sql2 = sql + " /* pivot B nonpivot A +measures-show-inrows */";
		rs = conn.createStatement().executeQuery(sql2);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("BOOL_AND", rs.getObject("Measure"));
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals(false, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"true"));

		rs.absolute(3);
		Assert.assertEquals("BOOL_OR", rs.getString("Measure"));
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals(true, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"true"));
	}

	@Test
	public void testQ4MeasuresInColumnsLast() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q4");
		String sql1 = sql + " /* pivot B nonpivot A */";
		ResultSet rs = conn.createStatement().executeQuery(sql1);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("false", rs.getString("BOOL_AND"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals("true", rs.getString("BOOL_OR"+PivotResultSet.COLS_SEP+"B"+PivotResultSet.COLVAL_SEP+"true"));
		
		String sql2 = sql + " /* pivot B nonpivot A +measures-show-inrows */";
		rs = conn.createStatement().executeQuery(sql2);
		QueryDumper.simplerRSDump(rs);

		rs.absolute(1);
		Assert.assertEquals("false", rs.getString("A"));
		Assert.assertEquals("BOOL_AND", rs.getObject("Measure"));
		Assert.assertEquals("false", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals(false, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"true"));

		rs.absolute(4);
		Assert.assertEquals("true", rs.getString("A"));
		Assert.assertEquals("BOOL_OR", rs.getString("Measure"));
		Assert.assertEquals("true", rs.getString("B"+PivotResultSet.COLVAL_SEP+"false"));
		Assert.assertEquals(true, rs.getObject("B"+PivotResultSet.COLVAL_SEP+"true"));
	}

	@Test
	public void testQ8() throws SQLException, ClassNotFoundException, IOException {
		String sql = prop.getProperty("q8");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
	}
	
	@Test
	public void q9() throws SQLException, IOException {
		String sql = prop.getProperty("q9");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		String[] colsNTP = {"A", "B"};
		String[] colsTP = {};
		rs = new PivotResultSet(rs, Arrays.asList(colsNTP), Arrays.asList(colsTP)); //, true, 4);
		QueryDumper.simplerRSDump(rs);
		// should return only 4 rows (not 5) & 3 cols
		Assert.assertEquals(3, rs.getMetaData().getColumnCount());
		Assert.assertEquals(true, rs.absolute(4));
		Assert.assertEquals(false, rs.next());
	}

	@Test
	public void q9b() throws SQLException, IOException {
		String sql = prop.getProperty("q9");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		String[] colsNTP = {};
		String[] colsTP = {"A", "B"};
		rs = new PivotResultSet(rs, Arrays.asList(colsNTP), Arrays.asList(colsTP));
		QueryDumper.simplerRSDump(rs);
		// should return only 1 row (not 5)
		Assert.assertEquals(true, rs.absolute(1));
		Assert.assertEquals(false, rs.next());
		//should return 16 cols (4 "distinct A values" x 4 "distinct B values")
		Assert.assertEquals(16, rs.getMetaData().getColumnCount());
	}

	@Test
	public void q9nonEmptyCols() throws SQLException, IOException {
		String sql = prop.getProperty("q9");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		String[] colsNTP = {};
		String[] colsTP = {"A", "B"};
		rs = new PivotResultSet(rs, Arrays.asList(colsNTP), Arrays.asList(colsTP), true, PivotResultSet.FLAG_NON_EMPTY_COLS);
		QueryDumper.simplerRSDump(rs);
		// should return only 1 row (not 5)
		Assert.assertEquals(true, rs.absolute(1));
		Assert.assertEquals(false, rs.next());
		//should return 4 cols (only cols with values)
		Assert.assertEquals(4, rs.getMetaData().getColumnCount());
	}
	
	@Test
	public void q10() throws SQLException, IOException {
		String sql = prop.getProperty("q10");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		String[] colsNTP = {"A", "B"};
		String[] colsTP = {};
		rs = new PivotResultSet(rs, Arrays.asList(colsNTP), Arrays.asList(colsTP));
		QueryDumper.simplerRSDump(rs);

		// should return only 4 rows (not 5) & 4 cols
		Assert.assertEquals(4, rs.getMetaData().getColumnCount());
		Assert.assertEquals(true, rs.absolute(4));
		Assert.assertEquals(false, rs.next());
	}
	
	@Test
	public void q3html() throws Exception {
		String sql = prop.getProperty("q3");
		ResultSet rs = conn.createStatement().executeQuery(sql);
		QueryDumper.simplerRSDump(rs);
		
		rs.absolute(0);
		Properties p = new Properties();
		p.setProperty(HTMLDataDump.PROP_PIVOT_ONROWS, "A");
		p.setProperty(HTMLDataDump.PROP_PIVOT_ONCOLS, "B,C");
		StringWriter sw = new StringWriter();
		HTMLDataDump dd = new HTMLDataDump();
		dd.procProperties(p);
		dd.initDump("", "", null, rs.getMetaData());
		dd.dumpHeader(sw);
		int i=0;
		while(rs.next()) {
			dd.dumpRow(rs, i++, sw);
		}
		dd.dumpFooter(i, sw);
		System.out.println(sw);
		
		File file = new File(DIR_OUT+"q3.html");
		FileWriter fw = new FileWriter(file);
		fw.write(sw.toString());
		fw.close();
		
		Document doc = DataDumpTest.parseXML(file);
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(2+2, DataDumpTest.countElementsOfType(n.getChildNodes(),"tr"));
	}
	
}
