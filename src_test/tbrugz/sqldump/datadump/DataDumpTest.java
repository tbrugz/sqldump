package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.NamingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

//http://docs.oracle.com/javase/1.4.2/docs/guide/intl/encoding.doc.html
public class DataDumpTest {

	static Log log = LogFactory.getLog(DataDumpTest.class);

	static String DIR_OUT = "work/output/DataDumpTest/";
	static String dbpath = "mem:DataDumpTest;DB_CLOSE_DELAY=-1";
	static String[] params = {};
	
	@Test
	public void testEncoding() throws IOException {
		//DataDump dd = new DataDump();
		Map<String, Writer> map = new HashMap<String, Writer>();
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-utf8.txt", "", DataDumpUtils.CHARSET_UTF8, null, false);
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-iso8859.txt", "", DataDumpUtils.CHARSET_ISO_8859_1, null, false); //ISO8859_1
		for(String s: map.keySet()) {
			map.get(s).write("Pôrto Alégre");
			map.get(s).close();
		}
	}
	
	@BeforeClass
	public static void setupDB() throws Exception {
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src_test/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src_test/tbrugz/sqldump/sqlrun/emp.csv",
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.exec.10.import=csv",
				"-Dsqlrun.exec.10.inserttable=etc",
				"-Dsqlrun.exec.10.importfile=src_test/tbrugz/sqldump/sqlrun/etc.csv",
				"-Dsqlrun.exec.10.skipnlines=1",
				//"-Dsqlrun.exec.05.emptystringasnull=true",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		String[] params = {};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(params, p, null);
	}
	
	@Before
	public void beforeTest() {
		Utils.deleteDirRegularContents(new File(DIR_OUT));
	}
	
	public void dump1() throws ClassNotFoundException, SQLException, NamingException, IOException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv, xml, html, json",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		SQLDump sqld = new SQLDump();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(params, p, null);
	}
	
	@Test
	public void testCSV() throws Exception {
		dump1();
		String csvDept = IOUtil.readFromFilename(DIR_OUT+"/data_DEPT.csv");
		String expected = "ID,NAME,PARENT_ID\n0,CEO,0\n1,HR,0\n2,Engineering,0\n";
		Assert.assertEquals(expected, csvDept);
	}
	
	@Test
	public void testSQL() throws Exception {
		dump1();
		String sqlEmp = IOUtil.readFromFilename(DIR_OUT+"/data_EMP.sql");
		String expected = "insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);";
		Assert.assertEquals(expected, sqlEmp.substring(0, expected.length()));
	}

	@Test
	public void testSQLDate() throws Exception {
		dump1();
		String sqlEtc = IOUtil.readFromFilename(DIR_OUT+"/data_ETC.sql");
		String expected = "insert into ETC (ID, DT_X, DESCRIPTION) values (1, '2013-01-01', 'lala &');";
		Assert.assertEquals(expected, sqlEtc.substring(0, expected.length()));
	}

	@Test
	public void testXML() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dump1();
		File f = new File(DIR_OUT+"/data_ETC.xml");
		//String xmlStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(xmlStr);
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(6, countElements(n.getChildNodes()));
	}

	@Test
	public void testHTML() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dump1();
		File f = new File(DIR_OUT+"/data_ETC.html");
		//String xmlStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(xmlStr);
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(7, countElements(n.getChildNodes()));
	}
	
	@Test
	public void testJSON() throws IOException, ParserConfigurationException, SAXException, ClassNotFoundException, SQLException, NamingException {
		dump1();
		File f = new File(DIR_OUT+"/data_ETC.json");
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);
		
		JSONObject jobj = (JSONObject) obj;
		obj = jobj.get("ETC");
		Assert.assertTrue("Should be a JSONArray", obj instanceof JSONArray);

		JSONArray jarr = (JSONArray) obj;
		Assert.assertEquals(6, jarr.size());
	}
	
	@Test
	public void dumpPartitioned() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=_[col:SUPERVISOR_ID]",
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n";
		Assert.assertEquals(expected, csvEmpS2);
	}

	@Test
	public void dumpPartitioned2Patterns() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=p2|p2_[col:SUPERVISOR_ID]", //this line changes it!
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n";
		Assert.assertEquals(expected, csvEmpS2);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n1,john,1,1,2000\n2,mary,2,2,2000\n3,jane,2,2,1000\n4,lucas,2,2,1200\n5,wilson,1,1,1000\n";
		Assert.assertEquals(expected, csvEmpAll);
	}
	
	@Test
	public void dumpWithRowNumber() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.datadump.dumpsyntaxes=rncsv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,1,john,1,1,2000\n"+"1,2,mary,2,2,2000\n"+"2,3,jane,2,2,1000\n"+"3,4,lucas,2,2,1200\n"+"4,5,wilson,1,1,1000\n"+"5\n";
		Assert.assertEquals(expected, csvEmpAll);
	}
	
	@Test
	public void dumpPartitionedWithRowNumber() throws IOException, ClassNotFoundException, SQLException, NamingException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select * from emp",
				"-Dsqldump.query.q1.partitionby=p2_[col:SUPERVISOR_ID]",
				"-Dsqldump.datadump.dumpsyntaxes=rncsv",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename][partitionby].[syntaxfileext]",
				"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(params, p, null);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,1,john,1,1,2000\n"+"1,5,wilson,1,1,1000\n"+"2\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.rn.csv");
		expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\n"+"0,2,mary,2,2,2000\n"+"1,3,jane,2,2,1000\n"+"2,4,lucas,2,2,1200\n"+"3\n";
		Assert.assertEquals(expected, csvEmpS2);
	}
	
	static Document parseXML(File f) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		return dBuilder.parse(f);
	}
	
	static int countElements(NodeList nl) {
		int count = 0;
		for(int i=0;i<nl.getLength();i++) {
			Node n = nl.item(i);
			if(n.getNodeType()==Node.ELEMENT_NODE) {
				count++;
			}
		}
		return count;
	}
	
}
