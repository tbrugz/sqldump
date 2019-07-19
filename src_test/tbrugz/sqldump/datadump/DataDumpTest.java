package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.datadump.DataDump.Outputter;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

//http://docs.oracle.com/javase/1.4.2/docs/guide/intl/encoding.doc.html
public class DataDumpTest {

	static Log log = LogFactory.getLog(DataDumpTest.class);

	static final String DIR_OUT = "work/output/DataDumpTest/";
	static final String dbpath = "mem:DataDumpTest;DB_CLOSE_DELAY=-1";
	static final String[] emptyArgs = {};
	
	@Test
	public void testEncoding() throws IOException {
		//DataDump dd = new DataDump();
		Map<String, Outputter> map = new HashMap<String, Outputter>();
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-utf8.txt", "", DataDumpUtils.CHARSET_UTF8, null, false, false);
		DataDump.isSetNewFilename(map, DIR_OUT+"t1-iso8859.txt", "", DataDumpUtils.CHARSET_ISO_8859_1, null, false, false); //ISO8859_1
		for(String s: map.keySet()) {
			map.get(s).w.write("Pôrto Alégre");
			map.get(s).w.close();
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
		String[] params = null;
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(params, p);
	}
	
	@Before
	public void beforeTest() {
		Utils.deleteDirRegularContents(new File(DIR_OUT));
	}
	
	void dump1() throws ClassNotFoundException, SQLException, NamingException, IOException {
		dumpWithParams(null);
	}

	void dumpWithParamsAndSyntax(String[] xtraparams, String syntax) throws ClassNotFoundException, SQLException, NamingException, IOException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				"-Dsqldump.datadump.dumpsyntaxes="+syntax,
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename]."+syntax,
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		dump1(vmparamsDump, xtraparams);
	}
	
	void dumpWithParams(String[] xtraparams) throws ClassNotFoundException, SQLException, NamingException, IOException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv, xml, html, json",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename].[syntaxfileext]",
				//"-Dsqldump.datadump.writebom=false",
				//"-Dsqldump.datadump.xml.escape=true",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		dump1(vmparamsDump, xtraparams);
	}
	
	void dump1(String[] vmparamsDump, String[] xtraparams) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDump sqld = new SQLDump();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		if(xtraparams!=null) {
			TestUtil.setProperties(p, xtraparams);
		}
		sqld.doMain(emptyArgs, p);
	}
	
	@Test
	public void testCSV() throws Exception {
		dump1();
		String csvDept = IOUtil.readFromFilename(DIR_OUT+"/data_DEPT.csv");
		String expected = "ID,NAME,PARENT_ID\r\n0,CEO,0\r\n1,HR,0\r\n2,Engineering,0\r\n";
		Assert.assertEquals(expected, csvDept);
	}
	
	@Test
	public void testCSVDate() throws Exception {
		dump1();
		String csvEtc = IOUtil.readFromFilename(DIR_OUT+"/data_ETC.csv");
		String expected = "ID,DT_X,DESCRIPTION\r\n1,2013-01-01,lala &\r\n";
		Assert.assertEquals(expected, csvEtc.substring(0, expected.length()));
	}
	
	@Test
	public void testSQL() throws Exception {
		dump1();
		String sqlEmp = IOUtil.readFromFilename(DIR_OUT+"/data_EMP.sql");
		String expected = "insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);";
		Assert.assertEquals(expected, sqlEmp.substring(0, expected.length()));
	}
	
	@Test
	public void testSQLWithQuote() throws ClassNotFoundException, SQLException, NamingException, IOException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.insertinto.quotesql=true",
				});
		String sqlEmp = IOUtil.readFromFilename(DIR_OUT+"/data_EMP.sql");
		String expected = "insert into \"EMP\" (\"ID\", \"NAME\", \"SUPERVISOR_ID\", \"DEPARTMENT_ID\", \"SALARY\") values (1, 'john', 1, 1, 2000);";
		Assert.assertEquals(expected, sqlEmp.substring(0, expected.length()));
	}

	@Test
	public void testSQLDate() throws Exception {
		dump1();
		String sqlEtc = IOUtil.readFromFilename(DIR_OUT+"/data_ETC.sql");
		String expected = "insert into ETC (ID, DT_X, DESCRIPTION) values (1, DATE '2013-01-01', 'lala &');";
		Assert.assertEquals(expected, sqlEtc.substring(0, expected.length()));
	}

	@Test
	public void testSQLDateNonDefault() throws Exception {
		String[] vmparamsDump = {
				"-Dsqldump.datadump.insertinto.dateformat='TIMESTAMP '''yyyy-MM-dd''"
		};
		dumpWithParams(vmparamsDump);
		String sqlEtc = IOUtil.readFromFilename(DIR_OUT+"/data_ETC.sql");
		String expected = "insert into ETC (ID, DT_X, DESCRIPTION) values (1, TIMESTAMP '2013-01-01', 'lala &');";
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
		Assert.assertEquals(6, countElementsOfType(n.getChildNodes(),"row"));
	}

	@Test(expected=SAXParseException.class)
	public void testXMLNoEscape() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.xml.escape=false",
				});
		File f = new File(DIR_OUT+"/data_ETC.xml");
		parseXML(f);
	}

	@Test
	public void testHTMLEscapeCol() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.xml.escape=false",
				"-Dsqldump.datadump.xml.escapecols4table@ETC=DESCRIPTION",
				});
		File f = new File(DIR_OUT+"/data_ETC.html");
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(7, countElementsOfType(n.getChildNodes(),"tr"));
	}
	
	@Test(expected=SAXParseException.class)
	public void testHTMLNoEscapeCol() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.xml.noescapecols4table@ETC=DESCRIPTION",
				});
		File f = new File(DIR_OUT+"/data_ETC.html");
		parseXML(f);
	}

	@Test
	public void testHTMLbreaksNoXtraColHeader() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				//"-Dsqldump.grabclass=",
				//"-Dsqldump.grabclass=EmptyModelGrabber",
				"-Dsqldump.datadump.dumpsyntaxes=html",
				"-Dsqldump.datadump.html.break-columns=SUPERVISOR_ID",
				});

		// ETC: no breaks
		{
			File f = new File(DIR_OUT+"/data_ETC.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(7, countElementsOfType(n.getChildNodes(),"tr"));
		}

		// EMP: has breaks
		{
			File f = new File(DIR_OUT+"/data_EMP.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(9, countElementsOfType(n.getChildNodes(),"tr"));
		}
	}

	@Test
	public void testHTMLbreaksAddColHeaderBefore() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.dumpsyntaxes=html",
				"-Dsqldump.datadump.html.break-columns=SUPERVISOR_ID",
				"-D"+HTMLDataDump.PROP_HTML_BREAKS_ADD_COL_HEADER_BEFORE+"=true",
				//"-D"+HTMLDataDump.PROP_HTML_BREAKS_ADD_COL_HEADER_AFTER+"=true"
				});

		// ETC: no breaks
		{
			File f = new File(DIR_OUT+"/data_ETC.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(7, countElementsOfType(n.getChildNodes(),"tr"));
		}
		
		// EMP: has breaks
		{
			File f = new File(DIR_OUT+"/data_EMP.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(11, countElementsOfType(n.getChildNodes(),"tr"));
		}
	}

	@Test
	public void testHTMLbreaksAddColHeaderAfter() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.dumpsyntaxes=html",
				"-Dsqldump.datadump.html.break-columns=SUPERVISOR_ID",
				//"-D"+HTMLDataDump.PROP_HTML_BREAKS_ADD_COL_HEADER_BEFORE+"=true",
				"-D"+HTMLDataDump.PROP_HTML_BREAKS_ADD_COL_HEADER_AFTER+"=true"
				});

		// ETC: no breaks
		{
			File f = new File(DIR_OUT+"/data_ETC.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(7, countElementsOfType(n.getChildNodes(),"tr"));
		}
		
		// EMP: has breaks
		{
			File f = new File(DIR_OUT+"/data_EMP.html");
			Document doc = parseXML(f);
			
			Node n = doc.getChildNodes().item(0);
			Assert.assertEquals(11, countElementsOfType(n.getChildNodes(),"tr"));
		}
	}
	
	@Test
	public void testXmlQueryRaw() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
				"-Dsqldump.grabclass=EmptyModelGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql=select name, '<value>'||salary||'</value>' as salary_value, '<value>'||salary||'</value>' as salary_raw from emp",
				});
		File f = new File(DIR_OUT+"/data_q1.xml");
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(5, countElementsOfType(n.getChildNodes(),"row"));
		NodeList row1 = n.getChildNodes().item(1).getChildNodes();
		/*log.info("Node.TEXT_NODE: "+Node.TEXT_NODE+" / Node.ELEMENT_NODE: "+Node.ELEMENT_NODE);
		log.info("text1: "+row1.item(1).getNodeName()+"/"+row1.item(1).getTextContent()+"/"+row1.item(1).getChildNodes().getLength()+" // "+row1.item(1).getChildNodes().item(0).getNodeType());
		log.info("text2: "+row1.item(2).getNodeName()+"/"+row1.item(2).getTextContent()+"/"+row1.item(2).getChildNodes().getLength()+" // "+row1.item(2).getChildNodes().item(0).getNodeType());
		log.info("text3: "+row1.item(3).getNodeName()+"/"+row1.item(3).getTextContent()+"/"+row1.item(3).getChildNodes().getLength()+" // "+row1.item(3).getChildNodes().item(0).getNodeType());*/
		Assert.assertEquals("SALARY_VALUE", row1.item(2).getNodeName());
		Assert.assertEquals("<value>2000</value>", row1.item(2).getTextContent());
		Assert.assertEquals(Node.TEXT_NODE, row1.item(2).getChildNodes().item(0).getNodeType());
		Assert.assertEquals("SALARY_RAW", row1.item(3).getNodeName());
		Assert.assertEquals(Node.ELEMENT_NODE, row1.item(3).getChildNodes().item(0).getNodeType());
	}
	
	@Test
	public void testHTML() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException, NamingException {
		dump1();
		File f = new File(DIR_OUT+"/data_ETC.html");
		//String xmlStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(xmlStr);
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(7, countElementsOfType(n.getChildNodes(),"tr"));
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

	byte UTF8_BOM1 = (byte) 0xef;
	byte UTF8_BOM2 = (byte) 0xbb;
	byte UTF8_BOM3 = (byte) 0xbf;
	//EF BB BF
	
	void testJsonForUTF8BOM() throws IOException {
		File f = new File(DIR_OUT+"/data_ETC.json");
		ByteBuffer bb = IOUtil.readFileBytes(f.getAbsolutePath());
		
		log.info("utf-8 BOM: "+UTF8_BOM1+","+UTF8_BOM2+","+UTF8_BOM3);
		
		Assert.assertEquals(UTF8_BOM1, bb.get(0));
		Assert.assertEquals(UTF8_BOM2, bb.get(1));
		Assert.assertEquals(UTF8_BOM3, bb.get(2));
		
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//Assert.assertEquals(UTF8_BOM1, jsonStr.charAt(0));
		//Assert.assertEquals(UTF8_BOM2, jsonStr.charAt(1));
		//Assert.assertEquals(UTF8_BOM3, jsonStr.charAt(2));
		//Assert.assertTrue("1st char should be '"+UTF8_BOM1+"' but is '"+jsonStr.charAt(0)+"'", jsonStr.charAt(0)==UTF8_BOM1);
		//Assert.assertTrue("2nd char should be '"+UTF8_BOM2+"'", jsonStr.charAt(1)==UTF8_BOM2);
		//Assert.assertTrue("3rd char should be '"+UTF8_BOM3+"'", jsonStr.charAt(2)==UTF8_BOM3);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be null", obj==null);
	}
	
	@Test
	public void testJSONWithBOM() throws IOException, ParserConfigurationException, SAXException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
			"-Dsqldump.datadump.writebom=true",
		});
		testJsonForUTF8BOM();
	}

	@Test
	public void testJSONWithSyntaxBOM() throws IOException, ParserConfigurationException, SAXException, ClassNotFoundException, SQLException, NamingException {
		dumpWithParams(new String[]{
			"-Dsqldump.datadump.json.writebom=true",
		});
		testJsonForUTF8BOM();
	}
	
	@Test
	public void testJsonNullDataElement() throws Exception {
		File f = dumpSelect("select 1 as a, 3 as c", "json",
				new String[] {"-Dsqldump.datadump.json.null-data-element=true"});
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		Object obj = JSONValue.parse(jsonStr);
		JSONArray jarr = (JSONArray) obj;
		JSONObject jobj = (JSONObject) jarr.get(0);
		Assert.assertEquals(1L, jobj.get("A"));
		Assert.assertEquals(3L, jobj.get("C"));
	}
	
	@Test
	public void testJsonNullDataElementUniqueRow() throws Exception {
		File f = dumpSelect("select 1 as a, 3 as c", "json",
				new String[] {"-Dsqldump.datadump.json.null-data-element=true",
						"-Dsqldump.datadump.json.force-unique-row=true",
						"-Dsqldump.datadump.json.no-array-on-unique-row=true"}
		);
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		Object obj = JSONValue.parse(jsonStr);
		JSONObject jobj = (JSONObject) obj;
		Assert.assertEquals(1L, jobj.get("A"));
		Assert.assertEquals(3L, jobj.get("C"));
	}

	@Test
	public void testJsonUniqueRow() throws Exception {
		File f = dumpSelect("select 1 as a, 3 as c", "json"
				,new String[] {"-Dsqldump.datadump.json.force-unique-row=true"}
		);
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		Object obj = JSONValue.parse(jsonStr);
		JSONObject jobj = (JSONObject) obj;
		JSONArray jarr = (JSONArray) jobj.get("q1");
		jobj = (JSONObject) jarr.get(0);
		Assert.assertEquals(1L, jobj.get("A"));
		Assert.assertEquals(3L, jobj.get("C"));
	}

	@Test
	public void testJsonUniqueRowNoArray() throws Exception {
		File f = dumpSelect("select 1 as a, 3 as c", "json"
				,new String[] {"-Dsqldump.datadump.json.force-unique-row=true",
						"-Dsqldump.datadump.json.no-array-on-unique-row=true"}
		);
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		Object obj = JSONValue.parse(jsonStr);
		JSONObject jobj = (JSONObject) obj;
		jobj = (JSONObject) jobj.get("q1");
		Assert.assertEquals(1L, jobj.get("A"));
		Assert.assertEquals(3L, jobj.get("C"));
	}

	@Test
	public void testJsonUniqueRowNoArray_With2Rows() throws Exception {
		File f = dumpSelect("select 1 as a, 3 as c union all select 2, 2", "json"
				,new String[] {"-Dsqldump.datadump.json.force-unique-row=true",
						"-Dsqldump.datadump.json.no-array-on-unique-row=true"}
		);
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		Object obj = JSONValue.parse(jsonStr);
		JSONObject jobj = (JSONObject) obj;
		jobj = (JSONObject) jobj.get("q1");
		System.out.println("size: "+jobj.size());
		Assert.assertEquals(2, jobj.size());
	}

	@Test
	public void testJsonUniqueRowNoArrayWithInnerArray() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c", "json"
				,new String[] {
						//"-Dsqldump.datadump.json.null-data-element=true",
						"-Dsqldump.datadump.json.no-array-on-unique-row=true",
						"-Dsqldump.datadump.json.force-unique-row=true"
						}
		);
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		System.out.println(jsonStr);
		
		Object obj = JSONValue.parse(jsonStr);
		JSONObject jobj = (JSONObject) obj;
		//System.out.println("obj:: "+obj+"\njobj:: "+jobj);
		jobj = (JSONObject) jobj.get("q1");
		Assert.assertEquals(3, jobj.size());
		JSONArray jarr  = (JSONArray) jobj.get("B");
		Assert.assertEquals(4, jarr.size());
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
				//"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(emptyArgs, p);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n1,john,1,1,2000\r\n5,wilson,1,1,1000\r\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n2,mary,2,2,2000\r\n3,jane,2,2,1000\r\n4,lucas,2,2,1200\r\n";
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
				//"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(emptyArgs, p);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.csv");
		String expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n1,john,1,1,2000\r\n5,wilson,1,1,1000\r\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n2,mary,2,2,2000\r\n3,jane,2,2,1000\r\n4,lucas,2,2,1200\r\n";
		Assert.assertEquals(expected, csvEmpS2);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2.csv");
		expected = "ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n1,john,1,1,2000\r\n2,mary,2,2,2000\r\n3,jane,2,2,1000\r\n4,lucas,2,2,1200\r\n5,wilson,1,1,1000\r\n";
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
				//"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(emptyArgs, p);
		
		String csvEmpAll = IOUtil.readFromFilename(DIR_OUT+"/data_q1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n"+
				"1,1,john,1,1,2000\r\n"+
				"2,2,mary,2,2,2000\r\n"+
				"3,3,jane,2,2,1000\r\n"+
				"4,4,lucas,2,2,1200\r\n"+
				"5,5,wilson,1,1,1000\r\n";
				//"5\r\n";
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
				//"-Dsqldump.datadump.writebom=false",
				"-Dsqldump.datadump.logeachxrows=1",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		new SQLDump().doMain(emptyArgs, p);
		
		String csvEmpS1 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_1.rn.csv");
		String expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n"+
				"1,1,john,1,1,2000\r\n"+
				"2,5,wilson,1,1,1000\r\n";//"2\r\n";
		Assert.assertEquals(expected, csvEmpS1);
		
		String csvEmpS2 = IOUtil.readFromFilename(DIR_OUT+"/data_q1p2_2.rn.csv");
		expected = "LineNumber,ID,NAME,SUPERVISOR_ID,DEPARTMENT_ID,SALARY\r\n"+
				"1,2,mary,2,2,2000\r\n"+
				"2,3,jane,2,2,1000\r\n"+
				"3,4,lucas,2,2,1200\r\n";//+"3\r\n";
		Assert.assertEquals(expected, csvEmpS2);
	}
	
	@Test
	public void testRowsetSer() throws Exception {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				"-Dsqldump.datadump.dumpsyntaxes=rowset-ser",
				"-Dsqldump.datadump.outfilepattern="+DIR_OUT+"/data_[tablename].[syntaxfileext]",
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		dump1(vmparamsDump, null);

		DumpSyntax ds = new CacheRowSetSyntax();
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(DIR_OUT+"/data_EMP."+ds.getDefaultFileExtension()));
		Object o = ois.readObject();
		ois.close();
		Assert.assertTrue("Shoud be an instance of ResultSet", o instanceof ResultSet);
		//id,name,supervisor_id,department_id,salary
		//1,john,1,1,2000
		//2,mary,2,2,2000
		
		ResultSet rs = (ResultSet) o;
		Assert.assertTrue(rs.first());
		Assert.assertEquals("2000", rs.getString(5));
		Assert.assertEquals("1", rs.getString(3));
		Assert.assertTrue(rs.next());
		// 2nd row
		Assert.assertEquals(2, rs.getInt(1));
		Assert.assertEquals("mary", rs.getString(2));
		Assert.assertTrue(rs.next());
		// 3rd row
		Assert.assertTrue(rs.next());
		// 4th row
		Assert.assertTrue(rs.next());
		// 5th row...
		Assert.assertFalse(rs.next());

		rs.close();
	}

	@Test
	public void testXls() throws ClassNotFoundException, SQLException, NamingException, IOException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.dumpsyntaxes=xls, xlsx",
				});
		//XXX: assert...
	}
	
	@Test
	public void testHtmlWithArray() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3) as b", "html");
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(2, countElementsOfType(n.getChildNodes(),"tr")); // 2 rows
		
		Element e = (Element) n;
		NodeList nl = e.getElementsByTagName("tr");
		Assert.assertEquals(6, nl.getLength()); // 6 total rows
		
		nl = e.getElementsByTagName("td");
		Assert.assertEquals(5, nl.getLength()); // 5 total 'td's

		nl = e.getElementsByTagName("th");
		Assert.assertEquals(3, nl.getLength()); // 5 total 'th's
	}

	@Test
	public void testXmlWithArray() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3) as b", "xml");
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(1, countElementsOfType(n.getChildNodes(),"row")); // 1 row
		
		Element e = (Element) n;
		NodeList nl = e.getElementsByTagName("row");
		Assert.assertEquals(4, nl.getLength()); // 4 total rows
		
		nl = e.getElementsByTagName("A");
		Assert.assertEquals(1, nl.getLength()); // 1 "A" element

		nl = e.getElementsByTagName("B");
		Assert.assertEquals(4, nl.getLength()); // 4 "B" elements
	}

	@Test
	public void testXmlWithArray2() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "xml");
		Document doc = parseXML(f);
		
		Node n = doc.getChildNodes().item(0);
		Assert.assertEquals(2, countElementsOfType(n.getChildNodes(),"row")); // 1 row
		
		Element e = (Element) n;
		NodeList nl = e.getElementsByTagName("row");
		Assert.assertEquals(10, nl.getLength()); // 10 total rows
		
		nl = e.getElementsByTagName("A");
		Assert.assertEquals(2, nl.getLength()); // 2 "A" element

		nl = e.getElementsByTagName("B");
		Assert.assertEquals(10, nl.getLength()); // 10 "B" elements
	}

	@Test
	public void testJsonWithArray2() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "json");
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(jsonStr);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);
		
		JSONObject jobj = (JSONObject) obj;
		obj = jobj.get("q1");
		Assert.assertTrue("Should be a JSONArray", obj instanceof JSONArray);

		JSONArray jarr = (JSONArray) obj;
		Assert.assertEquals(2, jarr.size());
		
		jobj = (JSONObject) jarr.get(1); // 2nd row
		jarr = (JSONArray) jobj.get("B");
		Assert.assertEquals(4, jarr.size());
		
		//System.out.println("jarr: "+jarr+" / "+jarr.get(3));
		Object i3 = jarr.get(3);
		//System.out.println(jobj);
		Assert.assertEquals("8", i3);
	}

	@Test
	public void testJsonWithArrayMetadata() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "json", new String[] {
				"-D"+JSONDataDump.PROP_ADD_METADATA+"=true",
				"-D"+JSONDataDump.PROP_INNER_TABLE_ADD_DATA_ELEMENT+"=true",
				"-D"+JSONDataDump.PROP_INNER_TABLE_ADD_METADATA+"=true",
				});
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(jsonStr);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);

		JSONObject jobj = (JSONObject) obj;
		Assert.assertTrue("$metadata should be a JSONObject", jobj.get("$metadata") instanceof JSONObject);
		Assert.assertTrue("q1 should be a JSONArray", jobj.get("q1") instanceof JSONArray);
		JSONArray jarr = (JSONArray) jobj.get("q1");
		Assert.assertEquals(2, jarr.size());
		
		JSONObject row1 = (JSONObject) jarr.get(0);
		//System.out.println("row1:\n"+row1);
		Assert.assertEquals(1L, row1.get("A"));
		
		Assert.assertTrue("B should be a JSONObject", row1.get("B") instanceof JSONObject);
		JSONObject innerB = (JSONObject) row1.get("B");
		Assert.assertTrue("innerB's $metadata should be a JSONObject", innerB.get("$metadata") instanceof JSONObject);
	}
	
	@Test
	public void testJsonWithArrayMetadataParentOnly() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "json", new String[] {
				"-D"+JSONDataDump.PROP_ADD_METADATA+"=true",
				});
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(jsonStr);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);

		JSONObject jobj = (JSONObject) obj;
		Assert.assertTrue("$metadata should be a JSONObject", jobj.get("$metadata") instanceof JSONObject);
		Assert.assertTrue("q1 should be a JSONArray", jobj.get("q1") instanceof JSONArray);
		JSONArray jarr = (JSONArray) jobj.get("q1");
		Assert.assertEquals(2, jarr.size());
		
		JSONObject row1 = (JSONObject) jarr.get(0);
		//System.out.println("row1:\n"+row1);
		Assert.assertEquals(1L, row1.get("A"));
		
		Assert.assertTrue("B should be a JSONArray", row1.get("B") instanceof JSONArray);
		JSONArray innerB = (JSONArray) row1.get("B");
		Assert.assertEquals(4, innerB.size());
	}
	
	
	@Test
	public void testJsonWithArrayWithData() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "json", new String[] {
				"-D"+JSONDataDump.PROP_ADD_METADATA+"=true",
				"-D"+JSONDataDump.PROP_TABLE_AS_DATA_ELEMENT+"=false",
				"-D"+JSONDataDump.PROP_INNER_TABLE_ADD_DATA_ELEMENT+"=true",
				});
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(jsonStr);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);

		JSONObject jobj = (JSONObject) obj;
		Assert.assertTrue("$metadata should be a JSONObject", jobj.get("$metadata") instanceof JSONObject);
		Assert.assertTrue("q1's 'data' should be a JSONArray", jobj.get("data") instanceof JSONArray);
		JSONArray jarr = (JSONArray) jobj.get("data");
		Assert.assertEquals(2, jarr.size());
		
		JSONObject row1 = (JSONObject) jarr.get(0);
		//System.out.println("row1:\n"+row1);
		Assert.assertEquals(1L, row1.get("A"));
		
		Assert.assertTrue("B should be a JSONObject", row1.get("B") instanceof JSONObject);
		Object innerBdata = ((JSONObject) row1.get("B")).get("data");
		Assert.assertTrue("B's 'data' should be a JSONArray", innerBdata instanceof JSONArray);
		Assert.assertEquals(4, ((JSONArray)innerBdata).size());
	}

	@Test
	public void testJsonWithInnerArrayAsArray() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "json", new String[] {
				"-D"+JSONDataDump.PROP_TABLE_AS_DATA_ELEMENT+"=false",
				"-D"+JSONDataDump.PROP_INNER_ARRAY_DUMP_AS_ARRAY+"=true",
				});
		String jsonStr = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(jsonStr);

		Object obj = JSONValue.parse(jsonStr);
		Assert.assertTrue("Should be a JSONObject", obj instanceof JSONObject);

		JSONObject jobj = (JSONObject) obj;
		Assert.assertTrue("q1's 'data' should be a JSONArray", jobj.get("data") instanceof JSONArray);
		JSONArray jarr = (JSONArray) jobj.get("data");
		Assert.assertEquals(2, jarr.size());
		
		JSONObject row1 = (JSONObject) jarr.get(0);
		Assert.assertEquals(1L, row1.get("A"));
		
		Assert.assertTrue("B should be a JSONArray", row1.get("B") instanceof JSONArray);
		Assert.assertEquals(4, ((JSONArray) row1.get("B")).size());
	}
	
	@Test
	public void testInsertIntoWithoutArray2() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "insertinto");
		String str = IOUtil.readFromFilename(f.getAbsolutePath());
		String LF = "\n";
		Assert.assertEquals("insert into q1 (A, B, C) values (1, null, 3);" + LF + 
				"insert into q1 (A, B, C) values (4, null, 9);" + LF 
				, str);
	}
	
	@Test
	public void testInsertIntoWithArray2() throws Exception {
		File f = dumpSelect("select 1 as a, (1,2,3,4) as b, 3 as c union all select 4, (5,6,7,8), 9", "insertinto",
				new String[] {"-Dsqldump.datadump.insertinto.dumpcursors=true"});
		String str = IOUtil.readFromFilename(f.getAbsolutePath());
		String LF = "\n";
		System.out.println(str);
		Assert.assertEquals("insert into q1 (A, B, C) values (1, null, 3);" + LF + 
				"insert into B (B) values (1);" + LF +
				"insert into B (B) values (2);" + LF +
				"insert into B (B) values (3);" + LF +
				"insert into B (B) values (4);" + LF +
				"insert into q1 (A, B, C) values (4, null, 9);" + LF + 
				"insert into B (B) values (5);" + LF +
				"insert into B (B) values (6);" + LF +
				"insert into B (B) values (7);" + LF +
				"insert into B (B) values (8);" + LF
				, str);
	}

	@Test
	public void testCsvRn() throws Exception {
		String LF = "\r\n";
		dumpWithParamsAndSyntax(null, "rncsv");
		String csvDept = IOUtil.readFromFilename(DIR_OUT+"/data_DEPT.rncsv");
		//System.out.println(csvDept);
		Assert.assertEquals(
				"LineNumber,ID,NAME,PARENT_ID" + LF +
				"1,0,CEO,0" + LF +
				"2,1,HR,0" + LF + 
				"3,2,Engineering,0" + LF,
				csvDept);
	}
	
	//----------------------------------
	
	public static Document parseXML(File f) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		return dBuilder.parse(f);
	}

	/*static int countElements(NodeList nl) {
		int count = 0;
		for(int i=0;i<nl.getLength();i++) {
			Node n = nl.item(i);
			if(n.getNodeType()==Node.ELEMENT_NODE) {
				count++;
			}
		}
		return count;
	}*/

	public static int countElementsOfType(NodeList nl, String tag) {
		int count = 0;
		for(int i=0;i<nl.getLength();i++) {
			Node n = nl.item(i);
			if(n.getNodeType()==Node.ELEMENT_NODE && tag.equals(n.getNodeName())) {
				count++;
			}
		}
		return count;
	}
	
	public File dumpSelect(String sql, String syntax) throws Exception {
		return dumpSelect(sql, syntax, null);
	}
	
	public File dumpSelect(String sql, String syntax, String[] xtrapar) throws Exception {
		String[] pars = new String[]{
				"-Dsqldump.grabclass=EmptyModelGrabber",
				"-Dsqldump.processingclasses=SQLQueries",
				"-Dsqldump.queries=q1",
				"-Dsqldump.query.q1.sql="+sql,
				};
		if(xtrapar!=null) {
			List<String> l = new ArrayList<String>();
			l.addAll(Arrays.asList(pars));
			l.addAll(Arrays.asList(xtrapar));
			pars = l.toArray(new String[]{});
		}
		dumpWithParamsAndSyntax(pars, syntax);
		return new File(DIR_OUT+"/data_q1."+syntax);
	}

	@Test
	public void testRsProjection() throws Exception {
		File f = dumpSelect(
				"select 1 as a, 2 as b, 3 as c union all select 4, 5, 6", "csv",
				new String[] {"-Dsqldump.query.q1.dump-cols=C, B"}
				);
		String csv = IOUtil.readFromFilename(f.getAbsolutePath());
		//System.out.println(csv);
		
		String LF = "\r\n";
		Assert.assertEquals(
				"C,B" + LF +
				"3,2" + LF +
				"6,5" + LF,
				csv);
		
	}
	
}
