package tbrugz.sqldump.datadump;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class ConcurrentDumpTest {
	
	Properties prop;
	Connection conn;
	
	@BeforeClass
	public static void beforeClass() {
		SQLUtils.failOnError = true;
	}
	
	@Before
	public void before() throws Exception {
		prop = new ParametrizedProperties();
		prop.load(ConcurrentDumpTest.class.getResourceAsStream("/tbrugz/sqldump/datadump/concurrent.properties"));
		conn = ConnectionUtil.initDBConnection("sqldump", prop);
	}

	@SuppressWarnings("deprecation")
	public void doInitDump(String className) throws Exception {
		DumpSyntax ds = (DumpSyntax) Utils.getClassInstance(className);
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		StringWriter sw1 = new StringWriter();
		StringWriter sw2 = new StringWriter();
		{
			PreparedStatement ps = conn.prepareStatement("select 1 as a, 2 as b union select 2, 3");
			rs1 = ps.executeQuery();
			//DumpSyntax ds1 = (DumpSyntax) Utils.getClassInstance("tbrugz.sqldump.datadump.HTMLDataDump");
			ds.procProperties(prop);
			ds.initDump("schema_x", "table_x", null, rs1.getMetaData());
			ds.dumpHeader(sw1);
			rs1.next();
			ds.dumpRow(rs1, 1, sw1);
		}
		
		{
			PreparedStatement ps = conn.prepareStatement("select 3 as a, 4 as b, 5 as c");
			rs2 = ps.executeQuery();
			//DumpSyntax ds1 = (DumpSyntax) Utils.getClassInstance("tbrugz.sqldump.datadump.HTMLDataDump");
			ds.procProperties(prop);
			ds.initDump("schema_x", "table_x", null, rs2.getMetaData());
			ds.dumpHeader(sw2);
			rs2.next();
			ds.dumpRow(rs2, 1, sw2);
		}

		rs1.next();
		ds.dumpRow(rs1, 2, sw1);
		System.out.println("sw1:\n"+sw1);
		System.out.println("sw2:\n"+sw2);
	}
	
	public void doDumpSyntaxBuilder(String dumpClass) throws Exception {
		DumpSyntaxBuilder dsb = (DumpSyntaxBuilder) Utils.getClassInstance(dumpClass);
		DumpSyntaxInt ds1 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		StringWriter sw1 = new StringWriter();
		StringWriter sw2 = new StringWriter();
		{
			PreparedStatement ps = conn.prepareStatement("select 1 as a, 2 as b union select 2, 3");
			rs1 = ps.executeQuery();
			//DumpSyntax ds1 = (DumpSyntax) Utils.getClassInstance("tbrugz.sqldump.datadump.HTMLDataDump");
			dsb.procProperties(prop);
			ds1 = dsb.build("schema_x", "table_x", null, rs1.getMetaData());
			ds1.dumpHeader(sw1);
			rs1.next();
			ds1.dumpRow(rs1, 1, sw1);
		}
		
		{
			PreparedStatement ps = conn.prepareStatement("select 1 as a, 2 as b union select 2, 3");
			rs2 = ps.executeQuery();
			//DumpSyntax ds1 = (DumpSyntax) Utils.getClassInstance("tbrugz.sqldump.datadump.HTMLDataDump");
			dsb.procProperties(prop);
			DumpSyntaxInt ds = dsb.build("schema_x", "table_x", null, rs2.getMetaData());
			ds.dumpHeader(sw1);
			rs2.next();
			ds.dumpRow(rs2, 1, sw1);
		}

		rs1.next();
		ds1.dumpRow(rs1, 2, sw1);
		System.out.println("sw1:\n"+sw1);
		System.out.println("sw2:\n"+sw2);
	}

	@Test(expected=SQLException.class)
	public void testInitCsv() throws Exception {
		doInitDump(CSVDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderCsv() throws Exception {
		doDumpSyntaxBuilder(CSVDataDump.class.getCanonicalName());
	}
	
	@Test(expected=SQLException.class)
	public void testInitHtml() throws Exception {
		doInitDump(HTMLDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderHtml() throws Exception {
		doDumpSyntaxBuilder(HTMLDataDump.class.getCanonicalName());
	}
	
	@Test(expected=SQLException.class)
	public void testInitJson() throws Exception {
		doInitDump(JSONDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderJson() throws Exception {
		doDumpSyntaxBuilder(JSONDataDump.class.getCanonicalName());
	}
	
	@Test(expected=SQLException.class)
	public void testInitXml() throws Exception {
		doInitDump(XMLDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderXml() throws Exception {
		doDumpSyntaxBuilder(XMLDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderFFC() throws Exception {
		doDumpSyntaxBuilder(FFCDataDump.class.getCanonicalName());
	}
	
	@Test
	public void testBuilderInsertInto() throws Exception {
		doDumpSyntaxBuilder(InsertIntoDataDump.class.getCanonicalName());
	}
	
	/*
		TODO: test builder with:
		CacheRowSetSyntax
		CSVWithRowNumber
		DeleteByPK
		DocbookTable
		InsertIntoDatabase
		JOpenDocODS
		PoiXlsSyntax
		PoiXlsxSyntax
		SimpleODS
		Turtle
		UpdateByPKDataDump
		WebRowSetSingleSyntax
	*/
	
}
