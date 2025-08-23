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
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.datadump.DataDump.Outputter;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

public class XlsDataDumpTest {

	static Log log = LogFactory.getLog(XlsDataDumpTest.class);

	static final String DIR_OUT = "target/work/output/DataDumpTest/";
	static final String dbpath = "mem:DataDumpTest;DB_CLOSE_DELAY=-1";
	static final String[] emptyArgs = {};
	
	@BeforeClass
	public static void setupDB() throws Exception {
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/tbrugz/sqldump/sqlrun/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src/test/resources/tbrugz/sqldump/sqlrun/emp.csv",
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.exec.10.import=csv",
				"-Dsqlrun.exec.10.inserttable=etc",
				"-Dsqlrun.exec.10.importfile=src/test/resources/tbrugz/sqldump/sqlrun/etc.csv",
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
	public void testXls() throws ClassNotFoundException, SQLException, NamingException, IOException, EncryptedDocumentException, InvalidFormatException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.dumpsyntaxes=xls, xlsx",
				"-Dsqldump.datadump.xtrasyntaxes=PoiXlsSyntax, PoiXlsxSyntax",
				});

		File f = new File(DIR_OUT+"/data_EMP.xls");
		Workbook wb = WorkbookFactory.create(f);
		Sheet sheet = wb.getSheetAt(0);
		int lastRow = sheet.getLastRowNum();
		//System.out.println(">> lastRow: "+lastRow);
		Assert.assertEquals(5, lastRow);
	}

}
