package tbrugz.sqldump.datadump.parquet;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.Utils;

public class ParquetDataDumpTest {

	static final Log log = LogFactory.getLog(ParquetDataDumpTest.class);

	static final String DIR_OUT = "target/work/output/DataDumpTest/";
	static final String dbpath = "mem:DataDumpTest;DB_CLOSE_DELAY=-1";
	static final String[] emptyArgs = {};
	
	@BeforeClass
	public static void setupDB() throws Exception {
		String[] vmparams = {
				"-Dsqlrun.exec.01.file=src/test/resources/empdept.sql",
				"-Dsqlrun.exec.02.import=csv",
				"-Dsqlrun.exec.02.inserttable=dept",
				"-Dsqlrun.exec.02.importfile=src/test/resources/dept.csv",
				"-Dsqlrun.exec.02.skipnlines=1",
				"-Dsqlrun.exec.05.import=csv",
				"-Dsqlrun.exec.05.inserttable=emp",
				"-Dsqlrun.exec.05.importfile=src/test/resources/emp.csv",
				"-Dsqlrun.exec.05.skipnlines=1",
				"-Dsqlrun.exec.10.import=csv",
				"-Dsqlrun.exec.10.inserttable=etc",
				"-Dsqlrun.exec.10.importfile=src/test/resources/etc.csv",
				"-Dsqlrun.exec.10.skipnlines=1",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		String[] params = null;
		Properties p = new Properties();
		TestUtil4Parquet.setProperties(p, vmparams);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(params, p);
	}
	
	@Before
	public void beforeTest() {
		Utils.deleteDirRegularContents(new File(DIR_OUT));
	}
	
	void dumpWithParams(String[] xtraparams) throws ClassNotFoundException, SQLException, NamingException, IOException {
		String[] vmparamsDump = {
				"-Dsqldump.grabclass=JDBCSchemaGrabber",
				"-Dsqldump.processingclasses=DataDump",
				//"-Dsqldump.datadump.dumpsyntaxes=insertinto, csv, xml, html, json",
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
		TestUtil4Parquet.setProperties(p, vmparamsDump);
		if(xtraparams!=null) {
			TestUtil4Parquet.setProperties(p, xtraparams);
		}
		sqld.doMain(emptyArgs, p);
	}
	
	@Test
	public void testAvroSchema() {
		String name = "User";
		List<String> fields = Arrays.asList(new String[]{"id", "name", "available"});
		List<Class<?>> types = Arrays.asList(new Class<?>[]{ Integer.class, String.class, Boolean.class });
		String schema = ParquetSyntax.getAvroSchema(name, fields, types);
		//System.out.println("schema:\n"+schema);
		Schema avroSchema = new Schema.Parser().parse(schema); 
		//System.out.println("avroSchema:\n"+avroSchema);
	}

	@Test
	public void testParquet() throws ClassNotFoundException, SQLException, NamingException, IOException {
		dumpWithParams(new String[]{
				"-Dsqldump.datadump.dumpsyntaxes=parquet",
				"-Dsqldump.datadump.xtrasyntaxes=parquet.ParquetSyntax",
				});

		File f = new File(DIR_OUT+"/data_EMP.parquet");
		Assert.assertTrue(f.exists());
		//Workbook wb = WorkbookFactory.create(f);
		//Sheet sheet = wb.getSheetAt(0);
		//int lastRow = sheet.getLastRowNum();
		//System.out.println(">> lastRow: "+lastRow);
		//Assert.assertEquals(5, lastRow);
	}

}
