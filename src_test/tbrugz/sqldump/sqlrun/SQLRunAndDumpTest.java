package tbrugz.sqldump.sqlrun;

import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class SQLRunAndDumpTest {
	
	public String dbpath = "work/db/empdept";
	
	@Test
	public void doRunAndDumpModel() throws Exception {
		String[] params = {};

		String[] vmparamsRun = {
				"-Dsqlrun.exec.01.file=src_test/tbrugz/sqldump/sqlrun/empdept.sql",
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		SQLRun sqlr = new SQLRun();
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparamsRun);
		sqlr.doMain(params, p);
		
		testForTwoTables();
		
		String[] vmparamsDump = {
					"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.schemadump.dumpclasses=JAXBSchemaXMLSerializer",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/empdept.jaxb.xml",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+dbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		SQLDump sqld = new SQLDump();
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		sqld.doMain(params, p);
	}
	
	void testForTwoTables() throws ClassNotFoundException, SQLException, NamingException {
		SchemaModelGrabber schemaGrabber = new JDBCSchemaGrabber();
		Properties jdbcPropOrig = new Properties();
		String[] vmparams = {
				"-Dsqldump.driverclass=org.h2.Driver",
				"-Dsqldump.dburl=jdbc:h2:"+dbpath,
				"-Dsqldump.user=h",
				"-Dsqldump.password=h"
				};
		TestUtil.setProperties(jdbcPropOrig, vmparams);
		schemaGrabber.procProperties(jdbcPropOrig);
		schemaGrabber.setConnection(TestUtil.getConn(jdbcPropOrig, "sqldump"));
		SchemaModel smOrig = schemaGrabber.grabSchema();
		Assert.assertEquals("should have grabbed 2 tables", 2, smOrig.getTables().size());
	}

	@Test
	public void doRunImportAndDumpModel() throws Exception {
		SQLDump sqld = new SQLDump();
		
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
				"-Dsqlrun.driverclass=org.h2.Driver",
				"-Dsqlrun.dburl=jdbc:h2:"+dbpath,
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		String[] params = {};
		Properties p = new Properties();
		TestUtil.setProperties(p, vmparams);
		//setSystemProperties(vmparams);
		sqld.doMain(params, p);
		
		String[] vmparamsDump = {
					"-Dsqldump.schemagrab.grabclass=JDBCSchemaGrabber",
					"-Dsqldump.schemadump.dumpclasses=JAXBSchemaXMLSerializer, SchemaModelScriptDumper",
					"-Dsqldump.processingclasses=DataDump",
					"-Dsqldump.mainoutputfilepattern=work/output/dbobjects.sql",
					"-Dsqldump.schemadump.dumpdropstatements=true",
					"-Dsqldump.datadump.dumpsyntaxes=insertinto",
					"-Dsqldump.datadump.outfilepattern=work/output/${tablename}.${syntaxfileext}",
					"-Dsqldump.datadump.writebom=false",
					"-Dsqldump.xmlserialization.jaxb.outfile=work/output/empdept.jaxb.xml",
					"-Dsqldump.driverclass=org.h2.Driver",
					"-Dsqldump.dburl=jdbc:h2:"+dbpath,
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		p = new Properties();
		TestUtil.setProperties(p, vmparamsDump);
		//setSystemProperties(vmparamsDump);
		sqld.doMain(params, p);
	}
	
	//not very much unit test 'isolated' - has 'side effects'
	/*@Deprecated
	public static void setSystemProperties(String[] vmparams) {
		for(String s: vmparams) {
			String key = null, value = null; 
			if(s.startsWith("-D")) {
				int i = s.indexOf("=");
				key = s.substring(2,i);
				value = s.substring(i+1);
				System.setProperty(key, value);
			}
		}
	}*/

}
