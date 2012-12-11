package tbrugz.sqldump.sqlrun;

import org.junit.Test;

import tbrugz.sqldump.SQLDump;

public class SQLRunTest {
	@Test
	public void doRun() throws Exception {
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
				"-Dsqlrun.dburl=jdbc:h2:work/db/empdept",
				"-Dsqlrun.user=h",
				"-Dsqlrun.password=h"
				};
		String[] params = {};
		setProperties(vmparams);
		SQLRun.main(params);
		
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
					"-Dsqldump.dburl=jdbc:h2:work/db/empdept",
					"-Dsqldump.user=h",
					"-Dsqldump.password=h"
					};
		setProperties(vmparamsDump);
		SQLDump.main(params);
	}
	
	void setProperties(String[] vmparams) {
		for(String s: vmparams) {
			String key = null, value = null; 
			if(s.startsWith("-D")) {
				int i = s.indexOf("=");
				key = s.substring(2,i);
				value = s.substring(i+1);
				System.setProperty(key, value);
			}
		}
	}
}
