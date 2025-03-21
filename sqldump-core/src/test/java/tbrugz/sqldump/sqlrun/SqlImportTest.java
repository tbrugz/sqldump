package tbrugz.sqldump.sqlrun;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class SqlImportTest {
	
	@Test
	public void doImportSql() throws Exception {
		String propsStr =
			"sqlrun.driverclass=org.h2.Driver\n"+
			"sqlrun.dburl=jdbc:h2:./target/work/db/sqlimport\n"+
			"sqlrun.exec.005.statement=drop table if exists t1\n"+
			"sqlrun.exec.006.statement=drop table if exists t2\n"+
			"sqlrun.exec.010.statement=create table t1 (ID integer, NAME varchar)\n"+
			"sqlrun.exec.020.statement=insert into t1 (ID, NAME) values (1, 'a'), (2, 'b')\n"+
			"sqlrun.exec.030.statement=create table t2 (ID integer, NAME varchar)\n"+
			
			"sqlrun.exec.050.import=sql\n"+
			"sqlrun.exec.050.read-connection-prefix=sqlrun\n"+
			"sqlrun.exec.050.sql=select * from t1\n"+
			"sqlrun.exec.050.inserttable=t2\n"+
			"";
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(2, CSVImportTest.get1stValue(conn, "select count(*) from t1"));
		Assert.assertEquals(2, CSVImportTest.get1stValue(conn, "select count(*) from t2"));
	}

	@Test
	public void doImportSqlWithInsertSql() throws Exception {
		String propsStr =
			"sqlrun.driverclass=org.h2.Driver\n"+
			"sqlrun.dburl=jdbc:h2:./target/work/db/sqlimport\n"+
			"sqlrun.exec.005.statement=drop table if exists t1\n"+
			"sqlrun.exec.006.statement=drop table if exists t2\n"+
			"sqlrun.exec.010.statement=create table t1 (ID integer, NAME varchar)\n"+
			"sqlrun.exec.020.statement=insert into t1 (ID, NAME) values (1, 'a'), (2, 'b')\n"+
			"sqlrun.exec.030.statement=create table t2 (ID integer, NAME varchar)\n"+
			
			"sqlrun.exec.050.import=sql\n"+
			"sqlrun.exec.050.read-connection-prefix=sqlrun\n"+
			"sqlrun.exec.050.sql=select id from t1\n"+
			"sqlrun.exec.050.insertsql=insert into t2 (id) values (?)\n"+
			"";
		InputStream sis = new ByteArrayInputStream(propsStr.getBytes());

		Properties p = new ParametrizedProperties();
		p.load(sis);
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(null, p);
		
		Connection conn = ConnectionUtil.initDBConnection("sqlrun", p);
		Assert.assertEquals(2, CSVImportTest.get1stValue(conn, "select count(*) from t1"));
		Assert.assertEquals(2, CSVImportTest.get1stValue(conn, "select count(*) from t2"));
	}
	
}
