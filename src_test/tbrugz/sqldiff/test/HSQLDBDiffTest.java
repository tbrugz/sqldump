package tbrugz.sqldiff.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.dbmodel.Column;

public class HSQLDBDiffTest extends SQLDiffTest {

	Properties hsqldbProp = new Properties();
	
	//in-memory hsqldb: http://hsqldb.org/doc/guide/ch01.html#N101CA
	
	@Before
	public void setupConnProperties() {
		dbURL = "jdbc:hsqldb:mem:HSQLDBDiffTest";
		dbDriver = "org.hsqldb.jdbcDriver";
		dbUser = "public";
		
		//FIXME: this should be in sqldump's logic -- sqldump.sqltypes.<dbid>@ignoreprecision ?
		hsqldbProp.setProperty("sqldump.sqltypes.ignoreprecision", "SMALLINT,BIGINT,INTEGER");
		Column.ColTypeUtil.setProperties(hsqldbProp);
		
		try {
			Connection conn = DriverManager.getConnection(dbURL, dbUser, SQLDiffTest.dbPassword);
			Statement st = conn.createStatement();
			st.executeUpdate("DROP SCHEMA PUBLIC CASCADE");
			st.executeUpdate("CREATE SCHEMA PUBLIC");
		} catch (SQLException e) {
			System.err.println("error dropping db: "+e);
		}
	}

	@Test
	@Override
	public void testDiffAddColumn() throws ClassNotFoundException,
			SQLException, NamingException, IOException {
		super.testDiffAddColumn();
	}
	
	@Test
	@Override
	public void testDiffCreateTable() throws ClassNotFoundException,
			SQLException, NamingException, IOException {
		super.testDiffCreateTable();
	}
	
	@Test
	@Override
	public void testDiffCreateView() throws ClassNotFoundException,
			SQLException, NamingException, IOException {
		super.testDiffCreateView();
	}
	
}
