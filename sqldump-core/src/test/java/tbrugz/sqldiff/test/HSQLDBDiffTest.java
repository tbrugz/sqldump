package tbrugz.sqldiff.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

public class HSQLDBDiffTest extends SQLDiffTest {

	//in-memory hsqldb: http://hsqldb.org/doc/guide/ch01.html#N101CA
	
	@Before
	public void setupConnProperties() {
		dbURL = "jdbc:hsqldb:mem:HSQLDBDiffTest";
		dbDriver = "org.hsqldb.jdbcDriver";
		dbUser = "public";
		
		try {
			//see: http://stackoverflow.com/questions/2951013/how-do-you-delete-the-default-database-schema-in-hsqldb
			Connection conn = DriverManager.getConnection(dbURL, dbUser, SQLDiffTest.dbPassword);
			Statement st = conn.createStatement();
			st.executeUpdate("DROP SCHEMA PUBLIC CASCADE");
			st.executeUpdate("CREATE SCHEMA PUBLIC");
			//DBMSResources.instance().updateMetaData(conn.getMetaData());
		} catch (SQLException e) {
			System.err.println("error dropping db: "+e);
		}
	}

	@Test
	@Override
	public void testDiffAddColumn() throws Exception {
		super.testDiffAddColumn();
	}
	
	@Test
	@Override
	public void testDiffCreateTable() throws Exception {
		super.testDiffCreateTable();
	}
	
	@Test
	@Override
	public void testDiffCreateView() throws Exception {
		super.testDiffCreateView();
	}
	
}
