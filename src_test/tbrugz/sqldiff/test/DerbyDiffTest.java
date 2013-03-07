package tbrugz.sqldiff.test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

public class DerbyDiffTest extends SQLDiffTest {

	@Before
	public void setupConnProperties() {
		dbURL = "jdbc:derby:memory:myDB;create=true";
		dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
		dbUser = "APP";
		
		try {
			DriverManager.getConnection("jdbc:derby:memory:myDB;drop=true");
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
