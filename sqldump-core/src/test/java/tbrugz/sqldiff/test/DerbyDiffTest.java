package tbrugz.sqldiff.test;

import java.sql.DriverManager;
import java.sql.SQLException;

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
