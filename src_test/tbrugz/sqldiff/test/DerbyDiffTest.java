package tbrugz.sqldiff.test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.dbmodel.Column;

public class DerbyDiffTest extends SQLDiffTest {

	Properties derbyProps = new Properties();
	
	@Before
	public void setupConnProperties() {
		dbURL = "jdbc:derby:memory:myDB;create=true";
		dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
		dbUser = "APP";
		
		//FIXME: this should be in sqldump's logic -- sqldump.sqltypes.<dbid>@ignoreprecision ?
		derbyProps.setProperty("sqldump.sqltypes.ignoreprecision", "SMALLINT,BIGINT,INTEGER");
		Column.ColTypeUtil.setProperties(derbyProps);
		
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
