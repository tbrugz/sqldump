package tbrugz.sqldump.pivot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

public class DriverTest {

	String pivotDriverClass = SQLPivotDriver.class.getName();
	
	@Test
	public void testConnect() throws SQLException, ClassNotFoundException {
		Class.forName(pivotDriverClass); //"tbrugz.sqldump.pivot.SQLPivotDriver");
		
		String url = "jdbc:sqlpivot:h2:mem:abc";
		Connection conn = DriverManager.getConnection(url, null);
		Assert.assertNotNull(conn);
		conn.close();
	}

	@Test
	public void testClass() throws SQLException, ClassNotFoundException {
		Class.forName(pivotDriverClass); //"tbrugz.sqldump.pivot.SQLPivotDriver");
		
		String url = "jdbc:sqlpivot:h2:mem:abc";
		Connection conn = DriverManager.getConnection(url, null);
		
		Statement st = conn.createStatement();
		Assert.assertTrue(st instanceof PivotStatement);

		PreparedStatement pst = conn.prepareStatement("select 1");
		Assert.assertTrue(pst instanceof PivotPreparedStatement);
		conn.close();
	}

}
