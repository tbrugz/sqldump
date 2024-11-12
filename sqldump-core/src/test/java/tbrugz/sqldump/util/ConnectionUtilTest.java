package tbrugz.sqldump.util;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class ConnectionUtilTest {

	@Before
	public void setup() throws Exception {
		File db = new File("target/work/connection.h2.db");
		db.delete();
		Properties p = new Properties();
		p.load(ConnectionUtilTest.class.getResourceAsStream("conn.properties"));
		p.setProperty("x.password", "abc");
		Connection conn = ConnectionUtil.initDBConnection("x", p);
		conn.close();
	}
	
	@Test(expected=SQLException.class)
	public void testWrongPassword() throws Exception {
		Properties p = new Properties();
		p.load(ConnectionUtilTest.class.getResourceAsStream("conn.properties"));
		p.setProperty("x.password", "def");
		Connection conn = ConnectionUtil.initDBConnection("x", p);
		conn.close();
	}

	@Test
	public void testPasswordOkbase64() throws Exception {
		Properties p = new Properties();
		p.load(ConnectionUtilTest.class.getResourceAsStream("conn.properties"));
		p.setProperty("x.password.base64", "YWJj");
		Connection conn = ConnectionUtil.initDBConnection("x", p);
		conn.close();
	}

}
