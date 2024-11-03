package tbrugz.sqldump.dbmsfeatures.oracle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import javax.naming.NamingException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmsfeatures.OracleFeatures;
import tbrugz.sqldump.dbmsfeatures.OracleFeaturesLite;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

@Ignore
public class OracleTest {

	static String connPrefix = "sqldump";
	static Properties prop;
	
	@BeforeClass
	public static void before() throws IOException {
		prop = new ParametrizedProperties();
		prop.load(OracleTest.class.getResourceAsStream("/tbrugz/sqldump/dbmsfeatures/oracle/orcl.properties"));
	}

	@Test
	public void testTableCount() throws ClassNotFoundException, SQLException, NamingException {
		Connection conn = ConnectionUtil.initDBConnection(connPrefix, prop);
		
		DBMSFeatures oFeat = new OracleFeatures();
		oFeat.procProperties(prop);
		DBMSFeatures oFeatLite = new OracleFeaturesLite();
		oFeatLite.procProperties(prop);
		String[] ttypes = {"TABLE"}; // {"TABLE"} {"VIEW"} {"MATERIALIZED VIEW"} {"SYNONYM"}
		String schema = prop.getProperty("t1.schema");

		long init1 = System.currentTimeMillis();
		ResultSet rs = oFeat.getMetadataDecorator(conn.getMetaData()).getTables(null, schema, null, ttypes);
		long e1 = System.currentTimeMillis() - init1;
		int count1 = countRsRows(rs); rs.close();
		
		long init2 = System.currentTimeMillis();
		ResultSet rsLite = oFeatLite.getMetadataDecorator(conn.getMetaData()).getTables(null, schema, null, ttypes);
		long e2 = System.currentTimeMillis() - init2;
		int count2 = countRsRows(rsLite); rsLite.close();
		
		System.out.println("t1: schema = "+schema+" ; types="+Arrays.toString(ttypes));
		System.out.println("OrclFeat    : count="+count1+" ; elapsed="+e1+"ms");
		System.out.println("OrclFeatLite: count="+count2+" ; elapsed="+e2+"ms");
		
		//diff cols 2,3
	}
	
	static int countRsRows(ResultSet rs) throws SQLException {
		int count = 0;
		while(rs.next()) {
			count++;
		}
		return count;
	}

}
