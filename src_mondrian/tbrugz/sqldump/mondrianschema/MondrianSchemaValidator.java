package tbrugz.sqldump.mondrianschema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.OlapConnection;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.NamedList;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * info:
 * http://olap4j-demo.googlecode.com/svn/trunk/doc/Olap4j_Introduction_An_end_user_perspective.pdf
 * http://mondrian.pentaho.com/documentation/configuration.php
 * 
 * XXX: more/better debugging info - more "validation"?
 * XXX: dtd/xsd validation?
 * XXX: what if in-memory database? concurrent connections?
 */
public class MondrianSchemaValidator extends AbstractSQLProc {

	static Log log = LogFactory.getLog(MondrianSchemaValidator.class);
	
	public static final String PROP_MONDRIAN_VALIDATOR_SCHEMAFILE = "sqldump.mondrianvalidator.schemafile";

	Properties prop = null;

	String dataSource = null;
	String driverClass = null;
	String url = null;
	String username = null;
	String password = null;
	
	String schemaFile = null;
	
	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
		
		String connPrefix = prop.getProperty(SQLDump.PROP_CONNPROPPREFIX);
		if(connPrefix==null) {
			connPrefix = SQLDump.CONN_PROPS_PREFIX;
		}

		dataSource = prop.getProperty(connPrefix+SQLUtils.ConnectionUtil.SUFFIX_CONNECTION_DATASOURCE);
		driverClass = prop.getProperty(connPrefix+SQLUtils.ConnectionUtil.SUFFIX_DRIVERCLASS);
		url = prop.getProperty(connPrefix+SQLUtils.ConnectionUtil.SUFFIX_URL);
		username = prop.getProperty(connPrefix+SQLUtils.ConnectionUtil.SUFFIX_USER);
		password = prop.getProperty(connPrefix+SQLUtils.ConnectionUtil.SUFFIX_PASSWD);
		
		schemaFile = prop.getProperty(PROP_MONDRIAN_VALIDATOR_SCHEMAFILE);
		if(schemaFile==null) {
			schemaFile = prop.getProperty(MondrianSchemaDumper.PROP_MONDRIAN_SCHEMA_OUTFILE);
		}
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	@Override
	public void setFailOnError(boolean failonerror) {
	}

	@Override
	public void process() {
		try {
			validate();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void validate() throws ClassNotFoundException, SQLException {
		log.info("validating mondrian schema [driver="+driverClass+"; url="+url+"; schema="+schemaFile+"]");

		//construct mondrian URL
		String mondrianUrl = 
				"jdbc:mondrian:" +
				"JdbcDrivers="+driverClass+";" +
				//"Jdbc="+url+";" +
				"Catalog="+schemaFile+";";
		if(dataSource!=null) {
			mondrianUrl += "DataSource=" + dataSource + ";";
		}
		else {
			mondrianUrl += "Jdbc="+url+";";
		}
		if(username != null && username.length() > 0) {
			mondrianUrl += "JdbcUser=" + username + ";";
		}
		if(password != null && password.length() > 0) {
			mondrianUrl += "JdbcPassword=" + password + ";";
		}

		//create connection
		Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
		Connection connection = DriverManager.getConnection(mondrianUrl);
		OlapConnection oConnection = connection.unwrap(OlapConnection.class);
		
		//show cube names
		NamedList<Cube> cubes = oConnection.getOlapSchema().getCubes();
		List<String> cubeNames = new ArrayList<String>();
		for(Cube cube: cubes) {
			cubeNames.add(cube.getUniqueName());
		}
		log.info("cubes: "+Utils.join(cubeNames, ", "));
		
		//close connection
		oConnection.close();
		connection.close();
	}

}
