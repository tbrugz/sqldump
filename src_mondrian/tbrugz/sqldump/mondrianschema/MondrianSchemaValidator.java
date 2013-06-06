package tbrugz.sqldump.mondrianschema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
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
			if(conn!=null && (conn instanceof OlapConnection)) {
				validateExistingOlapConn();
			}
			else {
				validate();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void validateExistingOlapConn() throws ClassNotFoundException, SQLException {
		log.info("validating mondrian schema [conn="+conn+"]");
		
		validateInternal((OlapConnection) conn);
	}
	
	public void validate() throws ClassNotFoundException, SQLException {
		log.info("validating mondrian schema [driver="+driverClass+"; url="+url+"; schema="+schemaFile+"]");

		OlapConnection oConnection = Olap4jUtil.getConnection(driverClass, schemaFile, dataSource, url, username, password);
		validateInternal(oConnection);
		
		//close connection
		oConnection.close();
	}
	
	void validateInternal(OlapConnection oConnection) throws OlapException {
		//show cube names
		NamedList<Cube> cubes = oConnection.getOlapSchema().getCubes();
		List<String> cubeNames = new ArrayList<String>();
		for(Cube cube: cubes) {
			cubeNames.add(cube.getUniqueName());
		}
		
		int cubeCount = cubeNames.size();
		if(cubeCount==0) {
			log.warn("schema is valid but has no cubes!");
			return;
		}
		
		log.info("cubes [#"+cubeNames.size()+"]: "+Utils.join(cubeNames, ", "));
	}

}
