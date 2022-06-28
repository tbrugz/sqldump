package tbrugz.sqldump.mondrianschema;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.OlapConnection;

import tbrugz.sqldump.def.ProcessingException;

public class Olap4jConnector extends MondrianSchemaValidator {
	
	static Log log = LogFactory.getLog(Olap4jConnector.class);
	
	OlapConnection oconn;

	@Override
	public void process() {
		if(schemaFile==null) {
			String message = "schemafile [prop '"+PROP_MONDRIAN_VALIDATOR_SCHEMAFILE+"'] nor mondrian outfile [prop '"+MondrianSchemaDumper.PROP_MONDRIAN_SCHEMA_OUTFILE+"'] are defined";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
		}
		
		log.info("creating OlapConnection ["
				+(dataSource!=null?"datasource: "+dataSource:(url!=null?"url: "+url:""))
				+"]");
		try {
			oconn = Olap4jUtil.getConnection(driverClass, schemaFile, dataSource, url, username, password);
		} catch (Exception e) {
			log.warn("error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	@Override
	public Connection getNewConnection() {
		return oconn;
	}

}
