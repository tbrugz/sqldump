package tbrugz.sqldump.mondrianschema;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.OlapConnection;

public class Olap4jConnector extends MondrianSchemaValidator {
	
	static Log log = LogFactory.getLog(Olap4jConnector.class);
	
	OlapConnection oconn;

	@Override
	public void process() {
		log.info("creating OlapConnection ["
				+(dataSource!=null?"datasource: "+dataSource:(url!=null?"url: "+url:""))
				+"]");
		try {
			oconn = Olap4jUtil.getConnection(driverClass, schemaFile, dataSource, url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Connection getConnection() {
		return oconn;
	}

}
