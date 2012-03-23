package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public abstract class AbstractSQLProc {
	
	protected Properties prop;
	protected Connection conn;
	protected SchemaModel model;

	public void setProperties(Properties prop) {
		this.prop = prop;
	}

	public void setPropertiesPrefix(String propertiesPrefix) {}
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public void setSchemaModel(SchemaModel schemamodel) {
		this.model = schemamodel;
	}

	public abstract void process();
	
}
