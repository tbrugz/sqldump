package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public abstract class AbstractSQLProc implements Processor {
	
	protected Properties prop;
	protected Connection conn;
	protected SchemaModel model;

	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {}
	
	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void setSchemaModel(SchemaModel schemamodel) {
		this.model = schemamodel;
	}

	@Override
	public abstract void process();
	
}
