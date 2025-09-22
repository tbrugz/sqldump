package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public abstract class AbstractProcessor extends AbstractFailable implements Processor {

	protected Properties prop;
	protected String processorId;
	
	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
	}
	
	protected Properties getProperties() {
		return prop;
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	@Override
	public boolean needsConnection() {
		return false;
	}

	@Override
	public boolean needsSchemaModel() {
		return false;
	}

	@Override
	public void setConnection(Connection conn) {
	}

	@Override
	public void setSchemaModel(SchemaModel schemamodel) {
	}

	@Override
	public Connection getConnection() {
		return null;
	}
	
	@Override
	public Connection getNewConnection() {
		return null;
	}
	
	@Override
	public boolean isIdempotent() {
		return false;
	}
	
	@Override
	public String getMimeType() {
		return null;
	}

	@Override
	public String getId() {
		return processorId;
	}
	
	@Override
	public void setId(String processorId) {
		this.processorId = processorId;
	}
	
	public String getIdDesc() {
		return processorId!=null?"["+processorId+"] ":"";
	}
	
}
