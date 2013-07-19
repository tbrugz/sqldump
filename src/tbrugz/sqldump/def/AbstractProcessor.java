package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public abstract class AbstractProcessor extends AbstractFailable implements Processor {

	protected Properties prop;
	
	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
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

}
