package tbrugz.sqldump;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class EmptyModelGrabber implements SchemaModelGrabber {

	@Override
	public void setProperties(Properties prop) {
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	@Override
	public boolean needsConnection() {
		return false;
	}

	@Override
	public Connection getConnection() {
		return null;
	}

	@Override
	public void setConnection(Connection conn) {
	}

	@Override
	public void setFailOnError(boolean failonerror) {
	}

	@Override
	public SchemaModel grabSchema() {
		return new SchemaModel();
	}
	
	@Override
	public void setId(String grabberId) {
	}
	
	@Override
	public String getId() {
		return null;
	}

}
