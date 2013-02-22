package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelGrabber {

	public void procProperties(Properties prop);

	public void setPropertiesPrefix(String propertiesPrefix);
	
	public boolean needsConnection();

	public void setConnection(Connection conn);

	public void setFailOnError(boolean failonerror);

	public SchemaModel grabSchema();

}