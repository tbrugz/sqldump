package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelGrabber extends ProcessComponent {

	public void setProperties(Properties prop);

	public void setPropertiesPrefix(String propertiesPrefix);
	
	public boolean needsConnection();

	public Connection getConnection();
	
	public void setConnection(Connection conn);

	public void setFailOnError(boolean failonerror);

	public SchemaModel grabSchema();

}