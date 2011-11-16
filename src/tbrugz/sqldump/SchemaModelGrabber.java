package tbrugz.sqldump;

import java.sql.Connection;
import java.util.Properties;

public interface SchemaModelGrabber {

	public void procProperties(Properties prop);

	public boolean needsConnection();

	public void setConnection(Connection conn);

	public SchemaModel grabSchema();

}