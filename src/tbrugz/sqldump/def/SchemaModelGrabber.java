package tbrugz.sqldump.def;

import java.sql.Connection;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelGrabber extends ProcessComponent {

	public boolean needsConnection();

	public Connection getConnection();
	
	public void setConnection(Connection conn);

	public SchemaModel grabSchema();

}
