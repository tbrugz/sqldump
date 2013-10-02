package tbrugz.sqldump.def;

import java.sql.Connection;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface Processor extends ProcessComponent {

	public boolean needsConnection();

	public boolean needsSchemaModel();
	
	public void setConnection(Connection conn);

	public void setSchemaModel(SchemaModel schemamodel);
	
	public void process();

	public Connection getConnection();

}