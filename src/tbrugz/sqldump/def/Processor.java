package tbrugz.sqldump.def;

import java.sql.Connection;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface Processor {

	public void setProperties(Properties prop);

	public void setPropertiesPrefix(String propertiesPrefix);

	public void setConnection(Connection conn);

	public void setSchemaModel(SchemaModel schemamodel);

	public void setFailOnError(boolean failonerror);
	
	public void process();

}