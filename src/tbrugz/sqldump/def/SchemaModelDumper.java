package tbrugz.sqldump.def;

import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelDumper extends ProcessComponent {

	public void setProperties(Properties prop);
	
	public void setPropertiesPrefix(String propertiesPrefix);
	
	public void setFailOnError(boolean failonerror);
	
	//XXX return errorlevel? not much java-like...
	public void dumpSchema(SchemaModel schemaModel);

}