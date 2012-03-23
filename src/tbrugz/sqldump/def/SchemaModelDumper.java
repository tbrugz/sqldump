package tbrugz.sqldump.def;

import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelDumper {

	public void procProperties(Properties prop);
	
	public void setPropertiesPrefix(String propertiesPrefix);
	
	/*public boolean isDumpWithSchemaName() {
		return false;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {}*/
	
	public void dumpSchema(SchemaModel schemaModel);

}