package tbrugz.sqldump.def;

import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelDumper {

	public void procProperties(Properties prop);
	
	public void setPropertiesPrefix(String propertiesPrefix);
	
	public void setFailOnError(boolean failonerror);
	
	/*public boolean isDumpWithSchemaName() {
		return false;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {}*/
	
	//XXX return errorlevel? not much java-like...
	public void dumpSchema(SchemaModel schemaModel);

}