package tbrugz.sqldump;

import java.util.Properties;

//TODOne: SchemaModelDumper should be an interface
public interface SchemaModelDumper {

	public void procProperties(Properties prop);
	
	/*public boolean isDumpWithSchemaName() {
		return false;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {}*/
	
	public abstract void dumpSchema(SchemaModel schemaModel) throws Exception;

}