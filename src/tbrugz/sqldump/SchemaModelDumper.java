package tbrugz.sqldump;

import java.io.File;
import java.util.Properties;

public abstract class SchemaModelDumper {

	public void procProperties(Properties prop) {}
	
	/*public boolean isDumpWithSchemaName() {
		return false;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {}*/
	
	public void setOutput(File output) {}

	public abstract void dumpSchema(SchemaModel schemaModel) throws Exception;

}