package tbrugz.sqldump;

import java.io.File;

public abstract class SchemaModelDumper {

	public boolean isDumpWithSchemaName() {
		return false;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {}
	
	public void setOutput(File output) {}

	public abstract void dumpSchema(SchemaModel schemaModel) throws Exception;

}