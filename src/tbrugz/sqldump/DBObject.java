package tbrugz.sqldump;

public abstract class DBObject {
	String schemaName;
	String name;
	
	public abstract String getDefinition(boolean dumpSchemaName);
}
