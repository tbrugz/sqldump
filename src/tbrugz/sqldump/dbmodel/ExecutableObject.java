package tbrugz.sqldump.dbmodel;

public class ExecutableObject extends DBObject {
	public String type;
	public String body;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return body;
	}
}
