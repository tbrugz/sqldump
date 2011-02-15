package tbrugz.sqldump;

public class ExecutableObject extends DBObject {
	String type;
	String body;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return body;
	}
}
