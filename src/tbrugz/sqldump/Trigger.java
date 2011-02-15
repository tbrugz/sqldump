package tbrugz.sqldump;

public class Trigger extends DBObject {
	String description;
	String body;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create trigger "+description+"\n"+body;
	}
}
