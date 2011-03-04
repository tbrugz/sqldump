package tbrugz.sqldump.dbmodel;

public class Trigger extends DBObject {
	public String description;
	public String body;
	public String tableName;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create trigger "+description+"\n"+body;
	}
}
