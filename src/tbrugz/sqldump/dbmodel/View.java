package tbrugz.sqldump.dbmodel;

public class View extends DBObject {
	public String query;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create view "+(dumpSchemaName && schemaName!=null?schemaName+".":"")+name+" as \n"+query+";";
	}
	
	@Override
	public String toString() {
		return "View["+name+"]";
	}
}
