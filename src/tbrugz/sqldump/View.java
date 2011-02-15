package tbrugz.sqldump;

public class View extends DBObject {
	String query;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create view "+(dumpSchemaName && schemaName!=null?schemaName+".":"")+name+" as \n"+query+";";
	}
	
	@Override
	public String toString() {
		return "View["+name+"]";
	}
}
