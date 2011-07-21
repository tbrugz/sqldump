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
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof View) {
			View v = (View) obj;
			return name.equals(v.name) && query.equals(v.query);
		}
		return false;
	}
}
