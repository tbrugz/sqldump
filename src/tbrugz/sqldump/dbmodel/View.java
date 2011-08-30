package tbrugz.sqldump.dbmodel;

/*
 * XXX: materialized views should subclass 'View'?
 */
public class View extends DBObject {
	public String query;
	public boolean materialized;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create "+(materialized?"materialized ":"")+"view "+(dumpSchemaName && schemaName!=null?schemaName+".":"")+name+" as \n"+query;
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
