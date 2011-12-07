package tbrugz.sqldump.dbmodel;

/*
 * TODO: mviews: refresh on commit|demand ; tablespace
 */
public class MaterializedView extends View {
	//public boolean materialized = true;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return (dumpCreateOrReplace?"create or replace ":"create ") + "materialized view "
				+ (dumpSchemaName && schemaName!=null?schemaName+".":"") + name + " as \n" + query;
	}
	
	@Override
	public String toString() {
		return "MaterializedView["+name+"]";
	}
	
	/*@Override
	public boolean equals(Object obj) {
		if(obj instanceof View) {
			View v = (View) obj;
			return name.equals(v.name) && query.equals(v.query);
		}
		return false;
	}*/

}
