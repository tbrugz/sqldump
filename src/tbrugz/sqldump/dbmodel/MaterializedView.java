package tbrugz.sqldump.dbmodel;

/*
 * TODO: mviews: refresh on commit|demand ; tablespace
 */
public class MaterializedView extends View {
	private static final long serialVersionUID = 1L;

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return (dumpCreateOrReplace?"create or replace ":"create ") + "materialized view "
				+ getFinalQualifiedName(dumpSchemaName) + " as \n" + query;
	}
	
	@Override
	public String toString() {
		return "MaterializedView["+getName()+"]";
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
