package tbrugz.sqldump.dbmodel;

/*
 * TODO: mviews: refresh on commit|demand ; tablespace
 */
public class MaterializedView extends View {
	private static final long serialVersionUID = 1L;

	//rewrite_enabled, rewrite_capability, refresh_mode, refresh_method,,, build_mode, fast_refreshable?
	public boolean rewriteEnabled;
	public String rewriteCapability;
	public String refreshMode;
	public String refreshMethod;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return getDefinition(dumpSchemaName, "materialized");
		//return (dumpCreateOrReplace?"create or replace ":"create ") + "materialized view "
		//		+ getFinalName(dumpSchemaName) + " as\n" + query;
	}
	
	@Override
	public String toString() {
		return "MaterializedView["+getName()+"]";
	}
	
	@Override
	public String getRelationType() {
		return "materialized view";
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
