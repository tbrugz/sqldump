package tbrugz.sqldump.dbmodel;

/*
 * TODO: mviews: tablespace/physical attributes
 */
public class MaterializedView extends View {
	private static final long serialVersionUID = 1L;

	//rewrite_enabled, rewrite_capability, refresh_mode, refresh_method
	//XXX: add build_mode, fast_refreshable?
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
	protected String getExtraConstraintsSnippet() {
		StringBuilder sb = new StringBuilder();
		//refresh
		if(refreshMethod!=null) {
			if(refreshMethod.equalsIgnoreCase("never")) {
				sb.append("\nnever refresh");
			}
			else {
				sb.append("\nrefresh "+refreshMethod.toLowerCase()
					+(refreshMode!=null?" on "+refreshMode.toLowerCase():""));
			}
		}
		//rewrite
		if(rewriteEnabled) {
			sb.append("\nenable query rewrite");
		}
		return sb.toString();
	}
	
	//default parameters on create MV: BUILD IMMEDIATE, REFRESH FORCE ON DEMAND, WITH PRIMARY KEY ?
	
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
