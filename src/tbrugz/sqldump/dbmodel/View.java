package tbrugz.sqldump.dbmodel;

/*
 * TODO: comments on view and columns
 * 
 * XXXxx: check option: LOCAL, CASCADED, NONE
 * see: http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fsqlp%2Frbafywcohdg.htm
 */
public class View extends DBObject {
	
	public enum CheckOptionType {
		LOCAL, CASCADED, NONE, 
		TRUE; //true: set for databases that doesn't have local and cascaded options 
	}
	
	public String query;
	
	public CheckOptionType checkOption;
	public boolean withReadOnly;
	//public String checkOptionConstraintName;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return (dumpCreateOrReplace?"create or replace ":"create ") + "view "
				+ (dumpSchemaName && schemaName!=null?schemaName+".":"") + name + " as\n" + query
				+ (withReadOnly?"\nwith read only":
					(checkOption!=null && !checkOption.equals(CheckOptionType.NONE)?
						"\nwith "+(checkOption.equals(CheckOptionType.TRUE)?"":checkOption+" ")+"check option":""
					)
				  );
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
