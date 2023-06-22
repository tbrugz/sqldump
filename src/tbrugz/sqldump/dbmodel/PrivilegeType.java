package tbrugz.sqldump.dbmodel;

public enum PrivilegeType {
	SELECT, INSERT, UPDATE, DELETE, ALTER, REFERENCES, INDEX,
	DEBUG, FLASHBACK, ON_COMMIT_REFRESH, QUERY_REWRITE, EXECUTE, MERGE_VIEW, //Oracle
	RULE, TRIGGER, TRUNCATE, //UNKNOWN?, //PostgreSQL
	//SHOW? // mysql - http://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html
	;
	//XXX: filter privilegetype on output based on dbid... see: column-type-mapping.properties
	
	public boolean allowedForColumn() {
		switch(this) {
		case DELETE:
		case TRIGGER:
		case TRUNCATE:
			return false;
		default:
			return true;
		}
	}
	
	@Override
	public String toString() {
		switch(this) {
			//case ON_COMMIT_REFRESH: return Utils.denormalizeEnumStringConstant(String.valueOf(ON_COMMIT_REFRESH)); 
			//case QUERY_REWRITE: return Utils.denormalizeEnumStringConstant(String.valueOf(QUERY_REWRITE)); 
			case ON_COMMIT_REFRESH: return "ON COMMIT REFRESH";
			case QUERY_REWRITE: return "QUERY REWRITE";
			default: return super.toString(); 
		}
	}
}
