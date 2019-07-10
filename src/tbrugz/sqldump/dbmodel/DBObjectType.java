package tbrugz.sqldump.dbmodel;

public enum DBObjectType {
	TABLE, FK, VIEW, INDEX, EXECUTABLE, TRIGGER, SEQUENCE, SYNONYM, GRANT, //main types
	MATERIALIZED_VIEW, //sub-types?
	FUNCTION, JAVA_SOURCE, PACKAGE, PACKAGE_BODY, PROCEDURE, SCRIPT, /*TRIGGER,*/ TYPE, TYPE_BODY, //executable types
	AGGREGATE, // derby, h2 & postgresql, at least
	//ALIAS, // XXX h2 'executable'
	
	CONSTRAINT, COLUMN, //REMARKS, //non '1st class' objects
	
	RELATION, QUERY, // "abstract" object types
	
	/** Abstract type for <em>any</em> database object */
	ANY,
	
	;
	
	public enum DBSyntax {
		SQL,
		JAVA
	}
	
	//XXX: include PROGRAM, SCHEDULE?
	//XXX: oracle: java_class, java_resource, java_source: http://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_5013.htm
	//XXX: executables: FUNCTION, JAVA SOURCE, PACKAGE, PACKAGE BODY, PROCEDURE, TRIGGER, TYPE, TYPE BODY - "select distinct type from all_source"
	
	public String desc() {
		switch (this) {
		case JAVA_SOURCE:
			return "java source";
		case MATERIALIZED_VIEW:
			return "materialized view";
		case PACKAGE_BODY:
			return "package body";
		case TYPE_BODY:
			return "type body";
		default:
			return this.name().toLowerCase();
		}
	}
	
	public static DBObjectType parse(String s) {
		if(s!=null) {
			s = s.replace(' ', '_').toUpperCase();
		}
		return DBObjectType.valueOf(s);
	}
	
	public boolean isExecutableType() {
		switch (this) {
		case EXECUTABLE: //generic type, but anyway...
		case FUNCTION:
		case JAVA_SOURCE:
		case PACKAGE:
		case PACKAGE_BODY:
		case PROCEDURE:
		case TYPE:
		case TYPE_BODY:
		case SCRIPT:
		case AGGREGATE:
		//case ALIAS:
			return true;
		default:
			return false;
		}
	}
	
	//XXX add isRelationType()?
	
	public boolean isAbstractType() {
		switch (this) {
		case RELATION:
		//case QUERY:
		case EXECUTABLE:
		case SCRIPT:
		case ANY:
			return true;
		default:
			return false;
		}
	}
	
	public DBSyntax getSyntax() {
		switch (this) {
		case JAVA_SOURCE:
			return DBSyntax.JAVA;
		default:
			return DBSyntax.SQL;
		}
	}

	public String getSyntaxExtension() {
		switch (this) {
		case JAVA_SOURCE:
			return "java";
		default:
			return "sql";
		}
	}
	
	/*public static DBObjectType[] getExecutableTypes() {
		List<DBObjectType> types = new ArrayList<DBObjectType>();
		for(DBObjectType t: DBObjectType.values()) {
			if(t.isExecutableType()) {
				types.add(t);
			}
		}
		return types.toArray(new DBObjectType[]{});
	}*/
	
}
