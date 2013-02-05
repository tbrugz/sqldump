package tbrugz.sqldump.dbmodel;

public enum DBObjectType {
	TABLE, FK, VIEW, INDEX, EXECUTABLE, TRIGGER, SEQUENCE, SYNONYM, GRANT, //main types
	MATERIALIZED_VIEW, //sub-types?
	FUNCTION, JAVA_SOURCE, PACKAGE, PACKAGE_BODY, PROCEDURE, /*TRIGGER,*/ TYPE, TYPE_BODY, //executable types
	
	CONSTRAINT, COLUMN; //non '1st class' objects
	
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
		}
		
		return this.name().toLowerCase();
	}
}
