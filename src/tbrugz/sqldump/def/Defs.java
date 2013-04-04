package tbrugz.sqldump.def;

//XXX: rename to Constants?
public class Defs {

	public static final String PROP_TO_DB_ID = "sqldump.todbid";
	public static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	
	public static final String DBMS_SPECIFIC_RESOURCE = "dbms-specific.properties";

	public static final String PATTERN_SCHEMANAME = "schemaname";
	public static final String PATTERN_OBJECTTYPE = "objecttype";
	public static final String PATTERN_OBJECTNAME = "objectname";
	public static final String PATTERN_TABLENAME = "tablename";
	public static final String PATTERN_CHANGETYPE = "changetype";
	
	public static String addSquareBraquets(String patternStr) {
		return "["+patternStr+"]";
	}
	
}
