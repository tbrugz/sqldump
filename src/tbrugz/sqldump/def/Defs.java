package tbrugz.sqldump.def;

//XXX: rename to Constants?
public class Defs {

	@Deprecated public static final String PROP_TO_DB_ID = "sqldump.todbid";
	public static final String PROP_FROM_DB_ID = "sqldump.fromdbid";

	// grabber properties
	@Deprecated public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	public static final String PROP_SCHEMAGRAB_SCHEMANAMES = "sqldump.schemagrab.schemas";
	
	// program-defined 'special' properties
	//public static final String PROP_START_TIME_MILLIS = "sqlx.startTimeMillis";
	
	static final String DBMS_SPECIFIC_RESOURCE = "dbms-specific.properties";
	
	public static final String[] DEFAULT_CLASSLOADING_PACKAGES = { "tbrugz.sqldump", "tbrugz.sqldump.datadump", "tbrugz.sqldump.processors", "tbrugz", "" };

	public static final String PATTERN_SCHEMANAME = "schemaname";
	public static final String PATTERN_OBJECTTYPE = "objecttype";
	public static final String PATTERN_OBJECTNAME = "objectname";
	public static final String PATTERN_TABLENAME = "tablename";
	public static final String PATTERN_CHANGETYPE = "changetype";
	public static final String PATTERN_SYNTAXFILEEXT = "syntaxfileext";
	
	public static String addSquareBraquets(String patternStr) {
		return "["+patternStr+"]";
	}
	
}
