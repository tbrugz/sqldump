package tbrugz.sqldump.ant;

public class SqlMigrateTask extends BaseTask {

	// XXX: reference actual SqlMigrate class
	static final String PROP_PREFIX = "sqlmigrate";
	static final String CLASSNAME = "tbrugz.sqlmigrate.SqlMigrate";

	@Override
	String getPropPrefix() {
		return PROP_PREFIX;
	}
	
	@Override
	String getClassName() {
		return CLASSNAME;
	}
}
