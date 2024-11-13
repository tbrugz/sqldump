package tbrugz.sqldump.ant;

import tbrugz.sqldump.SQLDump;

public class SQLDumpTask extends BaseTask {

	static final String PROP_PREFIX = "sqldump";
	static final String CLASSNAME = SQLDump.class.getName();

	@Override
	String getPropPrefix() {
		return PROP_PREFIX;
	}
	
	@Override
	String getClassName() {
		return CLASSNAME;
	}
}
