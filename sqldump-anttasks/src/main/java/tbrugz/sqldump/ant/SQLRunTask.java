package tbrugz.sqldump.ant;

import tbrugz.sqldump.sqlrun.SQLRun;

public class SQLRunTask extends BaseTask {

	static final String PROP_PREFIX = "sqlrun";
	static final String CLASSNAME = SQLRun.class.getName();

	@Override
	String getPropPrefix() {
		return PROP_PREFIX;
	}
	
	@Override
	String getClassName() {
		return CLASSNAME;
	}
}
