package tbrugz.sqldump.ant;

import tbrugz.sqldiff.SQLDiff;

public class SQLDiffTask extends BaseTask {

	static final String PROP_PREFIX = "sqldiff";
	static final String CLASSNAME = SQLDiff.class.getName();

	@Override
	String getPropPrefix() {
		return PROP_PREFIX;
	}
	
	@Override
	String getClassName() {
		return CLASSNAME;
	}
}
