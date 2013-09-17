package tbrugz.sqldump.ant;

import tbrugz.sqldiff.DiffTwoQueries;

public class DiffTwoQueriesTask extends BaseTask {

	static final String PROP_PREFIX = DiffTwoQueries.PREFIX;
	static final String CLASSNAME = DiffTwoQueries.class.getName();

	@Override
	String getPropPrefix() {
		return PROP_PREFIX;
	}
	
	@Override
	String getClassName() {
		return CLASSNAME;
	}
}
