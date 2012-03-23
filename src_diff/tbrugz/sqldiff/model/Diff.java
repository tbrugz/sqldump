package tbrugz.sqldiff.model;

import tbrugz.sqldump.dbmodel.DBObjectType;

public interface Diff {
	public ChangeType getChangeType();
	public String getDiff();
	public DBObjectType getObjectType();
}
