package tbrugz.sqldiff.model;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public interface Diff {
	public ChangeType getChangeType();
	public String getDiff();
	public DBObjectType getObjectType();
	public NamedDBObject getNamedObject();
}
