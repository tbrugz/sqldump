package tbrugz.sqldump.dbmodel;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public interface TypedDBObject extends NamedDBObject {

	public DBObjectType getDbObjectType();
	
	@Deprecated
	public DBObjectType getDBObjectType();

}
