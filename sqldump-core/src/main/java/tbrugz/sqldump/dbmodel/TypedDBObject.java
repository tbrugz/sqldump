package tbrugz.sqldump.dbmodel;

public interface TypedDBObject extends NamedDBObject {

	public DBObjectType getDbObjectType();
	
	@Deprecated
	public DBObjectType getDBObjectType();

}
