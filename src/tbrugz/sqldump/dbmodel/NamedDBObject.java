package tbrugz.sqldump.dbmodel;

public interface NamedDBObject /* extends Comparable<NamedDBObject>? */ {
	public String getName();
	public String getSchemaName();
	
	//public default String getQualifiedName(); // java8
}
