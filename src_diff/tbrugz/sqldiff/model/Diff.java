package tbrugz.sqldiff.model;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

//XXX Diff<T> ?
//XXX implements Comparable<Diff> ?
public interface Diff {
	public ChangeType getChangeType();
	public String getDiff();
	public DBObjectType getObjectType();
	public NamedDBObject getNamedObject();
	//XXX add public List<Diff<?>> getChildren()? maybe not (only SchemaDiff would use it)
	//public Diff<T> inverse()?
	public Diff inverse();
}
