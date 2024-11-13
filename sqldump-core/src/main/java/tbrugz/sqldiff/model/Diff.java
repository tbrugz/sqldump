package tbrugz.sqldiff.model;

import java.util.List;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

//XXX Diff<T> ?
//XXX implements Comparable<Diff> ?
public interface Diff {
	public ChangeType getChangeType();
	public String getDiff();
	public List<String> getDiffList();
	public int getDiffListSize();
	public DBObjectType getObjectType();
	public NamedDBObject getNamedObject();
	//XXX add public List<Diff<?>> getChildren()? maybe not (only SchemaDiff would use it)
	//public Diff<T> inverse()?
	public Diff inverse();
	
	public String getDefinition();
	public String getPreviousDefinition();
}
