package tbrugz.sqldiff.model;

import tbrugz.sqldiff.ChangeType;
import tbrugz.sqldiff.Diff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.View;

public class DBIdentifiableDiff implements Diff, Comparable<DBIdentifiableDiff> {
	ChangeType changeType;
	DBIdentifiable ident;
	//String prepend;
	String tableName;

	public DBIdentifiableDiff(ChangeType changeType, DBIdentifiable ident, String table) {
		this.changeType = changeType;
		this.ident = ident;
		this.tableName = table;
		//this.prepend = prepend;
	}

	public DBIdentifiableDiff(ChangeType changeType, DBIdentifiable ident) {
		this(changeType, ident, null);
	}
	
	@Override
	public ChangeType getChangeType() {
		return changeType;
	}

	@Override
	public String getDiff() {
		switch(changeType) {
			case ADD: return (tableName!=null?"alter table "+tableName+" ADD ":"")+ident.getDefinition(true).trim();
			//case ALTER:  return "ALTER "+ident.getDefinition(true);
			//case RENAME:  return "RENAME "+ident.getDefinition(true);
			//case DROP: return table!=null?"alter table "+table+" add ":ident.getDefinition(true);
			case DROP: return (tableName!=null?"alter table "+tableName+" ":"")+"DROP "+getType4Diff(ident)+" "+ident.getName();
		}
		throw new RuntimeException("changetype "+changeType+" not defined on DBId.getDiff()");
	}
	
	static DBObjectType getType4Diff(DBIdentifiable ident) {
		if(ident instanceof Column) { return DBObjectType.COLUMN; }
		if(ident instanceof Constraint) { return DBObjectType.CONSTRAINT; }
		//ExecutableObject
		if(ident instanceof FK) { return DBObjectType.CONSTRAINT; }
		//Grant?
		//Index
		//Sequence
		//Synonym
		if(ident instanceof View) { return DBObjectType.VIEW; }
		throw new RuntimeException("getType4Diff: DBObjectType not defined for: "+ident.getClass().getName());
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DBIdentifiableDiff) {
			DBIdentifiableDiff dbid = (DBIdentifiableDiff) obj;
			if(changeType.equals(dbid.changeType)) {
				return ident.equals(dbid.ident);
			}
			return false;
		}
		return false;
	}

	@Override
	public int compareTo(DBIdentifiableDiff o) {
		int comp = changeType.compareTo(o.changeType);
		if(comp==0) {
			return ident.getName().compareTo(o.ident.getName());
		}
		return comp;
	}
}
