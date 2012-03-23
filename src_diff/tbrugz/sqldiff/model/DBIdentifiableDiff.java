package tbrugz.sqldiff.model;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;

public class DBIdentifiableDiff implements Diff, Comparable<DBIdentifiableDiff> {
	ChangeType changeType;
	DBIdentifiable ident;
	String ownerTableName;

	public DBIdentifiableDiff(ChangeType changeType, DBIdentifiable ident, String ownerTableName) {
		this.changeType = changeType;
		this.ident = ident;
		this.ownerTableName = ownerTableName;
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
			case ADD: return (ownerTableName!=null?"alter table "+ownerTableName+" ADD ":"")+ident.getDefinition(true).trim();
			//case ALTER:  return "ALTER "+ident.getDefinition(true);
			//case RENAME:  return "RENAME "+ident.getDefinition(true);
			case DROP: return (ownerTableName!=null?"alter table "+ownerTableName+" ":"")+"DROP "+DBIdentifiable.getType4Diff(ident)+" "+(ident.getSchemaName()!=null?ident.getSchemaName()+".":"")+ident.getName();
		}
		throw new RuntimeException("changetype "+changeType+" not defined on DBId.getDiff()");
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

	@Override
	public DBObjectType getObjectType() {
		//return DBIdentifiable.getType(ident);
		return DBIdentifiable.getType4Diff(ident); //XXX: getType() or getType4Diff()? '4Diff' is better for logging...
	}
}
