package tbrugz.sqldiff.model;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

/*
 * XXX option to change 'new:' & 'old:' xtra comments?
 */
public class DBIdentifiableDiff implements Diff, Comparable<DBIdentifiableDiff> {
	final ChangeType changeType;
	final DBIdentifiable ident;
	final DBIdentifiable previousIdent;
	final String ownerTableName;
	
	static boolean dumpSchemaName = true;
	static boolean addComments = true;

	public DBIdentifiableDiff(ChangeType changeType, DBIdentifiable previousIdent, DBIdentifiable ident, String ownerTableName) {
		this.changeType = changeType;
		this.previousIdent = previousIdent;
		this.ident = ident;
		this.ownerTableName = ownerTableName;
	}

	/*public DBIdentifiableDiff(ChangeType changeType, DBIdentifiable previousIdent, DBIdentifiable ident) {
		this(changeType, previousIdent, ident, null);
	}*/
	
	@Override
	public ChangeType getChangeType() {
		return changeType;
	}

	@Override
	public String getDiff() {
		switch(changeType) {
			case ADD: return (ownerTableName!=null?"alter table "+ownerTableName+" add ":"")
					+ ident.getDefinition(true).trim()
					+ (addComments?getComment(previousIdent, "old: "):"");
			//case ALTER:  return "ALTER "+ident.getDefinition(true);
			//case RENAME:  return "RENAME "+ident.getDefinition(true);
			case DROP:
				if(ownerTableName!=null) {
					return "alter table "+ownerTableName+" drop "
							+ DBIdentifiable.getType4Diff(previousIdent).desc()+" "+DBObject.getFinalIdentifier(previousIdent.getName())
							+ (addComments?getComment(ident, "new: "):"");
				}
				return "drop "
					+ DBIdentifiable.getType4Diff(previousIdent).desc()+" "+DBObject.getFinalName(previousIdent, dumpSchemaName)
					+ (addComments?getComment(ident, "new: "):"");
		}
		throw new RuntimeException("changetype "+changeType+" not defined on DBIdentifiableDiff.getDiff()");
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DBIdentifiableDiff) {
			DBIdentifiableDiff dbid = (DBIdentifiableDiff) obj;
			if(changeType.equals(dbid.changeType)) {
				if(ident!=null) { return ident.equals(dbid.ident); }
				else { return previousIdent.equals(dbid.previousIdent); }
			}
			return false;
		}
		return false;
	}

	@Override
	public int compareTo(DBIdentifiableDiff o) {
		int comp = changeType.compareTo(o.changeType);
		if(comp==0) {
			return ident().getName().compareTo(o.ident().getName());
		}
		return comp;
	}

	@Override
	public DBObjectType getObjectType() {
		return DBIdentifiable.getType4Diff(ident()); //XXX: getType() or getType4Diff()? '4Diff' is better for logging...
	}
	
	@Override
	public NamedDBObject getNamedObject() {
		return ident();
	}
	
	public DBIdentifiable ident() {
		return ident!=null?ident:previousIdent;
	}
	
	static String getComment(DBIdentifiable dbident, String comment) {
		if(dbident==null) return "";
		return "\n/* "+comment
				+ DBIdentifiable.getType4Diff(dbident).desc()+" "
				+ DBObject.getFinalName(dbident, dumpSchemaName)
				+ " */";
	}
	
}
