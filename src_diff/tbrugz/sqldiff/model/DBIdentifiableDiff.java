package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

/*
 * XXX option to change 'new:' & 'old:' xtra comments?
 */
//@XmlType(factoryMethod="newInstance")
//@XmlJavaTypeAdapter(DBIdentifiableDiffAdapter.class)
public class DBIdentifiableDiff implements Diff, Comparable<DBIdentifiableDiff> {
	final ChangeType changeType;
	final DBIdentifiable ident;
	final DBIdentifiable previousIdent;
	final String ownerTableName;
	
	static boolean dumpSchemaName = true;
	public static boolean addComments = true;

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
			case ADD: return getAddDiffSQL();
			case DROP: return getDropDiffSQL();
			case REPLACE:
			case ALTER:
			case RENAME:
				throw new IllegalStateException("changetype "+changeType+" not defined on DBIdentifiableDiff.getDiff()");
		}
		throw new IllegalStateException("unknown changetype "+changeType+" on DBIdentifiableDiff.getDiff()");
	}
	
	String getAddDiffSQL() {
		return (ownerTableName!=null?"alter table "+ownerTableName+" add ":"")
					+ ident.getDefinition(true).trim()
					+ (addComments?getComment(previousIdent, "old: "):"");		
	}

	String getDropDiffSQL() {
		if(ownerTableName!=null) {
			return "alter table "+ownerTableName+" drop "
					+ DBIdentifiable.getType4Diff(previousIdent).desc()+" "+DBObject.getFinalIdentifier(previousIdent.getName())
					+ (addComments?getComment(ident, "new: "):"");
		}
		return "drop "
			+ DBIdentifiable.getType4Diff(previousIdent).desc()+" "+DBObject.getFinalName(previousIdent, dumpSchemaName)
			+ (addComments?getComment(ident, "new: "):"");
	}
	
	@Override
	public List<String> getDiffList() {
		List<String> ret = new ArrayList<String>();
		ret.add(getDiff());
		return ret;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((changeType == null) ? 0 : changeType.hashCode());
		result = prime * result + ((ident == null) ? 0 : ident.hashCode());
		result = prime * result
				+ ((ownerTableName == null) ? 0 : ownerTableName.hashCode());
		result = prime * result
				+ ((previousIdent == null) ? 0 : previousIdent.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBIdentifiableDiff other = (DBIdentifiableDiff) obj;
		if (changeType != other.changeType)
			return false;
		if (ident == null) {
			if (other.ident != null)
				return false;
		} else if (!ident.equals(other.ident))
			return false;
		if (ownerTableName == null) {
			if (other.ownerTableName != null)
				return false;
		} else if (!ownerTableName.equals(other.ownerTableName))
			return false;
		if (previousIdent == null) {
			if (other.previousIdent != null)
				return false;
		} else if (!previousIdent.equals(other.previousIdent))
			return false;
		return true;
	}

	@Override
	public int compareTo(DBIdentifiableDiff o) {
		int comp = changeType.compareTo(o.changeType);
		if(comp==0) {
			return ident().compareTo(o.ident());
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
	
	@Override
	public DBIdentifiableDiff inverse() {
		return new DBIdentifiableDiff(changeType.inverse(), ident, previousIdent, ownerTableName);
	}
	
	/*
	 * see:
	 * http://stackoverflow.com/questions/7552310/add-override-behavior-on-jaxb-generated-classes-by-extending-them
	 * http://blog.bdoughan.com/2011/06/jaxb-and-factory-methods.html
	 */
	/*
	public static DBIdentifiableDiff newInstance() throws JAXBException {
		return null;
	}
	*/
}
