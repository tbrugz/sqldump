package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.util.Utils;

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
	public List<String> getDiffList() {
		switch(changeType) {
			case ADD: return DiffUtil.singleElemList( getAddDiffSQL(addComments) );
			case DROP: return DiffUtil.singleElemList( getDropDiffSQL(addComments) );
			case REPLACE:
				//XXX: add DBMSFeatrures.sqlAlterDbIdByDiffing ? create or replace ...
				//XXX: option to do a line-by-line diff/patch as comment ...
				List<String> ret = new ArrayList<String>();
				ret.add( getDropDiffSQL(false) );
				ret.add( getAddDiffSQL(false) + (addComments?getComment(previousIdent, "replacing: "):"") );
				return ret;
			case ALTER:
			case RENAME: {
				if(DBIdentifiable.getType(ident)==DBObjectType.INDEX || DBIdentifiable.getType(ident)==DBObjectType.CONSTRAINT) {
					return DiffUtil.singleElemList( getRenameDiffSQL(addComments) );
				}
				else {
					throw new IllegalStateException("changetype "+changeType+" not defined on DBIdentifiableDiff.getDiff() for type "+
						DBIdentifiable.getType(ident));
				}
			}
			//case REMARKS: //XXX add REMARKS for DBIdentifiableDiff ?
		}
		throw new IllegalStateException("unknown changetype "+changeType+" on DBIdentifiableDiff.getDiff()");
	}
	
	String getAddDiffSQL(boolean dumpComments) {
		return (ownerTableName!=null?"alter table "+ownerTableName+" add ":"")
					+ ident.getDefinition(true).trim()
					+ (dumpComments?getComment(previousIdent, "old: "):"");
	}

	String getDropDiffSQL(boolean dumpComments) {
		if(ownerTableName!=null) {
			return "alter table "+ownerTableName+" drop "
					+ DBIdentifiable.getType4Alter(previousIdent).desc()+" "+DBObject.getFinalIdentifier(previousIdent.getName())
					+ (dumpComments?getComment(ident, "new: "):"");
		}
		return "drop "
			+ DBIdentifiable.getType4Alter(previousIdent).desc()+" "+DBObject.getFinalName(previousIdent, dumpSchemaName)
			+ (dumpComments?getComment(ident, "new: "):"");
	}

	String getRenameDiffSQL(boolean dumpComments) {
		if(ownerTableName!=null) {
			/*
			 * constraint:
			 * ok: oracle, postgresql
			 * nok: h2 (not possible) - http://stackoverflow.com/questions/17510167/h2-database-is-it-possible-to-rename-a-constraint
			 * nok?: mysql
			 */
			return
				"alter table "+ownerTableName+" rename "+DBIdentifiable.getType4Alter(ident).desc()
					+ " " + previousIdent.getName()
					+ " to " + ident.getName()
					+ (dumpComments?getComment(previousIdent, "old: "):"");
		}
		else {
			/*
			 * index:
			 * ok: oracle, postgresql, h2 - http://www.h2database.com/html/grammar.html#alter_index_rename
			 * nok: derby: RENAME INDEX idx TO new_idx
			 * nok: mysql 5.7+: alter table t rename index idx1 to idx2
			 * nok: mariadb: http://stackoverflow.com/questions/19797105/does-mariadb-support-renaming-an-index
			 *      https://mariadb.atlassian.net/browse/MDEV-7318
			 */
			return "alter "+DBIdentifiable.getType4Alter(ident).desc()
				+ " " + previousIdent.getName()
				+ " rename to " + ident.getName()
				+ (dumpComments?getComment(previousIdent, "old: "):"");
		}
	}
	
	@Override
	public String getDiff() {
		return Utils.join(getDiffList(), ";\n");
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
		return DBIdentifiable.getType(ident()); //XXX: getType() or getType4Alter()? 'getType4Alter' is better for logging...
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
				+ DBIdentifiable.getType(dbident).desc()+" "
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
	
	@Override
	public String getDefinition() {
		return ident!=null?ident.getDefinition(true):"";
	}
	
	@Override
	public String getPreviousDefinition() {
		return previousIdent!=null?previousIdent.getDefinition(true):"";
	}
	
	@Override
	public String toString() {
		return "DbIdDiff["+changeType+";"+ident()+"]";
	}
	
	public String getOwnerTableName() {
		return ownerTableName;
	}
	
}
