package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.StringUtils;

//@XmlJavaTypeAdapter(TableDiffAdapter.class)
public class TableDiff extends SingleDiff implements Diff, Comparable<TableDiff> {
	static final Log log = LogFactory.getLog(TableDiff.class);

	final ChangeType diffType; //ADD, ALTER, RENAME, DROP;
	final String renameFromSchema;
	final String renameFromName;
	final String previousRemarks;
	final Table table;
	
	public TableDiff(ChangeType changeType, Table table, String renameFromSchema, String renameFromName, String previousRemarks) {
		this.diffType = changeType;
		this.table = table;
		this.renameFromSchema = renameFromSchema;
		this.renameFromName = renameFromName;
		this.previousRemarks = previousRemarks;
	}

	public TableDiff(ChangeType changeType, Table table) {
		this(changeType, table, null, null, null);
	}
	
	public static TableDiff getTableDiffAddRemarks(Table table, String previousRemarks) {
		return new TableDiff(ChangeType.REMARKS, table, null, null, previousRemarks);
	}
	
	@Override
	public String getDiff() {
		switch(diffType) {
			case ADD:
				return table.getDefinition(true); //XXX: is it useful?
				//return getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable, colTypeConversionProp, foreignKeys);
			case RENAME:
				return "alter table "+(renameFromSchema!=null?renameFromSchema+".":"")+renameFromName+" rename to "+table.getFinalQualifiedName();
			case DROP:
				return "drop table "+table.getFinalQualifiedName();
			case REMARKS:
				return Table.getRelationRemarks(table, true, true);
			case ALTER:
				//throw new IllegalStateException("ALTER table without remarks?");
			case REPLACE:
				throw new IllegalStateException("cannot "+diffType.name()+" a table");
		}
		throw new IllegalStateException("unknown changetype: "+diffType);
	}
	
	/*@Override
	public List<String> getDiffList() {
		return DiffUtil.singleElemList( getDiff() );
	}
	
	@Override
	public int getDiffListSize() {
		return 1;
	}*/
	
	//XXX: move to SchemaDiff or other class?
	public static List<Diff> tableDiffs(Table origTable, Table newTable) {
		//log.debug("difftable:\n"+origTable.getDefinition(true)+"\n"+newTable.getDefinition(true));
		//log.debug("difftable: "+origTable.getName()+" - "+newTable.getName());
		List<Diff> diffs = new ArrayList<Diff>();
		
		//rename
		if(!origTable.getName().equalsIgnoreCase(newTable.getName())) {
			TableDiff td = new TableDiff(ChangeType.RENAME, newTable);
			diffs.add(td);
		}
		
		//alter columns
		//XXX: use diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origPrepend, String newPrepend)??
		Set<Column> newColumnsThatExistsInOrigModel = new HashSet<Column>();
		for(Column cOrig: origTable.getColumns()) {
			Column cNew = DiffUtil.getDBIdentifiableByTypeSchemaAndName(newTable.getColumns(), DBObjectType.COLUMN, origTable.getSchemaName(), cOrig.getName());
			if(cNew!=null) {
				newColumnsThatExistsInOrigModel.add(cNew);
				//boolean equal = cNew.equals(cOrig);
				boolean equal = cOrig.getDefinition4Diff().equals(cNew.getDefinition4Diff());
				if(!equal) {
					//alter column
					log.debug("alter column: orig: "+cOrig+" new: "+cNew);
					ColumnDiff tcd = new ColumnDiff(ChangeType.ALTER, newTable, cOrig, cNew);
					diffs.add(tcd);
				}
				
				if(!StringUtils.equalsNullsAsEmpty(cOrig.getRemarks(), cNew.getRemarks())) {
					ColumnDiff tcd = new ColumnDiff(ChangeType.REMARKS, newTable, cOrig, cNew);
					diffs.add(tcd);
				}
				
				//else {
					//log.info("equal columns: cOrig: "+cOrig+", cNew: "+cNew);
				//}
			}
			else {
				log.debug("drop column: orig: "+cOrig);
				ColumnDiff tcd = new ColumnDiff(ChangeType.DROP, origTable, cOrig, null);
				diffs.add(tcd);
			}
		}
		for(Column cNew: newTable.getColumns()) {
			if(newColumnsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.debug("add column: new: "+cNew);
			ColumnDiff tcd = new ColumnDiff(ChangeType.ADD, newTable, null, cNew);
			diffs.add(tcd);
		}
		
		SchemaDiffer sd = new SchemaDiffer();

		//constraints
		{
			//XXX: constraints should be dumper in defined order (FKs at end)
			List<DBIdentifiableDiff> dbiddiffs = new ArrayList<DBIdentifiableDiff>();
			sd.diffs(DBObjectType.CONSTRAINT, dbiddiffs, origTable.getConstraints(), newTable.getConstraints(), origTable.getFinalQualifiedName(), newTable.getFinalQualifiedName());
			for(int i=0;i<dbiddiffs.size();i++) {
				dbiddiffs.get(i).ident().setSchemaName(newTable.getSchemaName());
			}
			diffs.addAll(dbiddiffs);
		}

		//constraints: FKs
		{
			List<DBIdentifiableDiff> fkdiffs = new ArrayList<DBIdentifiableDiff>();
			sd.diffs(DBObjectType.FK, fkdiffs, origTable.getForeignKeys(), newTable.getForeignKeys(), origTable.getFinalQualifiedName(), newTable.getFinalQualifiedName());
			for(int i=0;i<fkdiffs.size();i++) {
				fkdiffs.get(i).ident().setSchemaName(newTable.getSchemaName());
			}
			diffs.addAll(fkdiffs);
		}
		
		//comments??
		String otRemarks = origTable.getRemarks();
		String ntRemarks = newTable.getRemarks();
		if(!StringUtils.equalsNullsAsEmpty(otRemarks, ntRemarks)) {
			//diffs.add(new RemarksDiff(newTable.getName(), null, ntRemarks));
			diffs.add(TableDiff.getTableDiffAddRemarks(newTable, otRemarks));
		}
		
		//grants
		diffGrants(diffs, origTable, newTable);
		
		return diffs;
	}
	
	static void diffGrants(List<Diff> diffs, Table origTable, Table newTable) {
		List<Grant> origGrants = origTable.getGrants();
		List<Grant> newGrants = newTable.getGrants();
		
		for(Grant og: origGrants) {
			if(!Grant.containsGrant(newGrants, og)) {
				//log.debug("revoke: "+og);
				diffs.add(new GrantDiff(og, origTable.getSchemaName(), origTable.getName(), true));
			}
		}

		for(Grant ng: newGrants) {
			if(!Grant.containsGrant(origGrants, ng)) {
				//log.debug("grant: "+ng);
				diffs.add(new GrantDiff(ng, newTable.getSchemaName(), origTable.getName(), false));
			}
		}
	}

	@Override
	public int compareTo(TableDiff o) {
		int comp = diffType.compareTo(o.diffType);
		if(comp==0) { return table.compareTo(o.table); }
		return comp;
	}

	@Override
	public ChangeType getChangeType() {
		return diffType;
	}

	@Override
	public DBObjectType getObjectType() {
		return DBObjectType.TABLE;
	}
	
	@Override
	public NamedDBObject getNamedObject() {
		return table;
	}
	
	@Override
	public TableDiff inverse() {
		return new TableDiff(diffType.inverse(), table);
	}
	
	public Table getTable() {
		return table;
	}
	
	public String getRenameFromSchema() {
		return renameFromSchema;
	}
	
	public String getRenameFromName() {
		return renameFromName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((diffType == null) ? 0 : diffType.hashCode());
		result = prime * result
				+ ((renameFromName == null) ? 0 : renameFromName.hashCode());
		result = prime
				* result
				+ ((renameFromSchema == null) ? 0 : renameFromSchema.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		TableDiff other = (TableDiff) obj;
		if (diffType != other.diffType)
			return false;
		if (renameFromName == null) {
			if (other.renameFromName != null)
				return false;
		} else if (!renameFromName.equals(other.renameFromName))
			return false;
		if (renameFromSchema == null) {
			if (other.renameFromSchema != null)
				return false;
		} else if (!renameFromSchema.equals(other.renameFromSchema))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	@Override
	public String getDefinition() {
		switch (diffType) {
		case ADD:
			return table.getDefinition(true);
		case DROP:
			return "";
		case RENAME:
			return "/* table renamed: "+table.getQualifiedName()+" */"; //XXX: show table definition on rename
		case REMARKS:
			return Table.getRelationRemarks(table, true, true);
		default:
			throw new IllegalStateException("TableDiff with illegal "+diffType+" state");
		}
	}
	
	@Override
	public String getPreviousDefinition() {
		switch (diffType) {
		case ADD:
			return "";
		case DROP:
			return table.getDefinition(true);
		case RENAME:
			return "/* table renamed: "+renameFromSchema+"."+renameFromName+" */"; //XXX: show table definition on rename
		case REMARKS:
			return Table.getRemarksSql(table, previousRemarks, true);
		default:
			throw new IllegalStateException("TableDiff with illegal "+diffType+" state");
		}
	}

	public String toString() {
		return "TableDiff[type="+diffType+";name="+table.getQualifiedName()+"]";
	}

}
