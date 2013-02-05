package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Table;

public class TableDiff implements Diff, Comparable<TableDiff> {
	static Log log = LogFactory.getLog(TableDiff.class);

	final ChangeType diffType; //ADD, ALTER, RENAME, DROP;
	final String renameFrom;
	final Table table;

	public TableDiff(ChangeType changeType, Table table) {
		this.diffType = changeType;
		this.table = table;
		this.renameFrom = null; //XXX: add constructor parameter?
	}
	
	@Override
	public String getDiff() {
		switch(diffType) {
			case ADD:
				return table.getDefinition(true); //XXX: is it useful?
				//return getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable, colTypeConversionProp, foreignKeys);
			case ALTER:
				return null; //XXX: alter table...??
			case RENAME:
				return "alter table "+renameFrom+" rename to "+table.getName();
			case DROP:
				return "drop table "+(table.getSchemaName()!=null?table.getSchemaName()+".":"")+table.getName();
		}
		return null;
	}
	
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
			Column cNew = SchemaDiff.getDBIdentifiableByTypeSchemaAndName(newTable.getColumns(), DBObjectType.COLUMN, origTable.getSchemaName(), cOrig.getName());
			if(cNew!=null) {
				newColumnsThatExistsInOrigModel.add(cNew);
				//boolean equal = cNew.equals(cOrig);
				boolean equal = Column.getColumnDesc(cOrig).equals(Column.getColumnDesc(cNew));
				if(!equal) {
					//alter column
					log.debug("alter column: orig: "+cOrig+" new: "+cNew);
					TableColumnDiff tcd = new TableColumnDiff(ChangeType.ALTER, newTable, cOrig, cNew);
					diffs.add(tcd);
				}
				//else {
					//log.info("equal columns: cOrig: "+cOrig+", cNew: "+cNew);
				//}
			}
			else {
				log.debug("drop column: orig: "+cOrig);
				TableColumnDiff tcd = new TableColumnDiff(ChangeType.DROP, origTable, cOrig, null);
				diffs.add(tcd);
			}
		}
		for(Column cNew: newTable.getColumns()) {
			if(newColumnsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.debug("add column: new: "+cNew);
			TableColumnDiff tcd = new TableColumnDiff(ChangeType.ADD, newTable, null, cNew);
			diffs.add(tcd);
		}
		
		//constraints
		//XXX: constraints should be dumper in defined order (FKs at end)
		List<DBIdentifiableDiff> dbiddiffs = new ArrayList<DBIdentifiableDiff>();
		diffs(DBObjectType.CONSTRAINT, dbiddiffs, origTable.getConstraints(), newTable.getConstraints(), origTable.getQualifiedName(), newTable.getQualifiedName());
		//FIXedME: schemaname dumps as null
		for(int i=0;i<dbiddiffs.size();i++) {
			dbiddiffs.get(i).ident().setSchemaName(newTable.getSchemaName());
		}
		diffs.addAll(dbiddiffs);
		
		return diffs;
	}
	
	//XXX: move to another class
	public static void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew) {
		diffs(objType, diffs, listOrig, listNew, null, null);
	}

	//XXX: move to another class
	public static void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origOwnerTableName, String newOwnerTableName) {
		Set<DBIdentifiable> newDBObjectsThatExistsInOrigModel = new HashSet<DBIdentifiable>();
		for(DBIdentifiable cOrig: listOrig) {
			DBIdentifiable cNew = SchemaDiff.getDBIdentifiableByTypeSchemaAndName(listNew, DBIdentifiable.getType4Diff(cOrig), cOrig.getSchemaName(), cOrig.getName());
			if(cNew!=null) {
				newDBObjectsThatExistsInOrigModel.add(cNew);
				if(!cOrig.equals(cNew)) {
					log.debug("drop/add "+objType+": orig: "+cOrig+" new: "+cNew);
					diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, cNew, origOwnerTableName));
					diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cOrig, cNew, newOwnerTableName));
				}
			}
			else {
				log.debug("drop "+objType+": orig: "+cOrig);
				diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, null, origOwnerTableName));
			}
		}
		for(DBIdentifiable cNew: listNew) {
			if(newDBObjectsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.debug("add "+objType+": new: "+cNew);
			diffs.add(new DBIdentifiableDiff(ChangeType.ADD, null, cNew, newOwnerTableName));
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

}
