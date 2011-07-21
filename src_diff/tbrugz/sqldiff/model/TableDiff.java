package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import tbrugz.sqldiff.ChangeType;
import tbrugz.sqldiff.Diff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Table;

public class TableDiff implements Diff, Comparable<TableDiff> {
	static Logger log = Logger.getLogger(TableDiff.class);

	ChangeType diffType; //ADD, ALTER, RENAME, DROP;
	String renameFrom;
	Table table;

	public TableDiff(ChangeType changeType, Table table) {
		this.diffType = changeType;
		this.table = table;
	}
	
	@Override
	public String getDiff() {
		switch(diffType) {
			case ADD:
				return table.getDefinition(true, true, false, null, null); //XXX: is it useful?
				//return getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable, colTypeConversionProp, foreignKeys);
			case ALTER:
				return null; //XXX: alter table...??
			case RENAME:
				return "ALTER TABLE "+renameFrom+" RENAME TO "+table.name+";";
			case DROP:
				return "DROP table "+table.name+";";
		}
		return null;
	}
	
	public static List<Diff> tableDiffs(Table origTable, Table newTable) {
		//log.debug("difftable:\n"+origTable.getDefinition(true)+"\n"+newTable.getDefinition(true));
		//log.debug("difftable: "+origTable.getName()+" - "+newTable.getName());
		List<Diff> diffs = new ArrayList<Diff>();
		
		//rename
		if(!origTable.getName().equals(newTable.getName())) {
			TableDiff td = new TableDiff(ChangeType.RENAME, newTable);
			diffs.add(td);
		}
		
		//alter columns
		Set<Column> newColumnsThatExistsInOrigModel = new HashSet<Column>();
		for(Column cOrig: origTable.getColumns()) {
			Column cNew = DBIdentifiable.getDBIdentifiableByName(newTable.getColumns(), cOrig.getName());
			if(cNew!=null) {
				newColumnsThatExistsInOrigModel.add(cNew);
				boolean equal = cNew.equals(cOrig);
				if(!equal) {
					//alter column
					log.debug("alter column: orig: "+cOrig+" new: "+cNew);
					TableColumnDiff tcd = new TableColumnDiff(ChangeType.ALTER, newTable, cNew);
					diffs.add(tcd);
				}
				//else {
					//log.info("equal columns: cOrig: "+cOrig+", cNew: "+cNew);
				//}
			}
			else {
				log.info("drop column: orig: "+cOrig);
				TableColumnDiff tcd = new TableColumnDiff(ChangeType.DROP, origTable, cOrig);
				diffs.add(tcd);
			}
		}
		for(Column cNew: newTable.getColumns()) {
			if(newColumnsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.info("add column: new: "+cNew);
			TableColumnDiff tcd = new TableColumnDiff(ChangeType.ADD, newTable, cNew);
			diffs.add(tcd);
		}
		
		//constraints
		/*
		Set<Constraint> newConstraintsThatExistsInOrigModel = new HashSet<Constraint>();
		for(Constraint cOrig: origTable.getConstraints()) {
			Constraint cNew = DBIdentifiable.getDBIdentifiableByName(newTable.getConstraints(), cOrig.getName());
			if(cNew!=null) {
				newConstraintsThatExistsInOrigModel.add(cNew);
				if(!cOrig.equals(cNew)) {
					log.debug("add/drop constraint: orig: "+cOrig+" new: "+cNew);
					diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, "alter table "+origTable.getName()));
					diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cNew, "alter table "+newTable.getName()));
				}
			}
			else {
				log.debug("drop constraint: orig: "+cOrig);
				diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, "alter table "+origTable.getName()));
			}
		}
		for(Constraint cNew: newTable.getConstraints()) {
			if(newConstraintsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.debug("add constraint: new: "+cNew);
			diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cNew, "alter table "+newTable.getName()));
		}*/
	
		//constraints
		diffs(DBObjectType.CONSTRAINT, diffs, origTable.getConstraints(), newTable.getConstraints(), "alter table "+origTable.getName(), "alter table "+newTable.getName());
		
		return diffs;
	}
	
	static void diffs(DBObjectType objType, List<Diff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew) {
		diffs(objType, diffs, listOrig, listNew, "", "");
	}

	static void diffs(DBObjectType objType, List<Diff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origPrepend, String newPrepend) {
		//constraints
		Set<DBIdentifiable> newConstraintsThatExistsInOrigModel = new HashSet<DBIdentifiable>();
		for(DBIdentifiable cOrig: listOrig) {
			DBIdentifiable cNew = DBIdentifiable.getDBIdentifiableByName(listNew, cOrig.getName());
			if(cNew!=null) {
				newConstraintsThatExistsInOrigModel.add(cNew);
				if(!cOrig.equals(cNew)) {
					log.debug("add/drop "+objType+": orig: "+cOrig+" new: "+cNew);
					diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, origPrepend));
					diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cNew, newPrepend));
				}
			}
			else {
				log.debug("drop "+objType+": orig: "+cOrig);
				diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, origPrepend));
			}
		}
		for(DBIdentifiable cNew: listNew) {
			if(newConstraintsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.debug("add "+objType+": new: "+cNew);
			diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cNew, newPrepend));
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

}
