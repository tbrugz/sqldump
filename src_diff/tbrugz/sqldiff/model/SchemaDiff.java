package tbrugz.sqldiff.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import tbrugz.sqldiff.ChangeType;
import tbrugz.sqldiff.Diff;
import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;

//XXX: should SchemaDiff implement Diff?
//XXX: what about renames?
public class SchemaDiff implements Diff {
	static Logger log = Logger.getLogger(SchemaDiff.class);

	Set<TableDiff> tableDiffs = new TreeSet<TableDiff>();
	Set<TableColumnDiff> columnDiffs = new TreeSet<TableColumnDiff>();
	Set<DBIdentifiableDiff> dbidDiffs = new TreeSet<DBIdentifiableDiff>();

	public static SchemaDiff diff(SchemaModel modelOrig, SchemaModel modelNew) {
		SchemaDiff diff = new SchemaDiff();
		
		//tables
		Set<Table> newTablesThatExistsInOrigModel = new HashSet<Table>();
		for(Table tOrig: modelOrig.getTables()) {
			Table tNew = (Table) DBObject.findDBObjectByName(modelNew.getTables(), tOrig.getName());
			if(tNew==null) {
				//if new table doesn't exist, drop old
				TableDiff td = new TableDiff(ChangeType.DROP, tOrig);
				//td.table = tOrig;
				//td.diffType = ChangeType.DROP;
				diff.tableDiffs.add(td);
			}
			else {
				//new and old tables exists
				newTablesThatExistsInOrigModel.add(tNew);
				//rename XXX: what about rename? external info needed?
				List<Diff> diffs = TableDiff.tableDiffs(tOrig, tNew);
				
				//FKs
				TableDiff.diffs(DBObjectType.FK, diff.dbidDiffs, 
						getFKsFromTable(modelOrig.getForeignKeys(), tOrig.getName()), 
						getFKsFromTable(modelNew.getForeignKeys(), tNew.getName()), 
						tOrig.getName(),
						tNew.getName());
				
				for(Diff dt: diffs) {
					if(dt instanceof TableDiff) {
						diff.tableDiffs.add((TableDiff)dt);
					}
					else if(dt instanceof TableColumnDiff) {
						diff.columnDiffs.add((TableColumnDiff)dt);
					}
					else if(dt instanceof DBIdentifiableDiff) {
						diff.dbidDiffs.add((DBIdentifiableDiff)dt);
					}
					else {
						log.warn("unknown diff: "+dt);
					}
				}
				//diff.tableDiffs.addAll(diffs);
				//column changes
			}
		}
		
		/*Set<Table> newTables = new HashSet<Table>();
		newTables.addAll(modelNew.getTables());
		newTables.removeAll(newTablesThatExistsInOrigModel);
		diff.getTables().addAll(newTables);*/
		/*for(Table tNew: newTables) {
			diff.getTables().add(tNew);
		}*/
		
		Set<Table> addTables = new TreeSet<Table>();
		
		//add tables
		addTables.addAll(modelNew.getTables());
		addTables.removeAll(newTablesThatExistsInOrigModel);
		for(Table t: addTables) {
			TableDiff td = new TableDiff(ChangeType.ADD, t);
			diff.tableDiffs.add(td);
		}

		//TODO: Table: grants, table.type?
		
		//Views
		TableDiff.diffs(DBObjectType.VIEW, diff.dbidDiffs, modelOrig.getViews(), modelNew.getViews());
		
		//TODO: triggers, executables, synonyms, indexes, sequences
		
		//XXX: query tableDiffs and columnDiffs: set schema.type: ADD, ALTER, DROP 
		logInfo(diff);
		
		return diff;
	}
	
	static void logInfo(SchemaDiff diff) {
		//log.info("oldTables                     : "+modelOrig.getTables());
		//log.info("newTables                     : "+modelNew.getTables());
		//log.info("newTablesThatExistsInOrigModel: "+newTablesThatExistsInOrigModel);
		//XXX log.info("addedTables...................: "+diff.getTables().size());
		log.info("tableDiffs....................: "+diff.tableDiffs.size());
		log.info("  add.........................: "+SQLDiff.getDiffOfType(ChangeType.ADD, diff.tableDiffs).size());
		log.info("  alter.......................: "+SQLDiff.getDiffOfType(ChangeType.ALTER, diff.tableDiffs).size());
		log.info("  rename......................: "+SQLDiff.getDiffOfType(ChangeType.RENAME, diff.tableDiffs).size());
		log.info("  drop........................: "+SQLDiff.getDiffOfType(ChangeType.DROP, diff.tableDiffs).size());
		log.info("tableColumnDiffs..............: "+diff.columnDiffs.size());
		log.info("  add.........................: "+SQLDiff.getDiffOfType(ChangeType.ADD, diff.columnDiffs).size());
		log.info("  alter.......................: "+SQLDiff.getDiffOfType(ChangeType.ALTER, diff.columnDiffs).size());
		log.info("  rename......................: "+SQLDiff.getDiffOfType(ChangeType.RENAME, diff.columnDiffs).size());
		log.info("  drop........................: "+SQLDiff.getDiffOfType(ChangeType.DROP, diff.columnDiffs).size());
	}
	
	static Set<FK> getFKsFromTable(Set<FK> fks, String table) {
		Set<FK> retfks = new HashSet<FK>();
		for(FK fk: fks) {
			if(fk.fkTable.equals(table)) { retfks.add(fk); }
		}
		return retfks;
	}
	
	@Override
	public String getDiff() {
		StringBuffer sb = new StringBuffer();
		//tables
		for(TableDiff td: tableDiffs) {
			sb.append(td.getDiff()+";\n\n");
		}
		//columns
		for(TableColumnDiff tcd: columnDiffs) {
			sb.append(tcd.getDiff()+";\n\n");
		}
		//dbidentifiable
		for(DBIdentifiableDiff dbdiff: dbidDiffs) {
			sb.append(dbdiff.getDiff()+";\n\n");
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return "[SchemaDiff: tables: #"+tableDiffs.size()+", cols: #"+columnDiffs.size()+", xtra: #"+dbidDiffs.size()+"]"; //XXX: A(dd), M(modified), R(emoved)
	}

	@Override
	public ChangeType getChangeType() {
		return null; //XXX: SchemaDiff.ChangeType?
	}

}
