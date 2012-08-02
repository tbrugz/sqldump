package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

//XXX: should SchemaDiff implement Diff?
//XXX: what about renames?
public class SchemaDiff implements Diff {
	static Log log = LogFactory.getLog(SchemaDiff.class);

	//XXX: should be List<>?
	Set<TableDiff> tableDiffs = new TreeSet<TableDiff>();
	Set<TableColumnDiff> columnDiffs = new TreeSet<TableColumnDiff>();
	Set<DBIdentifiableDiff> dbidDiffs = new TreeSet<DBIdentifiableDiff>();

	public static DBObject findDBObjectBySchemaAndName(Collection<? extends DBObject> col, String schemaName, String name) {
		for(DBObject obj: col) {
			if(schemaName.equalsIgnoreCase(obj.getSchemaName()) && name.equalsIgnoreCase(obj.name)) return obj;
		}
		return null;
	}

	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(DBIdentifiable.getType4Diff(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equalsIgnoreCase(schemaName):true) 
					&& d.getName().equalsIgnoreCase(name)) return (T) d;
		}
		return null;
	}


	//-----------------------
	
	void diffTable(SchemaModel modelOrig, SchemaModel modelNew) {
		SchemaDiff diff = this;
		//tables
		Set<Table> newTablesThatExistsInOrigModel = new HashSet<Table>();
		if(modelOrig.getTables().size()>0) {
			
		for(Table tOrig: modelOrig.getTables()) {
			Table tNew = (Table) findDBObjectBySchemaAndName(modelNew.getTables(), tOrig.getSchemaName(), tOrig.getName());
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
	}	
		
	public static SchemaDiff diff(SchemaModel modelOrig, SchemaModel modelNew) {
		SchemaDiff diff = new SchemaDiff();
		
		//Tables
		diff.diffTable(modelOrig, modelNew);
		
		//TODO: Table: grants, table.type?
		
		//Views
		TableDiff.diffs(DBObjectType.VIEW, diff.dbidDiffs, modelOrig.getViews(), modelNew.getViews());
		
		//Triggers
		TableDiff.diffs(DBObjectType.TRIGGER, diff.dbidDiffs, modelOrig.getTriggers(), modelNew.getTriggers());

		//FIXedME: package and package body: findByName must also use object type! (and schemaName!)
		//Executables
		TableDiff.diffs(DBObjectType.EXECUTABLE, diff.dbidDiffs, modelOrig.getExecutables(), modelNew.getExecutables());

		//Synonyms
		//FIXedME: doesn't detect schemaName changes
		TableDiff.diffs(DBObjectType.SYNONYM, diff.dbidDiffs, modelOrig.getSynonyms(), modelNew.getSynonyms());
		
		//Indexes
		TableDiff.diffs(DBObjectType.INDEX, diff.dbidDiffs, modelOrig.getIndexes(), modelNew.getIndexes());

		//Sequences
		TableDiff.diffs(DBObjectType.SEQUENCE, diff.dbidDiffs, modelOrig.getSequences(), modelNew.getSequences());
		
		//XXX: query tableDiffs and columnDiffs: set schema.type: ADD, ALTER, DROP 
		logInfo(diff);
		
		return diff;
	}
	
	static void logInfo(SchemaDiff diff) {
		/*log.info("tableDiffs....................: "+diff.tableDiffs.size());
		log.info("  add.........................: "+SQLDiff.getDiffOfChangeType(ChangeType.ADD, diff.tableDiffs).size());
		log.info("  alter.......................: "+SQLDiff.getDiffOfChangeType(ChangeType.ALTER, diff.tableDiffs).size());
		log.info("  rename......................: "+SQLDiff.getDiffOfChangeType(ChangeType.RENAME, diff.tableDiffs).size());
		log.info("  drop........................: "+SQLDiff.getDiffOfChangeType(ChangeType.DROP, diff.tableDiffs).size());
		log.info("tableColumnDiffs..............: "+diff.columnDiffs.size());
		log.info("  add.........................: "+SQLDiff.getDiffOfChangeType(ChangeType.ADD, diff.columnDiffs).size());
		log.info("  alter.......................: "+SQLDiff.getDiffOfChangeType(ChangeType.ALTER, diff.columnDiffs).size());
		log.info("  rename......................: "+SQLDiff.getDiffOfChangeType(ChangeType.RENAME, diff.columnDiffs).size());
		log.info("  drop........................: "+SQLDiff.getDiffOfChangeType(ChangeType.DROP, diff.columnDiffs).size());
		log.info("dbIdentifiableDiffs...........: "+diff.dbidDiffs.size());
		log.info("  add.........................: "+SQLDiff.getDiffOfChangeType(ChangeType.ADD, diff.dbidDiffs).size());
		log.info("  alter.......................: "+SQLDiff.getDiffOfChangeType(ChangeType.ALTER, diff.dbidDiffs).size());
		log.info("  rename......................: "+SQLDiff.getDiffOfChangeType(ChangeType.RENAME, diff.dbidDiffs).size());
		log.info("  drop........................: "+SQLDiff.getDiffOfChangeType(ChangeType.DROP, diff.dbidDiffs).size());*/
		logInfoByObjectAndChangeType(diff.tableDiffs);
		logInfoByObjectAndChangeType(diff.columnDiffs);
		logInfoByObjectAndChangeType(diff.dbidDiffs);
	}

	static void logInfoByObjectAndChangeType(Collection<? extends Diff> diffs) {
		//Map<DBObjectType, Integer> map = new NeverNullGetMap<DBObjectType, Integer>(Integer.class);
		for(DBObjectType type: DBObjectType.values()) {
			List<Diff> diffsoftype = getDiffsByDBObjectType(diffs, type);
			StringBuffer sb = new StringBuffer();
			boolean changed = false;
			sb.append(String.format("changes [%-12s]: ", type));
			//sb.append("changes ["+type+"]: ");
			for(ChangeType ct: ChangeType.values()) {
				int size = getDiffOfChangeType(ct, diffsoftype).size();
				if(size>0) {
					sb.append(" "+ct+"("+getDiffOfChangeType(ct, diffsoftype).size()+")");
					changed = true;
				}
			}
			if(changed) {
				log.info(sb.toString());
			}
		}
	}

	public static Collection<? extends Diff> getDiffOfChangeType(ChangeType changeType, Collection<? extends Diff> list) {
		Collection<Diff> ret = new ArrayList<Diff>();
		for(Diff d: list) {
			if(changeType.equals(d.getChangeType())) { ret.add(d); }
		}
		return ret;
	}
	
	static List<Diff> getDiffsByDBObjectType(Collection<? extends Diff> diffs, DBObjectType dbtype) {
		List<Diff> retdiff = new ArrayList<Diff>();
		for(Diff d: diffs) {
			if(dbtype.equals(d.getObjectType())) {
				retdiff.add(d);
			}
		}
		return retdiff;
	}
	
	static Set<FK> getFKsFromTable(Set<FK> fks, String table) {
		Set<FK> retfks = new HashSet<FK>();
		for(FK fk: fks) {
			if(fk.fkTable.equals(table)) { retfks.add(fk); }
		}
		return retfks;
	}
	
	List<Diff> getDiffList() {
		List<Diff> diffs = new ArrayList<Diff>();
		diffs.addAll(tableDiffs);
		diffs.addAll(columnDiffs);
		diffs.addAll(dbidDiffs);
		
		Collections.sort(diffs, new DiffComparator());
		return diffs;
	}
	
	@Override
	public String getDiff() {
		StringBuffer sb = new StringBuffer();
		
		List<Diff> diffs = getDiffList();

		for(Diff d: diffs) {
			sb.append(d.getDiff()+";\n\n");
		}

		/*
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
		*/
		
		return sb.toString();
	}

	public String getDiffByDBObjectTypes(List<DBObjectType> types) {
		StringBuffer sb = new StringBuffer();
		
		List<Diff> diffs = getDiffList();

		for(Diff d: diffs) {
			if(types.contains(d.getObjectType())) {
				sb.append(d.getDiff()+";\n\n");
			}
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

	@Override
	public DBObjectType getObjectType() {
		return null; //XXX: SchemaDiff.DBObjectType?
	}

}
