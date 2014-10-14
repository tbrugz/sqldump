package tbrugz.sqldiff;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.GrantDiff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;

public class SchemaDiffer {
	static final Log log = LogFactory.getLog(SchemaDiffer.class);

	static boolean mayReplaceDbId = true;
	
	Set<DBObjectType> doDiffTypes = null;

	void diffTables(SchemaModel modelOrig, SchemaModel modelNew, SchemaDiff diff) {
		//tables
		Set<Table> newTablesThatExistsInOrigModel = new HashSet<Table>();
		if(modelOrig.getTables().size()>0) {
			
		for(Table tOrig: modelOrig.getTables()) {
			if(! tOrig.getType().equals(TableType.TABLE)) continue;
			
			Table tNew = DBIdentifiable.getDBIdentifiableBySchemaAndName(modelNew.getTables(), tOrig.getSchemaName(), tOrig.getName());
			if(tNew==null) {
				//if new table doesn't exist, drop old
				TableDiff td = new TableDiff(ChangeType.DROP, tOrig);
				//td.table = tOrig;
				//td.diffType = ChangeType.DROP;
				diff.getTableDiffs().add(td);
			}
			else {
				//new and old tables exists
				newTablesThatExistsInOrigModel.add(tNew);
				//rename XXX: what about rename? external info needed?
				List<Diff> diffs = TableDiff.tableDiffs(tOrig, tNew);
				
				//FKs
				diffs(DBObjectType.FK, diff.getDbIdDiffs(), 
						getFKsFromTable(modelOrig.getForeignKeys(), tOrig.getName()), 
						getFKsFromTable(modelNew.getForeignKeys(), tNew.getName()), 
						tOrig.getFinalQualifiedName(),
						tNew.getFinalQualifiedName());
				
				for(Diff dt: diffs) {
					boolean added = false;
					if(dt instanceof TableDiff) {
						added = diff.getTableDiffs().add((TableDiff)dt);
					}
					else if(dt instanceof ColumnDiff) {
						added = diff.getColumnDiffs().add((ColumnDiff)dt);
					}
					else if(dt instanceof GrantDiff) {
						added = diff.getGrantDiffs().add((GrantDiff)dt);
					}
					else if(dt instanceof DBIdentifiableDiff) {
						added = diff.getDbIdDiffs().add((DBIdentifiableDiff)dt);
					}
					else {
						added = true;
						log.warn("unknown diff: "+dt);
					}
					
					if(!added) {
						log.warn("diff already present in set: "+dt);
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
			if(! t.getType().equals(TableType.TABLE)) continue;

			TableDiff td = new TableDiff(ChangeType.ADD, t);
			diff.getTableDiffs().add(td);
		}
	}	
	
	public SchemaDiff diffSchemas(SchemaModel modelOrig, SchemaModel modelNew) {
		if(modelOrig == null || modelNew == null) {
			log.warn("source, target, or both models are null");
			return null;
		}
		
		SchemaDiff diff = new SchemaDiff();
		
		if(doDiffTypes!=null) {
			log.info("diffing types: "+doDiffTypes);
		}
		
		//Tables
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.TABLE)) {
			//TODO: diff or not COLUMN, GRANT & CONSTRAINT types
			diffTables(modelOrig, modelNew, diff);
		}
		
		//Views
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.VIEW)) {
			diffs(DBObjectType.VIEW, diff.getDbIdDiffs(), modelOrig.getViews(), modelNew.getViews());
		}
		
		//Triggers
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.TRIGGER)) {
			diffs(DBObjectType.TRIGGER, diff.getDbIdDiffs(), modelOrig.getTriggers(), modelNew.getTriggers());
		}

		//FIXedME: package and package body: findByName must also use object type! (and schemaName!)
		//Executables
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.EXECUTABLE)) {
			diffs(DBObjectType.EXECUTABLE, diff.getDbIdDiffs(), modelOrig.getExecutables(), modelNew.getExecutables());
		}

		//Synonyms
		//FIXedME: doesn't detect schemaName changes
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.SYNONYM)) {
			diffs(DBObjectType.SYNONYM, diff.getDbIdDiffs(), modelOrig.getSynonyms(), modelNew.getSynonyms());
		}
		
		//Indexes
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.INDEX)) {
			diffs(DBObjectType.INDEX, diff.getDbIdDiffs(), modelOrig.getIndexes(), modelNew.getIndexes());
		}

		//Sequences
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.SEQUENCE)) {
			diffs(DBObjectType.SEQUENCE, diff.getDbIdDiffs(), modelOrig.getSequences(), modelNew.getSequences());
		}
		
		//XXX: query tableDiffs and columnDiffs: set schema.type: ADD, ALTER, DROP 
		SchemaDiff.logInfo(diff);
		
		return diff;
	}
	
	public void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew) {
		diffs(objType, diffs, listOrig, listNew, null, null);
	}

	public void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origOwnerTableName, String newOwnerTableName) {
		Set<DBIdentifiable> newDBObjectsThatExistsInOrigModel = new HashSet<DBIdentifiable>();
		for(DBIdentifiable cOrig: listOrig) {
			DBIdentifiable cNew = DiffUtil.getDBIdentifiableByTypeSchemaAndName(listNew, DBIdentifiable.getType4Diff(cOrig), cOrig.getSchemaName(), cOrig.getName());
			if(cNew!=null) {
				newDBObjectsThatExistsInOrigModel.add(cNew);
				if(!cOrig.equals4Diff(cNew)) {
					if(!cOrig.isDumpable()) {
						log.debug("original/new object not dumpable: "+cOrig);
						continue;
					}
					
					if(mayReplaceDbId 
							&& ( (origOwnerTableName==null && newOwnerTableName==null)
							|| (origOwnerTableName!=null && origOwnerTableName.equals(newOwnerTableName)) ) ) {
						log.debug("replace "+objType+": orig: "+cOrig+" new: "+cNew);
						diffs.add(new DBIdentifiableDiff(ChangeType.REPLACE, cOrig, cNew, origOwnerTableName));
					}
					else {
						log.debug("drop/add "+objType+": orig: "+cOrig+" new: "+cNew);
						diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, cNew, origOwnerTableName));
						diffs.add(new DBIdentifiableDiff(ChangeType.ADD, cOrig, cNew, newOwnerTableName));
					}
				}
			}
			else {
				if(!cOrig.isDumpable()) {
					log.debug("original object not dumpable: "+cOrig);
					continue;
				}
				log.debug("drop "+objType+": orig: "+cOrig);
				diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, null, origOwnerTableName));
			}
		}
		for(DBIdentifiable cNew: listNew) {
			if(newDBObjectsThatExistsInOrigModel.contains(cNew)) { continue; }
			if(!cNew.isDumpable()) {
				log.debug("new object not dumpable: "+cNew);
				continue;
			}
			log.debug("add "+objType+": new: "+cNew);
			diffs.add(new DBIdentifiableDiff(ChangeType.ADD, null, cNew, newOwnerTableName));
		}
	}
	
	static Set<FK> getFKsFromTable(Set<FK> fks, String table) {
		Set<FK> retfks = new HashSet<FK>();
		for(FK fk: fks) {
			if(fk.getFkTable().equals(table)) { retfks.add(fk); }
		}
		return retfks;
	}
	
	public void setTypesForDiff(String types) {
		if(types==null) { return; }
		doDiffTypes = new TreeSet<DBObjectType>();
		String[] typesArr = types.split(",");
		for(String s: typesArr) {
			if(s==null) { continue; }
			s = s.trim();
			try {
				DBObjectType t = DBObjectType.valueOf(s);
				doDiffTypes.add(t);
			}
			catch(IllegalArgumentException e) {
				log.warn("unknown object type: "+s);
			}
		}
	}

}
