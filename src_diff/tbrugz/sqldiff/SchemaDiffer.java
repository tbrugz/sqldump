package tbrugz.sqldiff;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
	
	static final TableType[] diffableTableTypesArray = {
		TableType.BASE_TABLE, TableType.EXTERNAL_TABLE, TableType.FOREIGN_TABLE,
		TableType.SYSTEM_TABLE, TableType.TABLE
		//, TableType.TYPE ?
	};
	static final List<TableType> diffableTableTypes;
	static {
		diffableTableTypes = Arrays.asList(diffableTableTypesArray);
	}

	void diffTables(SchemaModel modelOrig, SchemaModel modelNew, SchemaDiff diff) {
		//tables
		Set<Table> newTablesThatExistsInOrigModel = new HashSet<Table>();
		Map<TableType, Integer> nonDiffableTableTypesPresent = new TreeMap<TableType, Integer>();
		
		if(modelOrig.getTables().size()>0) {
		
		for(Table tOrig: modelOrig.getTables()) {
			if( !diffableTableTypes.contains(tOrig.getType()) ) {
				Integer count = nonDiffableTableTypesPresent.get(tOrig.getType());
				if(count==null) { count = 1; } else { count++; }
				nonDiffableTableTypesPresent.put(tOrig.getType(), count);
				continue;
			}
			
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
			if( !diffableTableTypes.contains(t.getType()) ) {
				Integer count = nonDiffableTableTypesPresent.get(t.getType());
				if(count==null) { count = 1; } else { count++; }
				nonDiffableTableTypesPresent.put(t.getType(), count);
				continue;
			}

			TableDiff td = new TableDiff(ChangeType.ADD, t);
			diff.getTableDiffs().add(td);
		}
		
		if(nonDiffableTableTypesPresent.size()>0) {
			log.warn("non-diffable tables by type: "+nonDiffableTableTypesPresent);
		}
	}	
	
	static final DBObjectType[] diffableTypes = {
		DBObjectType.TABLE, DBObjectType.VIEW, DBObjectType.MATERIALIZED_VIEW, DBObjectType.TRIGGER, DBObjectType.EXECUTABLE,
		DBObjectType.SYNONYM, DBObjectType.INDEX, DBObjectType.SEQUENCE,
		DBObjectType.FUNCTION, DBObjectType.PROCEDURE,
		DBObjectType.PACKAGE, DBObjectType.PACKAGE_BODY, DBObjectType.TYPE, DBObjectType.TYPE_BODY, DBObjectType.JAVA_SOURCE
	};
	
	static final Set<DBObjectType> diffableTypesSet = new HashSet<DBObjectType>();
	
	static {
		diffableTypesSet.addAll(Arrays.asList(diffableTypes));
	}
	
	public SchemaDiff diffSchemas(SchemaModel modelOrig, SchemaModel modelNew) {
		if(modelOrig == null || modelNew == null) {
			log.warn("source, target, or both models are null");
			return null;
		}
		
		SchemaDiff diff = new SchemaDiff();
		
		if(doDiffTypes!=null) {
			log.info("diffing types: "+doDiffTypes);
			Set<DBObjectType> setDiff = new HashSet<DBObjectType>();
			setDiff.addAll(doDiffTypes);
			setDiff.removeAll(diffableTypesSet);
			if(setDiff.size()>0) {
				log.warn("undiffable types: "+setDiff);
			}
		}
		
		String dialect = modelOrig.getSqlDialect();
		SQLDiff.setupFeaturesIfNull(dialect);
		
		//Tables
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.TABLE)) {
			//TODO: diff or not COLUMN, GRANT & CONSTRAINT types
			diffTables(modelOrig, modelNew, diff);
		}
		
		//Views
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.VIEW)) {
			diffs(DBObjectType.VIEW, diff.getDbIdDiffs(), modelOrig.getViews(), modelNew.getViews());
		}

		//Materialized Views
		if(doDiffTypes==null || doDiffTypes.contains(DBObjectType.MATERIALIZED_VIEW)) {
			diffs(DBObjectType.MATERIALIZED_VIEW, diff.getDbIdDiffs(), modelOrig.getViews(), modelNew.getViews(), null, null, true);
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
		else {
			//log.debug("countsByType-orig::\n"+ModelUtils.getExecutableCountsByType(modelOrig.getExecutables()));
			//log.debug("countsByType-new::\n"+ModelUtils.getExecutableCountsByType(modelNew.getExecutables()));
			DBObjectType[] types = { DBObjectType.FUNCTION, DBObjectType.PROCEDURE, DBObjectType.PACKAGE, DBObjectType.PACKAGE_BODY, DBObjectType.TYPE };
			//DBObjectType[] types = DBObjectType.getExecutableTypes();
			//int count = 0;
			for(DBObjectType et: types) {
				//log.debug("diffSchemas: execs["+count+"]: #dbid="+diff.getDbIdDiffs().size()+" #orig="+modelOrig.getExecutables().size()+" #new="+modelNew.getExecutables().size());
				if(doDiffTypes.contains(et)) {
					diffs(et, diff.getDbIdDiffs(), modelOrig.getExecutables(), modelNew.getExecutables(), null, null, true);
				}
				//count++;
			}
			//log.debug("diffSchemas: execs-end["+count+"]: #dbid="+diff.getDbIdDiffs().size()+" #orig="+modelOrig.getExecutables().size()+" #new="+modelNew.getExecutables().size());
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
		
		diff.setSqlDialect(modelNew.getSqlDialect());
		
		//XXX: query tableDiffs and columnDiffs: set schema.type: ADD, ALTER, DROP 
		SchemaDiff.logInfo(diff);
		
		return diff;
	}
	
	void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew) {
		diffs(objType, diffs, listOrig, listNew, null, null);
	}

	public void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origOwnerTableName, String newOwnerTableName) {
		diffs(objType, diffs, listOrig, listNew, origOwnerTableName, newOwnerTableName, false);
	}
	
	void diffs(DBObjectType objType, Collection<DBIdentifiableDiff> diffs, Collection<? extends DBIdentifiable> listOrig, Collection<? extends DBIdentifiable> listNew, String origOwnerTableName, String newOwnerTableName, boolean filterByType) {
		Set<DBIdentifiable> newDBObjectsThatExistsInOrigModel = new HashSet<DBIdentifiable>();
		int countAdd = 0, countReplace = 0, countDrop = 0, countSource = 0, countTarget = 0, countSourceInit=0, countTargetInit=0;
		/*List<DBIdentifiable> lo = new ArrayList<DBIdentifiable>();
		lo.addAll(listOrig);
		List<DBIdentifiable> ln = new ArrayList<DBIdentifiable>();
		ln.addAll(listNew);*/
		
		//log.info("diffs: "+objType+" / "+origOwnerTableName);
		for(DBIdentifiable cOrig: listOrig) {
			//log.info("dbid: "+cOrig);
			//DBIdentifiable cOrig = (DBIdentifiable) lo.get(i);
			countSourceInit++;
			if(filterByType && DBIdentifiable.getType(cOrig)!=objType) { continue; }
			countSource++;
			
			DBIdentifiable cNew = DiffUtil.getDBIdentifiableByTypeSchemaAndName(listNew, DBIdentifiable.getType(cOrig), cOrig.getSchemaName(), cOrig.getName());
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
					countReplace++;
				}
			}
			else {
				if(!cOrig.isDumpable()) {
					log.debug("original object not dumpable: "+cOrig);
					continue;
				}
				log.debug("drop "+objType+": orig: "+cOrig);
				diffs.add(new DBIdentifiableDiff(ChangeType.DROP, cOrig, null, origOwnerTableName));
				countDrop++;
			}
		}
		
		for(DBIdentifiable cNew: listNew) {
			//DBIdentifiable cNew = (DBIdentifiable) ln.get(i);
			countTargetInit++;
			if(filterByType && DBIdentifiable.getType(cNew)!=objType) { continue; }
			countTarget++;

			if(newDBObjectsThatExistsInOrigModel.contains(cNew)) { continue; }
			if(!cNew.isDumpable()) {
				log.debug("new object not dumpable: "+cNew);
				continue;
			}
			log.debug("add "+objType+": new: "+cNew);
			diffs.add(new DBIdentifiableDiff(ChangeType.ADD, null, cNew, newOwnerTableName));
			countAdd++;
		}
		log.debug("diffs["+objType+"]: #add="+countAdd+" #replace="+countReplace+" #drop="+countDrop+
				" #source="+countSource+"/"+countSourceInit+"/"+listOrig.size()+
				" #target="+countTarget+"/"+countTargetInit+"/"+listNew.size()
				);
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
