package tbrugz.sqldiff.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.compare.ExecOrderDiffComparator;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.util.CategorizedOut;

//XXX: should SchemaDiff implement Diff?
//XXX: what about renames?
public class SchemaDiff implements Diff {
	static Log log = LogFactory.getLog(SchemaDiff.class);

	//XXX: should be List<>?
	final Set<TableDiff> tableDiffs = new TreeSet<TableDiff>();
	final Set<TableColumnDiff> columnDiffs = new TreeSet<TableColumnDiff>();
	final Set<DBIdentifiableDiff> dbidDiffs = new TreeSet<DBIdentifiableDiff>();

	public static DBObject findDBObjectBySchemaAndName(Collection<? extends DBObject> col, String schemaName, String name) {
		for(DBObject obj: col) {
			if(schemaName.equalsIgnoreCase(obj.getSchemaName()) && name.equalsIgnoreCase(obj.getName())) return obj;
		}
		return null;
	}

	//XXX rename to getDumpeableByXXX?
	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(DBIdentifiable.getType4Diff(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equalsIgnoreCase(schemaName):true) 
					&& d.getName().equalsIgnoreCase(name)
					&& d.isDumpable())
				return (T) d;
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
			if(! tOrig.getType().equals(TableType.TABLE)) continue;
			
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
						tOrig.getFinalQualifiedName(),
						tNew.getFinalQualifiedName());
				
				for(Diff dt: diffs) {
					boolean added = false;
					if(dt instanceof TableDiff) {
						added = diff.tableDiffs.add((TableDiff)dt);
					}
					else if(dt instanceof TableColumnDiff) {
						added = diff.columnDiffs.add((TableColumnDiff)dt);
					}
					else if(dt instanceof DBIdentifiableDiff) {
						added = diff.dbidDiffs.add((DBIdentifiableDiff)dt);
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
			diff.tableDiffs.add(td);
		}
	}	
		
	public static SchemaDiff diff(SchemaModel modelOrig, SchemaModel modelNew) {
		if(modelOrig == null || modelNew == null) {
			log.warn("source, target, or both models are null");
			return null;
		}
		
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
	
	@SuppressWarnings("unchecked")
	static void logInfo(SchemaDiff diff) {
		int maxNameSize = getMaxDBObjectNameSize(diff.tableDiffs, diff.columnDiffs, diff.dbidDiffs);
		logInfoByObjectAndChangeType(diff.tableDiffs, maxNameSize);
		logInfoByObjectAndChangeType(diff.columnDiffs, maxNameSize);
		logInfoByObjectAndChangeType(diff.dbidDiffs, maxNameSize);
	}

	static void logInfoByObjectAndChangeType(Collection<? extends Diff> diffs, int labelSize) {
		//Map<DBObjectType, Integer> map = new NeverNullGetMap<DBObjectType, Integer>(Integer.class);
		//String format = "changes [%-12s]: ";
		String formatStr = "changes [%-"+labelSize+"s]: ";
		if(labelSize<=0) {
			formatStr = "changes [%s]: ";
		}
		for(DBObjectType type: DBObjectType.values()) {
			List<Diff> diffsoftype = getDiffsByDBObjectType(diffs, type);
			StringBuffer sb = new StringBuffer();
			boolean changed = false;
			sb.append(String.format(formatStr, type));
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
		
		if(log.isDebugEnabled()) {
			for(Diff d: diffs) {
				log.debug("diff: obj = "+d.getNamedObject().getSchemaName()+"."+d.getNamedObject().getName()+" ; type = "+d.getObjectType());
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
	
	static int getMaxDBObjectNameSize(Collection<? extends Diff>... diffs) {
		int max = 0;
		for(Collection<? extends Diff> c: diffs) {
			for(Diff d: c) {
				int size = d.getObjectType().name().length();
				if(size>max) { max = size; }
			}
		}
		return max;
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
			if(fk.getFkTable().equals(table)) { retfks.add(fk); }
		}
		return retfks;
	}
	
	//XXX: rename to getChildren()?
	//@Override
	public List<Diff> getChildren() {
		List<Diff> diffs = new ArrayList<Diff>();
		diffs.addAll(tableDiffs);
		diffs.addAll(columnDiffs);
		diffs.addAll(dbidDiffs);
		
		//XXX: option to select diff comparator?
		Collections.sort(diffs, new ExecOrderDiffComparator());
		return diffs;
	}
	
	public void outDiffs(CategorizedOut out) throws IOException {
		List<Diff> diffs = getChildren();

		log.info("output diffs...");
		int count = 0;
		for(Diff d: diffs) {
			String schemaName = d.getNamedObject()!=null?d.getNamedObject().getSchemaName():"";
			log.debug("diff: "+d.getChangeType()+" ; "+DBIdentifiable.getType4Diff(d.getObjectType()).name()
					+" ; "+schemaName+"; "+d.getNamedObject().getName());
			//XXX: if diff is ADD+EXECUTABLE, not to include ';'?
			out.categorizedOut(d.getDiff()+";\n", schemaName, DBIdentifiable.getType4Diff(d.getObjectType()).name() );
			count++;
		}
		log.info(count+" diffs dumped");
	} 

	@Deprecated
	void outDiffsZ(CategorizedOut out) throws IOException {
		//table
		log.info("output table diffs...");
		for(TableDiff td: tableDiffs) {
			out.categorizedOut(td.getDiff()+";\n", td.table.getSchemaName(), DBObjectType.TABLE.toString());
		}

		//column
		log.info("output column diffs...");
		for(TableColumnDiff tcd: columnDiffs) {
			out.categorizedOut(tcd.getDiff()+";\n", tcd.table.getSchemaName(), DBObjectType.COLUMN.toString());
		}

		//other
		//TODO: executables: do not dump extra ";"
		log.info("output other diffs...");
		for(DBIdentifiableDiff dbid: dbidDiffs) {
			DBIdentifiable dbident = dbid.ident();
			switch(DBIdentifiable.getType(dbident)) {
			case EXECUTABLE:
			case TRIGGER:
				out.categorizedOut(dbid.getDiff()+"\n", dbident.getSchemaName(), DBIdentifiable.getType4Diff(dbident).toString());
				break;
			default:
				out.categorizedOut(dbid.getDiff()+";\n", dbident.getSchemaName(), DBIdentifiable.getType4Diff(dbident).toString());
			}
		}
	} 
	
	@Override
	public String getDiff() {
		StringBuffer sb = new StringBuffer();
		
		List<Diff> diffs = getChildren();

		for(Diff d: diffs) {
			//XXX: if diff is ADD+EXECUTABLE, not to include ';'?
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

	/*public String getDiffByDBObjectTypes(List<DBObjectType> types) {
		StringBuffer sb = new StringBuffer();
		
		List<Diff> diffs = getDiffList();

		for(Diff d: diffs) {
			if(types.contains(d.getObjectType())) {
				sb.append(d.getDiff()+";\n\n");
			}
		}

		return sb.toString();
	}*/
	
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
	
	@Override
	public NamedDBObject getNamedObject() {
		return null; //XXX: SchemaDiff.getNamedObject
	}
	
	@Override
	public SchemaDiff inverse() {
		//List<Diff<?>> dlist = getChildren();
		SchemaDiff inv = new SchemaDiff();
		//List<Diff<?>> invlist = new ArrayList<Diff<?>>();
		for(TableDiff d: tableDiffs) {
			inv.tableDiffs.add(d.inverse());
		}
		for(TableColumnDiff d: columnDiffs) {
			inv.columnDiffs.add(d.inverse());
		}
		for(DBIdentifiableDiff d: dbidDiffs) {
			inv.dbidDiffs.add(d.inverse());
		}
		
		return inv;
	}

}
