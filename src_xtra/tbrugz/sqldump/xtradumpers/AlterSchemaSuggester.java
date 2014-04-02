package tbrugz.sqldump.xtradumpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.Utils;

/*
 * currently suggests:
 * - index creations based on FKs
 * - FKs creation based on column names and existing PKs/UKs
 * 
 * TODOne: FK creations based on column names and existing PKs/UKs
 * x option to suggest only simple (not composite) FKs
 * - XXX: test if FK is possible (all values in fkTable exists in pkTable) - needs connection, can be very time consuming
 * - XXX: test column types
 * 
 * XXX: suggest PKs/UKs for tables [that doesn't have one?] - needs connection, can be time-consuming
 * -> if count(*) == select count(*) from (select distinct colX) - good candidate
 * - do not suggest if table has 0 or 1 rows 
 * - [optional] do not suggest if table has less than X rows
 * -- 1st: test for each column; 
 * -- 2nd: test for each 2-col combination that doesn't include known PK/UK
 * -- 3rd+: test for each [2+]-col combination ...
 * 
 * XXXdone: output warn for tables that doesn't have PK
 * 
 * XXX: option to ignore tables by regex pattern
 * 
 * TODOne: use tbrugz.sqldump.util.CategorizedOut
 * 
 * XXX: rename to SchemaHints?
 */
public class AlterSchemaSuggester extends AbstractFailable implements SchemaModelDumper {
	
	public static class Warning implements Comparable<Warning> {
		DBObjectType type;
		String warning;
		String schemaName; 
		
		public Warning(DBObjectType type, String warning, String schemaName) {
			this.type = type;
			this.warning = warning;
			this.schemaName = schemaName;
		}

		@Override
		public int compareTo(Warning o) {
			int comp = type.compareTo(o.type);
			if(comp!=0) { return comp; }
			
			comp = schemaName.compareTo(o.schemaName);
			if(comp!=0) { return comp; }
			
			return warning.compareTo(o.warning);
		}
	}

	static final String PROP_PREFIX = "sqldump.alterschemasuggester";
	
	static final String FILENAME_PATTERN_SCHEMA = "[schemaname]";
	static final String FILENAME_PATTERN_OBJECTTYPE = "[objecttype]";
	
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS = PROP_PREFIX+".alterobjectsfromschemas";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_SIMPLEFKSONLY = PROP_PREFIX+".simplefksonly";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_FKINDEXESSONLY = PROP_PREFIX+".fkindexessonly";
	
	static Log log = LogFactory.getLog(AlterSchemaSuggester.class);
	
	String fileOutput;
	List<String> schemasToAlter;
	boolean dumpSimpleFKsOnly = false;
	boolean dumpFKIndexesOnly = true;
	CategorizedOut cout;

	@Override
	public void setProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_ALTER_SCHEMA_SUGGESTER_OUTFILEPATTERN);
		String schemas = prop.getProperty(PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS);
		if(schemas!=null) {
			schemasToAlter = new ArrayList<String>();
			String[] schemasArr = schemas.split(",");
			for(String sch: schemasArr) {
				schemasToAlter.add(sch.trim());
			}
		}
		dumpSimpleFKsOnly = Utils.getPropBool(prop, PROP_ALTER_SCHEMA_SUGGESTER_SIMPLEFKSONLY, dumpSimpleFKsOnly);
		dumpFKIndexesOnly = Utils.getPropBool(prop, PROP_ALTER_SCHEMA_SUGGESTER_FKINDEXESSONLY, dumpFKIndexesOnly);
	}

	/*
	 * IDX name suggested:
	 * - table_name + UKI/FKI                        //may gererate non-unique names
	 * - table_name + UKI/FKI + index counter        //non-idempotent (index order changes when index is added or removed)
	 * - table_name + UKI/FKI + hash(col_names)      //hashCode()? weird - maybe another hash?
	 * - table_name + col_names + UKI/FKI            //too long
	 * - table_name + initials(col_names) + UKI/FKI  //seems better
	 * - 2initials(table) + initials(col_names) + UKI/FKI  //...
	 */
	//FIXME: simpleOut() has no reference to schemaName - see CategorizedOut
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(schemaModel==null) {
			log.error("schemaModel null, nothing to dump");
			if(failonerror) { throw new ProcessingException("schemaModel null, nothing to dump"); }
			return;
		}
		
		try {

		if(fileOutput==null) {
			log.error("prop '"+PROP_ALTER_SCHEMA_SUGGESTER_OUTFILEPATTERN+"' not defined");
			if(failonerror) { throw new ProcessingException("prop '"+PROP_ALTER_SCHEMA_SUGGESTER_OUTFILEPATTERN+"' not defined"); }
			return;
		}
		
		String finalPattern = CategorizedOut.generateFinalOutPattern(fileOutput,
				FILENAME_PATTERN_SCHEMA,
				FILENAME_PATTERN_OBJECTTYPE);
		cout = new CategorizedOut(finalPattern);
		
		//FileWriter fos = new FileWriter(fileOutput, false);
		
		//XXX: do not dump suggested indexes if indexes hasn't been grabbed
		Set<Index> addIndexes = dumpCreateIndexes(schemaModel, schemaModel.getForeignKeys());
		if(addIndexes.size()>0) {
			simpleOut("-- indexes for existing FKs", cout, null, DBObjectType.INDEX);
			int count = writeIndexes(addIndexes, cout);
			log.info("dumped "+count+" 'create index' statements");
		}
		
		Set<FK> alterFKs = dumpCreateFKs(schemaModel);
		if(alterFKs.size()>0) {
			simpleOut("-- FKs", cout, null, DBObjectType.FK);
			int count = writeFKs(alterFKs, cout);
			log.info("dumped "+count+" 'add foreign key' statements");
		}
		
		Set<Index> addIndexesFromFKs = dumpCreateIndexes(schemaModel, alterFKs);
		if(addIndexesFromFKs.size()>0) {
			simpleOut("-- indexes for generated FKs", cout, null, DBObjectType.INDEX);
			int count = writeIndexes(addIndexesFromFKs, cout);
			log.info("dumped "+count+" 'create index' statements for generated FKs");
		}
		
		Set<Warning> noPKWarnings = dumpTablesWithNoPK(schemaModel);
		if(noPKWarnings.size()>0) {
			simpleOut("-- tables with no PK:", cout, null, DBObjectType.CONSTRAINT);
			int count = writeWarnings(noPKWarnings, cout);
			log.info("dumped "+count+" warnings for tables with no PKs");
		}
		
		//fos.close();
		
		} catch (IOException e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
		
	Set<Index> dumpCreateIndexes(SchemaModel schemaModel, Set<FK> foreignKeys) {
		
		Set<Index> indexes = new TreeSet<Index>(); //HashSet doesn't work (uses hashCode()?)
		
		//FKs
		//int fkIndexCounter = 0;
		for(FK fk: foreignKeys) {
			boolean pkTableHasIndex = false;
			boolean fkTableHasIndex = false;
			
			//Table
			//Table pkTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, fk.pkTableSchemaName, fk.pkTable);
			//Table fkTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, fk.fkTableSchemaName, fk.fkTable);
			
			for(Table t: schemaModel.getTables()) {
				boolean pkTable = false;
				boolean fkTable = false;
				if(t.getName().equals(fk.getPkTable())) { pkTable = true; }
				if(t.getName().equals(fk.getFkTable())) { fkTable = true; }
				if(! (pkTable || fkTable)) { continue; }
				
				//Index
				for(Index idx: schemaModel.getIndexes()) {
					if(! (idx.tableName.equals(fk.getPkTable()) || idx.tableName.equals(fk.getFkTable()))) { continue; }

					Set<String> cols = new HashSet<String>();
					cols.addAll(idx.columns);
					if(idx.tableName.equals(fk.getPkTable())) {
						if(stringCollectionEquals(cols,fk.getPkColumns())) { pkTableHasIndex = true; }
						//if(cols.equals(fk.pkColumns)) { pkTableHasIndex = true; }
						//log.info("cols["+idx.tableName+"/pkt]: "+cols+" fk.pkCols: "+fk.pkColumns+"; hasI: "+pkTableHasIndex+"/"+stringCollectionEquals(cols, fk.pkColumns));
					}
					if(idx.tableName.equals(fk.getFkTable())) { 
						if(stringCollectionEquals(cols,fk.getFkColumns())) { fkTableHasIndex = true; }
						//if(cols.equals(fk.fkColumns)) { fkTableHasIndex = true; }
						//log.info("cols["+idx.tableName+"/fkt]: "+cols+" fk.fkCols: "+fk.fkColumns+"; hasI: "+fkTableHasIndex+"/"+stringCollectionEquals(cols, fk.fkColumns));
					}
				}
			}
			
			//if(pkTableHasIndex && fkTableHasIndex) { continue; }
			
			if(!pkTableHasIndex && !dumpFKIndexesOnly) {
				Index idx = new Index();
				idx.tableName = fk.getPkTable();
				idx.setSchemaName(fk.getPkTableSchemaName());
				idx.unique = false;
				idx.columns.addAll(fk.getPkColumns());
				idx.setName(fk.getPkTable() + "_" + suggestAcronym(idx.columns) + "_UKI"); //_" + (indexes.size()+1);
				addIndex(indexes, idx);
			}

			if(!fkTableHasIndex) {
				Index idx = new Index();
				idx.tableName = fk.getFkTable();
				idx.setSchemaName(fk.getFkTableSchemaName());
				idx.unique = false;
				idx.columns.addAll(fk.getFkColumns());
				idx.setName(fk.getFkTable() + "_" + suggestAcronym(idx.columns) + "_FKI");
				//idx.name = fk.fkTable + "_FKI";
				//idx.name = fk.fkTable + "_FKI_" + idx.hashCode();
				//idx.name = fk.fkTable + "_FKI_" + (++fkIndexCounter);
				addIndex(indexes, idx);
			}
		}
		
		//int dumpCounter = 0;
		if(indexes.size()>0) {
			log.debug(indexes.size()+" 'create index' generated");
		}
		else {
			log.info("no 'create index' alter schema suggestions");
		}
		
		return indexes;
	}
	
	void addIndex(Set<Index> indexes, Index idx) {
		//TreeSet.contains() doesn't work if Comparator<> isn't properly implemented
		if(indexes.contains(idx)) {
		//if(containsBasedOnEquals(indexes, idx)) {
			log.debug("duplicate index "+idx+" ignored");
			/*for(Index i: indexes) {
				if(i.equals(idx)) {
					log.debug("dup idx is: "+i);
				}
				if(i.hashCode()==idx.hashCode()) {
					log.debug("dup idx is: "+i+"; hash is "+idx.hashCode());
				}
			}*/
		}
		else {
			indexes.add(idx);
		}
	}

	Set<FK> dumpCreateFKs(SchemaModel schemaModel) {
		Set<FK> fks = new TreeSet<FK>();
		
		//Tables
		for(Table table: schemaModel.getTables()) {
			//Unique constraints
			for(Constraint cons: table.getConstraints()) {
				if(! (cons.getType()==ConstraintType.PK || cons.getType()==ConstraintType.UNIQUE)) { continue; }
				if(dumpSimpleFKsOnly && (cons.getUniqueColumns().size()>1) ) { continue; }
				
				//Tables
				for(Table otherT: schemaModel.getTables()) {
					if(table.getName().equals(otherT.getName())) { continue; }
					//if(otherT.getDomainTable() && !table.getDomainTable()) { continue; }
					if(otherT.isTableADomainTable() && !table.isTableADomainTable()) { continue; } //can't have FK from non-domain table to domain table
					
					List<String> otherTCols = new ArrayList<String>();
					for(Column c: otherT.getColumns()) {
						otherTCols.add(c.getName());
					}
					//log.debug("pk.cols: "+cons.uniqueColumns+" ; other.cols: "+otherTCols);
					//log.debug("pk.cols: "+cons.uniqueColumns+" ; other.cols: "+otherT.getColumns());
					if( otherTCols.containsAll(cons.getUniqueColumns()) ) {
						
						//Create FK
						FK fk = new FK();
						fk.setPkTable( table.getName() );
						fk.setPkTableSchemaName( table.getSchemaName() );
						fk.getPkColumns().addAll(cons.getUniqueColumns());
						fk.setFkTable( otherT.getName() );
						fk.setFkTableSchemaName( otherT.getSchemaName() );
						fk.getFkColumns().addAll(cons.getUniqueColumns());
						fk.setFkReferencesPK((cons.getType()==ConstraintType.PK));
						fk.setName(suggestAcronym(fk.getFkTable()) + "_" + suggestAcronym(fk.getPkTable()) + "_FK");

						//Test if FK already exists
						boolean fkAlreadyExists = false;
						for(FK fktest: schemaModel.getForeignKeys()) {
							if(fktest.equals(fk)) { fkAlreadyExists = true; break; }
						}
						if(fkAlreadyExists) { continue; }
						
						fks.add(fk);
					} 
				}
			}
		}
		
		//int dumpCounter = 0;
		if(fks.size()>0) {
			log.debug(fks.size()+" 'add foreign key's generated");
		}
		else {
			log.info("no 'add foreign key' alter schema suggestions");
		}
		
		return fks;
	}

	Set<Warning> dumpTablesWithNoPK(SchemaModel schemaModel) {
		Set<Warning> warnings = new TreeSet<Warning>();
		
		for(Table t: schemaModel.getTables()) {
			Constraint c = t.getPKConstraint();
			if(c==null) {
				warnings.add(new Warning(DBObjectType.CONSTRAINT, "-- table "+t.getSchemaName()+"."+t.getName()+" has no primary key", t.getSchemaName()));
			}
		}
		
		return warnings;
	}
	
	/*static boolean containsBasedOnEquals(Collection col, Object o) {
		for(Object co: col) {
			if(co==null) {
				if(o==null) { return true; }
				else { return false; }
			}
			else if(co.equals(o)) {
				return true;
			}
		}
		return false;
	}*/
	
	int writeIndexes(Set<Index> indexes, CategorizedOut fos) throws IOException {
		int dumpCounter = 0;
		for(Index idx: indexes) {
			if(schemasToAlter==null || schemasToAlter.contains(idx.getSchemaName()) ) {
				fos.categorizedOut( idx.getDefinition(true)+";\n", idx.getSchemaName(), DBObjectType.INDEX.toString());
				dumpCounter++;
			}
		}
		return dumpCounter;
	}
	
	int writeFKs(Set<FK> fks, CategorizedOut fos) throws IOException {
		int dumpCounter = 0;
		for(FK fk: fks) {
			if(schemasToAlter==null || schemasToAlter.contains(fk.getFkTableSchemaName()) ) {
				fos.categorizedOut(fk.fkScriptWithAlterTable(false, true), fk.getSchemaName(), DBObjectType.FK.toString() );
				//fos.write( fk.getDefinition(true)+";\n\n" );
				dumpCounter++;
			}
		}
		return dumpCounter;
	}

	int writeWarnings(Set<Warning> warnings, CategorizedOut fos) throws IOException {
		int dumpCounter = 0;
		for(Warning w: warnings) {
			if(schemasToAlter==null || schemasToAlter.contains(w.schemaName) ) {
				fos.categorizedOut( w.warning, w.schemaName, DBObjectType.CONSTRAINT.toString());
				dumpCounter++;
			}
		}
		return dumpCounter;
	}
	
	static String suggestAcronym(Collection<String> strings) {
		StringBuffer sb = new StringBuffer();
		for(String col: strings) {
			String[] strs = col.split("_");
			for(String s: strs) {
				sb.append(s.substring(0, 1));
			}
		}
		return sb.toString();
	}

	static String suggestAcronym(String string) {
		StringBuffer sb = new StringBuffer();
		String[] strs = string.split("_");
		for(String s: strs) {
			sb.append(s.length()>0?s.substring(0, 1):"_");
		}
		return sb.toString();
	}
	
	static void simpleOut(String s, CategorizedOut fos, String schemaName, DBObjectType ot) throws IOException {
		fos.categorizedOut( s + "\n", schemaName, ot.toString() );
	}
	
	static boolean stringCollectionEquals(Collection<String> s1, Collection<String> s2) {
		if(s1==null && s2==null) { return true; }
		if(s1==null || s2==null) { return false; }
		
		if(s1.containsAll(s2) && s2.containsAll(s1)) { return true; }
		return false;
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
}
