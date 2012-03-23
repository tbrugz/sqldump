package tbrugz.sqldump.xtradumpers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.SchemaModelDumper;
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
 * XXX: output warn for tables that doesn't have PK
 * 
 * XXX: option to ignore tables by regex pattern
 */
public class AlterSchemaSuggester implements SchemaModelDumper {

	public static final String PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE = "sqldump.alterschemasuggester.outfile";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS = "sqldump.alterschemasuggester.alterobjectsfromschemas";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_SIMPLEFKSONLY = "sqldump.alterschemasuggester.simplefksonly";
	
	static Logger log = Logger.getLogger(AlterSchemaSuggester.class);
	
	String fileOutput;
	List<String> schemasToAlter;
	boolean dumpSimpleFKsOnly = false; //XXXdone: add prop for dumpSimpleFKsOnly

	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE);
		String schemas = prop.getProperty(PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS);
		if(schemas!=null) {
			schemasToAlter = new ArrayList<String>();
			String[] schemasArr = schemas.split(",");
			for(String sch: schemasArr) {
				schemasToAlter.add(sch.trim());
			}
		}
		dumpSimpleFKsOnly = Utils.getPropBool(prop, PROP_ALTER_SCHEMA_SUGGESTER_SIMPLEFKSONLY, dumpSimpleFKsOnly);
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
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(schemaModel==null) {
			log.warn("schemaModel null, nothing to dump");
			return;
		}
		
		try {

		if(fileOutput==null) {
			log.warn("prop '"+PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE+"' not defined");
			return;
		}
		
		FileWriter fos = new FileWriter(fileOutput, false);
		
		//XXX: do not dump suggested indexes if indexes hasn't been grabbed
		Set<Index> addIndexes = dumpCreateIndexes(schemaModel, schemaModel.getForeignKeys());
		if(addIndexes.size()>0) {
			simpleOut("-- indexes", fos);
			int count = writeIndexes(addIndexes, fos);
			log.info("dumped "+count+" 'create index' statements");
		}
		
		Set<FK> alterFKs = dumpCreateFKs(schemaModel);
		if(alterFKs.size()>0) {
			simpleOut("-- FKs", fos);
			int count = writeFKs(alterFKs, fos);
			log.info("dumped "+count+" 'add foreign key' statements");
		}
		
		Set<Index> addIndexesFromFKs = dumpCreateIndexes(schemaModel, alterFKs);
		if(addIndexesFromFKs.size()>0) {
			simpleOut("-- indexes for generated FKs", fos);
			int count = writeIndexes(addIndexesFromFKs, fos);
			log.info("dumped "+count+" 'create index' statements for generated FKs");
		}
		
		fos.close();
		
		} catch (IOException e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
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
				if(t.name.equals(fk.pkTable)) {	pkTable = true;	}
				if(t.name.equals(fk.fkTable)) {	fkTable = true;	}
				if(! (pkTable || fkTable)) { continue; }
				
				//Index
				for(Index idx: schemaModel.getIndexes()) {
					if(! (idx.tableName.equals(fk.pkTable) || idx.tableName.equals(fk.fkTable))) { continue; }

					Set<String> cols = new HashSet<String>();
					cols.addAll(idx.columns);
					if(idx.tableName.equals(fk.pkTable)) {
						if(cols.equals(fk.pkColumns)) { pkTableHasIndex = true; }
					}
					if(idx.tableName.equals(fk.fkTable)) { 
						if(cols.equals(fk.fkColumns)) { fkTableHasIndex = true; }
					}
				}
			}
			
			//if(pkTableHasIndex && fkTableHasIndex) { continue; }
			
			if(!pkTableHasIndex) {
				Index idx = new Index();
				idx.tableName = fk.pkTable;
				idx.setSchemaName(fk.pkTableSchemaName);
				idx.unique = false;
				idx.columns.addAll(fk.pkColumns);
				idx.name = fk.pkTable + "_" + suggestAcronym(idx.columns) + "_UKI"; //_" + (indexes.size()+1);
				addIndex(indexes, idx);
			}

			if(!fkTableHasIndex) {
				Index idx = new Index();
				idx.tableName = fk.fkTable;
				idx.setSchemaName(fk.fkTableSchemaName);
				idx.unique = false;
				idx.columns.addAll(fk.fkColumns);
				idx.name = fk.fkTable + "_" + suggestAcronym(idx.columns) + "_FKI";
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
				if(! (cons.type==ConstraintType.PK || cons.type==ConstraintType.UNIQUE)) { continue; }
				if(dumpSimpleFKsOnly && (cons.uniqueColumns.size()>1) ) { continue; }
				
				//Tables
				for(Table otherT: schemaModel.getTables()) {
					if(table.name.equals(otherT.name)) { continue; }
					//if(otherT.getDomainTable() && !table.getDomainTable()) { continue; }
					if(otherT.isTableADomainTable() && !table.isTableADomainTable()) { continue; } //can't have FK from non-domain table to domain table
					
					List<String> otherTCols = new ArrayList<String>();
					for(Column c: otherT.getColumns()) {
						otherTCols.add(c.name);
					}
					//log.debug("pk.cols: "+cons.uniqueColumns+" ; other.cols: "+otherTCols);
					//log.debug("pk.cols: "+cons.uniqueColumns+" ; other.cols: "+otherT.getColumns());
					if( otherTCols.containsAll(cons.uniqueColumns) ) {
						
						//Create FK
						FK fk = new FK();
						fk.pkTable = table.name;
						fk.pkTableSchemaName = table.getSchemaName();
						fk.pkColumns.addAll(cons.uniqueColumns);
						fk.fkTable = otherT.name;
						fk.fkTableSchemaName = otherT.getSchemaName();
						fk.fkColumns.addAll(cons.uniqueColumns);
						fk.fkReferencesPK = (cons.type==ConstraintType.PK);
						fk.name = suggestAcronym(fk.fkTable) + "_" + suggestAcronym(fk.pkTable) + "_FK";

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
	
	int writeIndexes(Set<Index> indexes, FileWriter fos) throws IOException {
		int dumpCounter = 0;
		for(Index idx: indexes) {
			if(schemasToAlter==null || (schemasToAlter!=null && schemasToAlter.contains(idx.getSchemaName()))) {
				fos.write( idx.getDefinition(true)+";\n\n" );
				dumpCounter++;
			}
		}
		return dumpCounter;
	}
	
	int writeFKs(Set<FK> fks, FileWriter fos) throws IOException {
		int dumpCounter = 0;
		for(FK fk: fks) {
			if(schemasToAlter==null || (schemasToAlter!=null && schemasToAlter.contains(fk.fkTableSchemaName))) {
				fos.write( SchemaModelScriptDumper.fkScriptWithAlterTable(fk, false, true) + "\n");
				//fos.write( fk.getDefinition(true)+";\n\n" );
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
	
	static void simpleOut(String s, FileWriter fos) throws IOException {
		fos.write( s + "\n" );
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
}
