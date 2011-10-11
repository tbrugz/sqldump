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

import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;

/*
 * currently suggests:
 * - index creations based on FKs
 * - FKs creation based on column names and existing PKs/UKs
 * 
 * TODOne: FK creations based on column names and existing PKs/UKs
 * - option to suggest only simple (not composite) FKs
 * - test if FK is possible (all values in fkTable exists in pkTable) - needs connection, can be very time consuming
 * - test column types
 * 
 * XXX: suggest PKs for tables that doesn't have one...
 */
public class AlterSchemaSuggester implements SchemaModelDumper {

	public static final String PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE = "sqldump.alterschemasuggester.outfile";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS = "sqldump.alterschemasuggester.alterobjectsfromschemas";
	
	static Logger log = Logger.getLogger(AlterSchemaSuggester.class);
	
	String fileOutput;
	List<String> schemasToAlter;
	boolean dumpSimpleFKsOnly = true; //XXX: add prop for dumpSimpleFKsOnly

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
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		if(fileOutput==null) {
			log.warn("prop '"+PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE+"' not defined");
			return;
		}
		
		simpleOut("-- indexes", fileOutput);
		dumpCreateIndexes(schemaModel, schemaModel.getForeignKeys());
		
		simpleOut("\n-- FKs", fileOutput);
		Set<FK> alterFKs = dumpCreateFKs(schemaModel);
		
		simpleOut("\n-- indexes for generated FKs", fileOutput);
		dumpCreateIndexes(schemaModel, alterFKs);
	}
		
	int dumpCreateIndexes(SchemaModel schemaModel, Set<FK> foreignKeys) throws Exception {
		
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
				idx.schemaName = fk.pkTableSchemaName;
				idx.unique = false;
				idx.columns.addAll(fk.pkColumns);
				idx.name = fk.pkTable + "_" + suggestAcronym(idx.columns) + "_UKI"; //_" + (indexes.size()+1);
				addIndex(indexes, idx);
			}

			if(!fkTableHasIndex) {
				Index idx = new Index();
				idx.tableName = fk.fkTable;
				idx.schemaName = fk.fkTableSchemaName;
				idx.unique = false;
				idx.columns.addAll(fk.fkColumns);
				idx.name = fk.fkTable + "_" + suggestAcronym(idx.columns) + "_FKI";
				//idx.name = fk.fkTable + "_FKI";
				//idx.name = fk.fkTable + "_FKI_" + idx.hashCode();
				//idx.name = fk.fkTable + "_FKI_" + (++fkIndexCounter);
				addIndex(indexes, idx);
			}
		}
		
		int dumpCounter = 0;
		if(indexes.size()>0) {
			log.info(indexes.size()+" 'create index' generated");
			FileWriter fos = new FileWriter(fileOutput, true); //append
			for(Index idx: indexes) {
				if(schemasToAlter==null || (schemasToAlter!=null && schemasToAlter.contains(idx.schemaName))) {
					fos.write( idx.getDefinition(true)+";\n\n" );
					dumpCounter++;
				}
			}
			log.info("dumped "+dumpCounter+" 'create index' statements");
			fos.close();
		}
		else {
			log.info("no 'create index' alter schema suggestions");
		}
		
		return dumpCounter;
	}
	
	void addIndex(Set<Index> indexes, Index idx) {
		//TreeSet.contains() doesn't work if Comparator<> isn't properly implemented
		if(indexes.contains(idx)) {
		//if(containsBasedOnEquals(indexes, idx)) {
			log.debug("duplicate index "+idx+" ignored");
			for(Index i: indexes) {
				if(i.equals(idx)) {
					log.debug("dup idx is: "+i);
				}
				if(i.hashCode()==idx.hashCode()) {
					log.debug("dup idx is: "+i+"; hash is "+idx.hashCode());
				}
			}
		}
		else {
			indexes.add(idx);
		}
	}

	Set<FK> dumpCreateFKs(SchemaModel schemaModel) throws Exception {
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
						fk.pkTableSchemaName = table.schemaName;
						fk.pkColumns.addAll(cons.uniqueColumns);
						fk.fkTable = otherT.name;
						fk.fkTableSchemaName = otherT.schemaName;
						fk.fkColumns.addAll(cons.uniqueColumns);
						fk.fkReferencesPK = (cons.type==ConstraintType.PK);
						fk.name = suggestAcronym(fk.pkTable) + "_" + suggestAcronym(fk.fkTable) + "_FK";

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
		
		int dumpCounter = 0;
		if(fks.size()>0) {
			log.info(fks.size()+" 'add foreign key's generated");
			FileWriter fos = new FileWriter(fileOutput, true); //append
			for(FK fk: fks) {
				if(schemasToAlter==null || (schemasToAlter!=null && schemasToAlter.contains(fk.fkTableSchemaName))) {
					fos.write( SchemaModelScriptDumper.fkScriptWithAlterTable(fk, false, true) + "\n");
					//fos.write( fk.getDefinition(true)+";\n\n" );
					dumpCounter++;
				}
			}
			log.info("dumped "+dumpCounter+" 'add foreign key' statements");
			fos.close();
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
			sb.append(s.substring(0, 1));
		}
		return sb.toString();
	}
	
	static void simpleOut(String s, String fileOutput) throws IOException {
		FileWriter fos = new FileWriter(fileOutput, true); //append
		fos.write( s + "\n");
		fos.close();
	}
	
}
