package tbrugz.sqldump.xtradumpers;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;

/*
 * currently suggests:
 * - index creations based on FKs
 * 
 * XXX: future suggestions: FK creations based on column names and existing PKs/UKs (?)
 */
public class AlterSchemaSuggester implements SchemaModelDumper {

	public static final String PROP_ALTER_SCHEMA_SUGGESTER_OUTFILE = "sqldump.alterschemasuggester.outfile";
	public static final String PROP_ALTER_SCHEMA_SUGGESTER_ALTEROBJECTSFROMSCHEMAS = "sqldump.alterschemasuggester.alterobjectsfromschemas";
	
	static Logger log = Logger.getLogger(AlterSchemaSuggester.class);
	
	String fileOutput;
	List<String> schemasToAlter;

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
		
		dumpCreateIndexes(schemaModel);
	}
		
	public void dumpCreateIndexes(SchemaModel schemaModel) throws Exception {
		
		Set<Index> indexes = new TreeSet<Index>(); //HashSet doesn't work (uses hashCode()?)
		
		//FKs
		//int fkIndexCounter = 0;
		for(FK fk: schemaModel.getForeignKeys()) {
			boolean pkTableHasIndex = false;
			boolean fkTableHasIndex = false;
			
			//Table
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
				idx.name = fk.pkTable + "_" + suggestIndexAcronym(idx) + "_UKI"; //_" + (indexes.size()+1);
				addIndex(indexes, idx);
			}

			if(!fkTableHasIndex) {
				Index idx = new Index();
				idx.tableName = fk.fkTable;
				idx.schemaName = fk.fkTableSchemaName;
				idx.unique = false;
				idx.columns.addAll(fk.fkColumns);
				idx.name = fk.fkTable + "_" + suggestIndexAcronym(idx) + "_FKI";
				//idx.name = fk.fkTable + "_FKI";
				//idx.name = fk.fkTable + "_FKI_" + idx.hashCode();
				//idx.name = fk.fkTable + "_FKI_" + (++fkIndexCounter);
				addIndex(indexes, idx);
			}
		}
		
		if(indexes.size()>0) {
			log.info(indexes.size()+" alter index generated");
			FileWriter fos = new FileWriter(fileOutput, true); //append
			int dumpCounter = 0;
			for(Index idx: indexes) {
				if(schemasToAlter==null || (schemasToAlter!=null && schemasToAlter.contains(idx.schemaName))) {
					fos.write( idx.getDefinition(true)+";\n\n" );
					dumpCounter++;
				}
			}
			log.info("dumped "+dumpCounter+" alter index statements");
			fos.close();
		}
		else {
			log.info("no alter schema suggestions");
		}
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
	
	static String suggestIndexAcronym(Index idx) {
		StringBuffer sb = new StringBuffer();
		for(String col: idx.columns) {
			String[] strs = col.split("_");
			for(String s: strs) {
				sb.append(s.substring(0, 1));
			}
		}
		return sb.toString();
	}

}
