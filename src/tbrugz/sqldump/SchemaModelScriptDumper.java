package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

public class SchemaModelScriptDumper extends SchemaModelDumper {
	
	static Logger log = Logger.getLogger(SchemaModelScriptDumper.class);

	//File fileOutput;
	
	boolean dumpWithSchemaName;
	boolean doSchemaDumpPKs;
	boolean dumpFKsInsideTable;
	boolean dumpSynonymAsTable;
	boolean dumpViewAsTable;
	
	boolean dumpGrantsWithReferencingTable = false;
	boolean dumpIndexesWithReferencingTable = false;
	boolean dumpFKsWithReferencingTable = false;
	boolean dumpTriggersWithReferencingTable = false;
	
	Properties columnTypeMapping;
	String fromDbId, toDbId;
	
	String mainOutputFilePattern;
	Properties prop;
	
	static final String FILENAME_PATTERN_SCHEMA = "\\$\\{schemaname\\}";
	static final String FILENAME_PATTERN_OBJECTTYPE	= "\\$\\{objecttype\\}";
	static final String FILENAME_PATTERN_OBJECTNAME	= "\\$\\{objectname\\}";
	
	public static final String PROP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE = "sqldump.outputobjectwithreferencingtable";
	public static final String PROP_MAIN_OUTPUT_FILE_PATTERN = "sqldump.mainoutputfilepattern";
	
	Map<DBObjectType, DBObjectType> mappingBetweenDBObjectTypes = new HashMap<DBObjectType, DBObjectType>();
	
	@Override
	public void procProperties(Properties prop) {
		//init control vars
		doSchemaDumpPKs = prop.getProperty(SQLDump.PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		//XXX doSchemaDumpFKs = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		boolean doSchemaDumpFKsAtEnd = prop.getProperty(SQLDump.PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		//XXX doSchemaDumpGrants = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		dumpWithSchemaName = prop.getProperty(SQLDump.PROP_DUMP_WITH_SCHEMA_NAME, "").equals("true");
		dumpSynonymAsTable = prop.getProperty(SQLDump.PROP_DUMP_SYNONYM_AS_TABLE, "").equals("true");
		dumpViewAsTable = prop.getProperty(SQLDump.PROP_DUMP_VIEW_AS_TABLE, "").equals("true");

		//dumpPKs = doSchemaDumpPKs;
		fromDbId = prop.getProperty(SQLDump.PROP_FROM_DB_ID);
		toDbId = prop.getProperty(SQLDump.PROP_TO_DB_ID);
		dumpFKsInsideTable = !doSchemaDumpFKsAtEnd;
		
		mainOutputFilePattern = prop.getProperty(PROP_MAIN_OUTPUT_FILE_PATTERN);
		if(mainOutputFilePattern==null) { mainOutputFilePattern = prop.getProperty(SQLDump.PROP_OUTPUTFILE); }
		
		String outputobjectswithtable = prop.getProperty(PROP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE);
		if(outputobjectswithtable!=null) {
			String[] outputsWith = outputobjectswithtable.split(",");
			for(String out: outputsWith) {
				if("grant".equalsIgnoreCase(out.trim())) {
					dumpGrantsWithReferencingTable = true;
				}
				else if("fk".equalsIgnoreCase(out.trim())) {
					dumpFKsWithReferencingTable = true;
				}
				else if("index".equalsIgnoreCase(out.trim())) {
					dumpIndexesWithReferencingTable = true;
				}
				else if("trigger".equalsIgnoreCase(out.trim())) {
					dumpTriggersWithReferencingTable = true;
				}
				else {
					log.warn("unknown object type to output within referencing table: '"+out+"'");
				}
			}
		}
		
		columnTypeMapping = new Properties();
		try {
			InputStream is = SchemaModelScriptDumper.class.getClassLoader().getResourceAsStream(SQLDump.COLUMN_TYPE_MAPPING_RESOURCE);
			if(is==null) throw new IOException("resource "+SQLDump.COLUMN_TYPE_MAPPING_RESOURCE+" not found");
			columnTypeMapping.load(is);
		}
		catch(IOException ioe) {
			log.warn("resource "+SQLDump.COLUMN_TYPE_MAPPING_RESOURCE+" not found");
		}
		
		for(DBObjectType dbtype: DBObjectType.values()) {
			DBObjectType typeMappedTo = null;
			String typeMappedToStr = prop.getProperty("sqldump.outputfilepattern.maptype."+dbtype.name());
			if(typeMappedToStr==null) { continue; }
			try {
				typeMappedTo = DBObjectType.valueOf(typeMappedToStr);
				mappingBetweenDBObjectTypes.put(dbtype, typeMappedTo);
			}
			catch(IllegalArgumentException e) {
				log.warn("unknown object type on property file: '"+typeMappedToStr+"'");
			}
		}
		//sqldump.outputfilepattern.maptype.PROCEDURE=EXECUTABLE
		
		this.prop = prop;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#dumpSchema(tbrugz.sqldump.SchemaModel)
	 */
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		log.info("dumping schema... from '"+fromDbId+"' to '"+toDbId+"'");
		log.debug("props->"+columnTypeMapping);
		
		//XXX: order of objects within table: FK, index, grants? grant, fk, index?
		
		StringBuffer sb = new StringBuffer();
		for(Table table: schemaModel.tables) {
			switch(table.type) {
				case SYNONYM: if(dumpSynonymAsTable) { break; } else { continue; } 
				case VIEW: if(dumpViewAsTable) { break; } else { continue; }
			}
			sb.setLength(0);
			List<String> pkCols = new ArrayList<String>();
			
			String tableName = (dumpWithSchemaName?table.schemaName+".":"")+table.name;
			
			//Table
			sb.append("--drop table "+tableName+";\n");
			sb.append("create table "+tableName+" ( -- type="+table.type+"\n");
			//Columns
			for(Column c: table.columns) {
				String colDesc = SQLDump.getColumnDesc(c, columnTypeMapping, fromDbId, toDbId);
				if(c.pk) { pkCols.add(c.name); }
				sb.append("\t"+colDesc+",\n");
			}
			//PKs
			if(doSchemaDumpPKs && pkCols.size()>0) {
				sb.append("\tconstraint "+table.pkConstraintName+" primary key ("+Utils.join(pkCols, ", ")+"),\n");
			}
			//FKs?
			if(dumpFKsInsideTable) {
				sb.append(dumpFKsInsideTable(schemaModel.foreignKeys, table.schemaName, table.name));
			}
			//Table end
			sb.delete(sb.length()-2, sb.length());
			sb.append("\n);\n");
			
			categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, sb.toString());

			//FK outside table, with referencing table
			if(dumpFKsWithReferencingTable && !dumpFKsInsideTable) {
				for(FK fk: schemaModel.foreignKeys) {
					if(fk.fkTable.equals(table.name)) {
						String fkscript = fkScriptWithAlterTable(fk);
						categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, fkscript);
					}
				}
			}
			
			//Indexes
			if(dumpIndexesWithReferencingTable) {
				for(Index idx: schemaModel.indexes) {
					//option for index output inside table
					if(table.name.equals(idx.tableName)) {
						categorizedOut(idx.schemaName, idx.tableName, DBObjectType.TABLE, idx.getDefinition(dumpWithSchemaName)+"\n");
					}
				}
			}

			//Grants
			if(dumpGrantsWithReferencingTable) {
				String grantOutput = compactGrantDump(table.grants, tableName);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, grantOutput);
				}
			}
			
			//Triggers
			if(dumpTriggersWithReferencingTable) {
				for(Trigger tr: schemaModel.triggers) {
					//option for trigger output inside table
					if(table.name.equals(tr.tableName)) {
						categorizedOut(tr.schemaName, tr.tableName, DBObjectType.TABLE, tr.getDefinition(dumpWithSchemaName)+"\n");
					}
				}
			}
		}
		
		//FKs
		if(!dumpFKsInsideTable && !dumpFKsWithReferencingTable) {
			dumpFKsOutsideTable(schemaModel.foreignKeys);
		}
		
		//Views
		for(View v: schemaModel.views) {
			categorizedOut(v.schemaName, v.name, DBObjectType.VIEW, v.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Triggers
		for(Trigger t: schemaModel.triggers) {
			if(!dumpTriggersWithReferencingTable || t.tableName==null) {
				categorizedOut(t.schemaName, t.name, DBObjectType.TRIGGER, t.getDefinition(dumpWithSchemaName)+"\n");
			}
		}

		//ExecutableObjects
		for(ExecutableObject eo: schemaModel.executables) {
			// TODOne categorizedOut(eo.schemaName, eo.name, DBObjectType.EXECUTABLE, 
			categorizedOut(eo.schemaName, eo.name, eo.type, 
				"-- Executable: "+eo.type+" "+eo.name+"\n"
				+eo.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Synonyms
		for(Synonym s: schemaModel.synonyms) {
			categorizedOut(s.schemaName, s.name, DBObjectType.SYNONYM, s.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Indexes
		if(!dumpIndexesWithReferencingTable) {
			for(Index idx: schemaModel.indexes) {
				categorizedOut(idx.schemaName, idx.name, DBObjectType.INDEX, idx.getDefinition(dumpWithSchemaName)+"\n");
			}
		}
		
		//Grants
		if(!dumpGrantsWithReferencingTable) {
			for(Table table: schemaModel.tables) {
				String tableName = (dumpWithSchemaName?table.schemaName+".":"")+table.name;
				String grantOutput = compactGrantDump(table.grants, tableName);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.schemaName, table.name, DBObjectType.GRANT, grantOutput);
				}
			}
		}

		//Sequences
		for(Sequence s: schemaModel.sequences) {
			categorizedOut(s.schemaName, s.name, DBObjectType.SEQUENCE, s.getDefinition(dumpWithSchemaName)+"\n");
		}

		log.info("...schema dumped");
	}
	
	String fkScriptWithAlterTable(FK fk) {
		return "alter table "+(dumpWithSchemaName?fk.fkTableSchemaName+".":"")+fk.fkTable
			+"\n\tadd "+fkSimpleScript(fk, "\n\t")+";\n";
			
			//"add constraint "+fk.getName()
			//+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
			//")\n\treferences "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+");\n";
	}

	String fkSimpleScript(FK fk, String whitespace) {
		whitespace = whitespace.replaceAll("[^ \n\t]", " ");
		return "constraint "+fk.getName()
			+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
			")"+whitespace+"references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+")";
	}
	
	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		//StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			String fkscript = fkScriptWithAlterTable(fk);
			//sb.append(fkscript+"\n");
			//if(dumpFKsWithReferencingTable) {
			//	categorizedOut(fk.fkTableSchemaName, fk.fkTable, DBObjectType.TABLE, fkscript);
			//}
			//else {
			categorizedOut(fk.fkTableSchemaName, fk.getName(), DBObjectType.FK, fkscript);
			//}
		}
		//out(sb.toString());
	}
	
	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName) throws IOException {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				//sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
				//	+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
				sb.append("\t"+fkSimpleScript(fk, " ")+",\n");
			}
		}
		return sb.toString();
	}
	
	//XXXcc Map<String, FileWriter> ? not if "sqldump.outputfilepattern" contains ${objectname}
	//Map<DBObjectType, String> outFilePatterns = new HashMap<DBObjectType, String>();
	Set<String> filesOpened = new TreeSet<String>();
	
	void categorizedOut(String schemaName, String objectName, DBObjectType objectType, String message) throws IOException {
		//String outFilePattern = outFilePatterns.get(objectType);
		//if(outFilePatterns.containsKey(objectType)) {}
		DBObjectType mappedObjectType = mappingBetweenDBObjectTypes.get(objectType);
		if(mappedObjectType!=null) { objectType = mappedObjectType; }
		
		String outFilePattern = prop.getProperty("sqldump.outputfilepattern.bytype."+objectType.name());
		if(outFilePattern==null) {
			outFilePattern = mainOutputFilePattern;
		}
		
		objectName = objectName.replaceAll("\\$", "\\\\\\$");  //indeed strange but necessary if objectName contains "$". see Matcher.replaceAll
		
		String outFile = outFilePattern.replaceAll(FILENAME_PATTERN_SCHEMA, schemaName)
			.replaceAll(FILENAME_PATTERN_OBJECTTYPE, objectType.name())
			.replaceAll(FILENAME_PATTERN_OBJECTNAME, objectName);
		boolean alreadyOpened = filesOpened.contains(outFile);
		if(!alreadyOpened) { filesOpened.add(outFile); }
		
		File f = new File(outFile);
		//String dirStr = f.getParent();
		File dir = new File(f.getParent());
		if(!dir.exists()) {
			dir.mkdirs();
		}
		else {
			if(!dir.isDirectory()) {
				throw new IOException(dir+" already exists and is not a directory");
			}
		}
		FileWriter fos = new FileWriter(f, alreadyOpened); //if already opened, append; if not, create
		fos.write(message+"\n");
		fos.close();
	}
	
	String compactGrantDump(List<Grant> grants, String tableName) {
		Map<String, Set<PrivilegeType>> mapWithGrant = new TreeMap<String, Set<PrivilegeType>>();
		Map<String, Set<PrivilegeType>> mapWOGrant = new TreeMap<String, Set<PrivilegeType>>();
		
		for(Grant g: grants) {
			if(g.withGrantOption) {
    			Set<PrivilegeType> privs = mapWithGrant.get(g.grantee);
    			if(privs==null) {
    				privs = new TreeSet<PrivilegeType>();
    				mapWithGrant.put(g.grantee, privs);
    			}
    			privs.add(g.privilege);
			}
			else {
    			Set<PrivilegeType> privs = mapWOGrant.get(g.grantee);
    			if(privs==null) {
    				privs = new TreeSet<PrivilegeType>();
    				mapWOGrant.put(g.grantee, privs);
    			}
    			privs.add(g.privilege);
			}
		}
		
		StringBuffer sb = new StringBuffer();
		
		for(String grantee: mapWithGrant.keySet()) {
			Set<PrivilegeType> privs = mapWithGrant.get(grantee);
			String privsStr = Utils.join(privs, ", ");
			sb.append("grant "+privsStr
					+" on "+tableName
					+" to "+grantee
					+" WITH GRANT OPTION"+";\n\n");
			/*for(PrivilegeType priv: privs) {
    			sb.append("grant "+priv
    					+" on "+tableName
    					+" to "+grantee
    					+" WITH GRANT OPTION"+";\n");
			}*/
		}

		for(String grantee: mapWOGrant.keySet()) {
			Set<PrivilegeType> privs = mapWOGrant.get(grantee);
			String privsStr = Utils.join(privs, ", ");
			sb.append("grant "+privsStr
					+" on "+tableName
					+" to "+grantee
					+";\n\n");
			/*for(PrivilegeType priv: privs) {
    			sb.append("grant "+priv
    					+" on "+tableName
    					+" to "+grantee
    					+";\n");
			}*/
		}

		//return sb.toString();
		if(sb.length()>2) {
			return sb.substring(0, sb.length()-1);
		}
		return "";
	}
}
