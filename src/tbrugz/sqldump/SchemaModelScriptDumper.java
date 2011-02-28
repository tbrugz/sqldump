package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
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

	File fileOutput;
	
	boolean dumpWithSchemaName;
	boolean doSchemaDumpPKs;
	boolean dumpFKsInsideTable;
	boolean dumpSynonymAsTable;
	boolean dumpViewAsTable;
	
	Properties columnTypeMapping;
	String fromDbId, toDbId;
	
	String outputFilePattern;
	
	static final String FILENAME_PATTERN_SCHEMA = "\\$\\{schemaname\\}";
	static final String FILENAME_PATTERN_OBJECTTYPE	= "\\$\\{objecttype\\}";
	static final String FILENAME_PATTERN_OBJECTNAME	= "\\$\\{objectname\\}";
	
	@Override
	public void procProperties(Properties prop) {
		//init control vars
		doSchemaDumpPKs = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		//XXX doSchemaDumpFKs = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		boolean doSchemaDumpFKsAtEnd = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		//XXX doSchemaDumpGrants = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		dumpWithSchemaName = prop.getProperty(SQLDataDump.PROP_DUMP_WITH_SCHEMA_NAME, "").equals("true");
		dumpSynonymAsTable = prop.getProperty(SQLDataDump.PROP_DUMP_SYNONYM_AS_TABLE, "").equals("true");
		dumpViewAsTable = prop.getProperty(SQLDataDump.PROP_DUMP_VIEW_AS_TABLE, "").equals("true");

		//dumpPKs = doSchemaDumpPKs;
		fromDbId = prop.getProperty(SQLDataDump.PROP_FROM_DB_ID);
		toDbId = prop.getProperty(SQLDataDump.PROP_TO_DB_ID);
		dumpFKsInsideTable = !doSchemaDumpFKsAtEnd;
		
		outputFilePattern = prop.getProperty("sqldump.outputfilepattern"); //XXX
		if(outputFilePattern==null) { outputFilePattern = prop.getProperty(SQLDataDump.PROP_OUTPUTFILE); }
		
		columnTypeMapping = new Properties();
		try {
			InputStream is = SchemaModelScriptDumper.class.getClassLoader().getResourceAsStream(SQLDataDump.COLUMN_TYPE_MAPPING_RESOURCE);
			if(is==null) throw new IOException("resource "+SQLDataDump.COLUMN_TYPE_MAPPING_RESOURCE+" not found");
			columnTypeMapping.load(is);
		}
		catch(IOException ioe) {
			log.warn("resource "+SQLDataDump.COLUMN_TYPE_MAPPING_RESOURCE+" not found");
		}
	}
	
	
	/*public SchemaModelScriptDumper(FileWriter fos) {
		this.fos = fos;
	}*/
	
	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#isDumpWithSchemaName()
	 */
	public boolean isDumpWithSchemaName() {
		return dumpWithSchemaName;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#setDumpWithSchemaName(boolean)
	 */
	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {
		this.dumpWithSchemaName = dumpWithSchemaName;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#dumpSchema(tbrugz.sqldump.SchemaModel)
	 */
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		log.info("dumping schema... from '"+fromDbId+"' to '"+toDbId+"'");
		log.debug("props->"+columnTypeMapping);
		
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
				String colDesc = SQLDataDump.getColumnDesc(c, columnTypeMapping, fromDbId, toDbId);
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
			
			//Grants
			sb.append(compactGrantDump(table.grants,tableName));
			/*
			for(Grant grant: table.grants) {
				sb.append("grant "+grant.privilege
						+" on "+tableName
						+" to "+grant.grantee
						+(grant.withGrantOption?" WITH GRANT OPTION":"")
						+";\n");
			}*/
			categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, sb.toString());
		}
		
		//FKs
		if(!dumpFKsInsideTable) {
			dumpFKsOutsideTable(schemaModel.foreignKeys);
		}
		
		//Views
		for(View v: schemaModel.views) {
			categorizedOut(v.schemaName, v.name, DBObjectType.VIEW, v.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Triggers
		for(Trigger t: schemaModel.triggers) {
			categorizedOut(t.schemaName, t.name, DBObjectType.TRIGGER, t.getDefinition(dumpWithSchemaName)+"\n");
		}

		//ExecutableObjects
		for(ExecutableObject eo: schemaModel.executables) {
			categorizedOut(eo.schemaName, eo.name, DBObjectType.EXECUTABLE, 
				"-- Executable: "+eo.type+" "+eo.name+"\n"
				+eo.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Synonyms
		for(Synonym s: schemaModel.synonyms) {
			categorizedOut(s.schemaName, s.name, DBObjectType.SYNONYM, s.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Indexes
		for(Index idx: schemaModel.indexes) {
			categorizedOut(idx.schemaName, idx.name, DBObjectType.INDEX, idx.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Sequences
		for(Sequence s: schemaModel.sequences) {
			categorizedOut(s.schemaName, s.name, DBObjectType.SEQUENCE, s.getDefinition(dumpWithSchemaName)+"\n");
		}
	}
	
	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		//StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			String fkscript = "alter table "+(dumpWithSchemaName?fk.fkTableSchemaName+".":"")+fk.fkTable
				+"\n\tadd constraint "+fk.getName()
				+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
				")\n\treferences "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+");\n";
			//sb.append(fkscript+"\n");
			categorizedOut(fk.fkTableSchemaName, fk.getName(), DBObjectType.FK, fkscript);
		}
		//out(sb.toString());
	}
	
	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName) throws IOException {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
					+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
			}
		}
		return sb.toString();
	}
	
	Set<String> filesOpened = new TreeSet<String>();
	
	void categorizedOut(String schemaName, String objectName, DBObjectType objectType, String message) throws IOException {
		String outFile = outputFilePattern.replaceAll(FILENAME_PATTERN_SCHEMA, schemaName)
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
		FileWriter fos = new FileWriter(outFile, alreadyOpened);
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
					+" WITH GRANT OPTION"+";\n");
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
					+";\n");
			/*for(PrivilegeType priv: privs) {
    			sb.append("grant "+priv
    					+" on "+tableName
    					+" to "+grantee
    					+";\n");
			}*/
		}
		
		return sb.toString();
	}
}
