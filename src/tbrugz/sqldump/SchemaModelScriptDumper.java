package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: quote object names when they contain strange symbols (like "-")
 * TODOne: quote all object names
 * TODO: option to output object name with toLowerCase() or toUpperCase()
 */
public class SchemaModelScriptDumper implements SchemaModelDumper {
	
	static Log log = LogFactory.getLog(SchemaModelScriptDumper.class);

	//File fileOutput;
	
	boolean dumpWithSchemaName = false;
	boolean doSchemaDumpPKs = true;
	boolean dumpFKsInsideTable = false;
	boolean dumpSynonymAsTable = false;
	boolean dumpViewAsTable = false;
	boolean dumpMaterializedViewAsTable = false;
	
	boolean dumpGrantsWithReferencingTable = false;
	boolean dumpIndexesWithReferencingTable = false;
	boolean dumpFKsWithReferencingTable = false;
	boolean dumpTriggersWithReferencingTable = false;
	
	boolean dumpDropStatements = false;
	boolean dumpWithCreateOrReplace = false;
	
	//Properties dbmsSpecificsProperties;
	String fromDbId, toDbId;
	
	String mainOutputFilePattern;
	Properties prop;
	
	static final String FILENAME_PATTERN_SCHEMA = "\\$\\{schemaname\\}";
	static final String FILENAME_PATTERN_OBJECTTYPE	= "\\$\\{objecttype\\}";
	static final String FILENAME_PATTERN_OBJECTNAME	= "\\$\\{objectname\\}";
	
	public static final String PROP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE = "sqldump.outputobjectwithreferencingtable";
	public static final String PROP_MAIN_OUTPUT_FILE_PATTERN = "sqldump.mainoutputfilepattern";
	
	static final String PROP_OUTPUTFILE = "sqldump.outputfile";

	static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";
	static final String PROP_DO_SCHEMADUMP_FKS_ATEND = "sqldump.doschemadump.fks.atend";

	static final String PROP_DUMP_SYNONYM_AS_TABLE = "sqldump.dumpsynonymastable";
	static final String PROP_DUMP_VIEW_AS_TABLE = "sqldump.dumpviewastable";
	static final String PROP_DUMP_MATERIALIZEDVIEW_AS_TABLE = "sqldump.dumpmaterializedviewastable";

	static final String PROP_SCHEMADUMP_DUMPDROPSTATEMENTS = "sqldump.schemadump.dumpdropstatements";
	static final String PROP_SCHEMADUMP_USECREATEORREPLACE = "sqldump.schemadump.usecreateorreplace";
	static final String PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS = "sqldump.schemadump.quoteallsqlidentifiers";
	
	Map<DBObjectType, DBObjectType> mappingBetweenDBObjectTypes = new HashMap<DBObjectType, DBObjectType>();
	
	@Override
	public void procProperties(Properties prop) {
		//init control vars
		doSchemaDumpPKs = Utils.getPropBool(prop, JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_PKS, doSchemaDumpPKs);
		//XXX doSchemaDumpFKs = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		boolean doSchemaDumpFKsAtEnd = Utils.getPropBool(prop, PROP_DO_SCHEMADUMP_FKS_ATEND, !dumpFKsInsideTable);
		//XXX doSchemaDumpGrants = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		dumpWithSchemaName = Utils.getPropBool(prop, PROP_DUMP_WITH_SCHEMA_NAME, dumpWithSchemaName);
		dumpSynonymAsTable = Utils.getPropBool(prop, PROP_DUMP_SYNONYM_AS_TABLE, dumpSynonymAsTable);
		dumpViewAsTable = Utils.getPropBool(prop, PROP_DUMP_VIEW_AS_TABLE, dumpViewAsTable);
		dumpMaterializedViewAsTable = Utils.getPropBool(prop, PROP_DUMP_MATERIALIZEDVIEW_AS_TABLE, dumpMaterializedViewAsTable); //default should be 'true'?
		dumpDropStatements = Utils.getPropBool(prop, PROP_SCHEMADUMP_DUMPDROPSTATEMENTS, dumpDropStatements);
		dumpWithCreateOrReplace = Utils.getPropBool(prop, PROP_SCHEMADUMP_USECREATEORREPLACE, dumpWithCreateOrReplace);
		DBObject.dumpCreateOrReplace = dumpWithCreateOrReplace;
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);

		//dumpPKs = doSchemaDumpPKs;
		fromDbId = DBMSResources.instance().dbid();
		toDbId = prop.getProperty(Defs.PROP_TO_DB_ID);
		dumpFKsInsideTable = !doSchemaDumpFKsAtEnd;
		
		mainOutputFilePattern = prop.getProperty(PROP_MAIN_OUTPUT_FILE_PATTERN);
		if(mainOutputFilePattern==null) { mainOutputFilePattern = prop.getProperty(PROP_OUTPUTFILE); }
		
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
		
		/*dbmsSpecificsProperties = new ParametrizedProperties();
		try {
			InputStream is = SchemaModelScriptDumper.class.getClassLoader().getResourceAsStream(SQLDump.DBMS_SPECIFIC_RESOURCE);
			if(is==null) throw new IOException("resource "+SQLDump.DBMS_SPECIFIC_RESOURCE+" not found");
			dbmsSpecificsProperties.load(is);
		}
		catch(IOException ioe) {
			log.warn("resource "+SQLDump.DBMS_SPECIFIC_RESOURCE+" not found");
		}*/
		
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
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			
		log.info("dumping schema... from '"+fromDbId+"' to '"+toDbId+"'");
		if(fromDbId==null || toDbId==null) {
			log.info("fromDbId or toDbId null: no conversion");
		}
		else {
			if(fromDbId.equals(toDbId)) {
				//XXX: define global var for optimization?
				log.info("equal origin and target DBMSs: no column type conversion");
			}
		}
		Properties colTypeConversionProp = new ParametrizedProperties();
		
		if(DBMSResources.instance().dbid()!=null) { colTypeConversionProp.put(Defs.PROP_FROM_DB_ID, DBMSResources.instance().dbid()); }
		if(toDbId!=null) { colTypeConversionProp.put(Defs.PROP_TO_DB_ID, toDbId); }
		//XXX: order of objects within table: FK, index, grants? grant, fk, index?
		
		if(schemaModel==null || schemaModel.getTables()==null) {
			log.warn("no tables grabbed! terminating dumpSchema()");
			return;
		}
		
		for(Table table: schemaModel.getTables()) {
			switch(table.getType()) {
				case SYNONYM: if(dumpSynonymAsTable) { break; } else { continue; } 
				case VIEW: if(dumpViewAsTable) { break; } else { continue; }
				case MATERIALIZED_VIEW: if(dumpMaterializedViewAsTable) { break; } else { continue; }
			}
			
			categorizedOut(table.getSchemaName(), table.name, DBObjectType.TABLE, table.getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable, dumpDropStatements, colTypeConversionProp, schemaModel.getForeignKeys())+";\n");
			String afterTableScript = table.getAfterCreateTableScript();
			if(afterTableScript!=null && !afterTableScript.trim().equals("")) {
				categorizedOut(table.getSchemaName(), table.name, DBObjectType.TABLE, afterTableScript);
			}
			
			//table end: ';'
			//categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, ";\n");

			//FK outside table, with referencing table
			if(dumpFKsWithReferencingTable && !dumpFKsInsideTable) {
				for(FK fk: schemaModel.getForeignKeys()) {
					if(fk.fkTable.equals(table.name)) {
						String fkscript = fkScriptWithAlterTable(fk, dumpDropStatements, dumpWithSchemaName);
						categorizedOut(table.getSchemaName(), table.name, DBObjectType.TABLE, fkscript);
					}
				}
			}
			
			//Indexes
			if(dumpIndexesWithReferencingTable) {
				for(Index idx: schemaModel.getIndexes()) {
					//option for index output inside table
					if(table==null || idx==null) {
						log.warn("index null? table: "+table+" / idx: "+idx);
						continue;
					}
					if(table.name.equals(idx.tableName)) {
						categorizedOut(idx.getSchemaName(), idx.tableName, DBObjectType.TABLE, idx.getDefinition(dumpWithSchemaName)+";\n");
					}
				}
			}

			String tableName = (dumpWithSchemaName?table.getSchemaName()+".":"")+table.name;
			
			//Grants
			if(dumpGrantsWithReferencingTable) {
				String grantOutput = compactGrantDump(table.getGrants(), tableName, toDbId);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.getSchemaName(), table.name, DBObjectType.TABLE, grantOutput);
				}
			}
			
			//Triggers
			if(dumpTriggersWithReferencingTable) {
				for(Trigger tr: schemaModel.getTriggers()) {
					//option for trigger output inside table
					if(table.name.equals(tr.tableName)) {
						categorizedOut(tr.getSchemaName(), tr.tableName, DBObjectType.TABLE, tr.getDefinition(dumpWithSchemaName)+"\n");
					}
				}
			}
		}
		
		//FKs
		if(!dumpFKsInsideTable && !dumpFKsWithReferencingTable) {
			dumpFKsOutsideTable(schemaModel.getForeignKeys());
		}
		
		//Views
		for(View v: schemaModel.getViews()) {
			categorizedOut(v.getSchemaName(), v.name, DBObjectType.VIEW, v.getDefinition(dumpWithSchemaName)+";\n");
		}

		//Triggers
		for(Trigger t: schemaModel.getTriggers()) {
			if(!dumpTriggersWithReferencingTable || t.tableName==null) {
				categorizedOut(t.getSchemaName(), t.name, DBObjectType.TRIGGER, t.getDefinition(dumpWithSchemaName)+"\n");
			}
		}

		//ExecutableObjects
		for(ExecutableObject eo: schemaModel.getExecutables()) {
			// TODOne categorizedOut(eo.schemaName, eo.name, DBObjectType.EXECUTABLE, 
			categorizedOut(eo.getSchemaName(), eo.name, eo.type, 
				"-- Executable: "+eo.type+" "+eo.name+"\n"
				+eo.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Synonyms
		for(Synonym s: schemaModel.getSynonyms()) {
			categorizedOut(s.getSchemaName(), s.name, DBObjectType.SYNONYM, s.getDefinition(dumpWithSchemaName)+";\n");
		}

		//Indexes
		if(!dumpIndexesWithReferencingTable) {
			for(Index idx: schemaModel.getIndexes()) {
				categorizedOut(idx.getSchemaName(), idx.name, DBObjectType.INDEX, idx.getDefinition(dumpWithSchemaName)+";\n");
			}
		}
		
		//Grants
		if(!dumpGrantsWithReferencingTable) {
			for(Table table: schemaModel.getTables()) {
				String tableName = (dumpWithSchemaName?table.getSchemaName()+".":"")+table.name;
				String grantOutput = compactGrantDump(table.getGrants(), tableName, toDbId);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.getSchemaName(), table.name, DBObjectType.GRANT, grantOutput);
				}
			}
		}

		//Sequences
		for(Sequence s: schemaModel.getSequences()) {
			categorizedOut(s.getSchemaName(), s.name, DBObjectType.SEQUENCE, s.getDefinition(dumpWithSchemaName)+";\n");
		}

		log.info("...schema dumped");
		
		}
		catch(Exception e) {
			log.warn("error dumping schema: "+e);
			log.info("error dumping schema", e);
		}
	}
	
	public static String fkScriptWithAlterTable(FK fk, boolean dumpDropStatements, boolean dumpWithSchemaName) {
		String fkTableName = (dumpWithSchemaName?DBObject.getFinalIdentifier(fk.fkTableSchemaName)+".":"")+DBObject.getFinalIdentifier(fk.fkTable);
		return
			(dumpDropStatements?"--alter table "+fkTableName+" drop constraint "+fk.getName()+";\n":"")
			+"alter table "+fkTableName
			+"\n\tadd "+FK.fkSimpleScript(fk, "\n\t", dumpWithSchemaName)+";\n";
	}

	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		//StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			String fkscript = fkScriptWithAlterTable(fk, dumpDropStatements, dumpWithSchemaName);
			//sb.append(fkscript+"\n");
			//if(dumpFKsWithReferencingTable) {
			//	categorizedOut(fk.fkTableSchemaName, fk.fkTable, DBObjectType.TABLE, fkscript);
			//}
			//else {
			categorizedOut(fk.getSchemaName(), fk.getName(), DBObjectType.FK, fkscript);
			//}
		}
		//out(sb.toString());
	}
	
	/*
	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName) throws IOException {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				//sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
				//	+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
				sb.append("\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName)+",\n");
			}
		}
		return sb.toString();
	}
	*/
	
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
		if(outFilePattern==null) {
			throw new RuntimeException("output file patterns (e.g. 'sqldump.mainoutputfilepattern') not defined, aborting");
		}
		
		objectName = objectName.replaceAll("\\$", "\\\\\\$");  //indeed strange but necessary if objectName contains "$". see Matcher.replaceAll
		
		if(schemaName==null) { schemaName = ""; };
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
		//XXX: remove '\n'?
		fos.write(message+"\n");
		fos.close();
	}
	
	String compactGrantDump(List<Grant> grants, String tableName, String toDbId) {
		Map<String, Set<PrivilegeType>> mapWithGrant = new TreeMap<String, Set<PrivilegeType>>();
		Map<String, Set<PrivilegeType>> mapWOGrant = new TreeMap<String, Set<PrivilegeType>>();
		
		Set<String> privsToDump = new TreeSet<String>();
		if(toDbId!=null && !toDbId.equals("")) {
			String sPriv = DBMSResources.instance().getPrivileges(toDbId);
			//dbmsSpecificsProperties.getProperty("privileges."+toDbId);
			if(sPriv!=null) {
				String[] privs = sPriv.split(",");
				for(String priv: privs) {
					String privOk = priv.trim();
					privsToDump.add(privOk);
				}
			}
		}
		
		for(Grant g: grants) {
			//if privilege is not defined for target DB, do not dump
			if(toDbId!=null && !privsToDump.contains(g.privilege.toString())) { continue; }
			
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
					+" on "+DBObject.getFinalIdentifier(tableName)
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
					+" on "+DBObject.getFinalIdentifier(tableName)
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

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
}
