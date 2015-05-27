package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
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
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: quote object names when they contain strange symbols (like "-")
 * TODOne: quote all object names
 * TODO: option to output object name with toLowerCase() or toUpperCase()
 */
public class SchemaModelScriptDumper extends AbstractFailable implements SchemaModelDumper {
	
	private static final Log log = LogFactory.getLog(SchemaModelScriptDumper.class);

	boolean dumpWithSchemaName = false;
	boolean doSchemaDumpPKs = true;
	boolean dumpFKsInsideTable = false;
	boolean dumpSynonymAsTable = false;
	boolean dumpViewAsTable = false;
	boolean dumpMaterializedViewAsTable = false;
	
	boolean dumpGrantsWithReferencingTable = false;
	boolean dumpIndexesWithReferencingTable = false;
	boolean dumpIndexesSortedByTableName = false;
	boolean dumpFKsWithReferencingTable = false;
	boolean dumpTriggersWithReferencingTable = false;
	
	boolean dumpDropStatements = false;
	boolean dumpWithCreateOrReplace = false;
	boolean dumpScriptComments = true;
	boolean dumpRemarks = true;
	//boolean dumpWriteAppend = false;
	
	boolean dumpFKs = true;
	
	//Properties dbmsSpecificsProperties;
	String fromDbId, toDbId;
	
	String mainOutputFilePattern;
	Properties prop;
	String outputConnPropPrefix;
	Connection outputConn = null;
	
	static final String PREFIX = "sqldump.schemadump";
	
	static final String PATTERN_SCHEMANAME_FINAL = Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME);
	static final String PATTERN_SCHEMANAME_QUOTED = Pattern.quote(PATTERN_SCHEMANAME_FINAL);
	static final String PATTERN_OBJECTTYPE_FINAL = Defs.addSquareBraquets(Defs.PATTERN_OBJECTTYPE);
	static final String PATTERN_OBJECTTYPE_QUOTED = Pattern.quote(PATTERN_OBJECTTYPE_FINAL);
	static final String PATTERN_OBJECTNAME_FINAL = Defs.addSquareBraquets(Defs.PATTERN_OBJECTNAME);
	static final String PATTERN_OBJECTNAME_QUOTED = Pattern.quote(PATTERN_OBJECTNAME_FINAL);
	
	@Deprecated //also used by SQLDiff
	static final String FILENAME_PATTERN_SCHEMA = "${schemaname}";
	@Deprecated
	static final String FILENAME_PATTERN_SCHEMA_QUOTED = Pattern.quote(FILENAME_PATTERN_SCHEMA);
	@Deprecated
	static final String FILENAME_PATTERN_OBJECTTYPE = "${objecttype}";
	@Deprecated
	static final String FILENAME_PATTERN_OBJECTTYPE_QUOTED = Pattern.quote(FILENAME_PATTERN_OBJECTTYPE);
	@Deprecated
	static final String FILENAME_PATTERN_OBJECTNAME = "${objectname}";
	@Deprecated
	static final String FILENAME_PATTERN_OBJECTNAME_QUOTED = Pattern.quote(FILENAME_PATTERN_OBJECTNAME);

	@Deprecated static final String PROP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE = "sqldump.outputobjectwithreferencingtable";

	@Deprecated static final String PROP_MAIN_OUTPUT_FILE_PATTERN = "sqldump.mainoutputfilepattern";
	static final String PROP_OUTPUT_CONN_PROP_PREFIX = PREFIX+".output.connpropprefix";

	@Deprecated static final String PROP_DO_SCHEMADUMP_FKS_ATEND = "sqldump.doschemadump.fks.atend";

	static final String PROP_SCHEMADUMP_SYNONYM_AS_TABLE = PREFIX+".dumpsynonymastable";
	static final String PROP_SCHEMADUMP_VIEW_AS_TABLE = PREFIX+".dumpviewastable";
	static final String PROP_SCHEMADUMP_MATERIALIZEDVIEW_AS_TABLE = PREFIX+".dumpmaterializedviewastable";

	@Deprecated static final String PROP_DUMP_SYNONYM_AS_TABLE = "sqldump.dumpsynonymastable";
	@Deprecated static final String PROP_DUMP_VIEW_AS_TABLE = "sqldump.dumpviewastable";
	@Deprecated static final String PROP_DUMP_MATERIALIZEDVIEW_AS_TABLE = "sqldump.dumpmaterializedviewastable";
	@Deprecated static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";

	//also used by SQLDiff
	static final String PROP_SCHEMADUMP_DUMP_WITH_SCHEMA_NAME = PREFIX+".dumpwithschemaname"; //XXX SQLDiff should use this?
	public static final String PROP_SCHEMADUMP_USECREATEORREPLACE = PREFIX+".usecreateorreplace";
	public static final String PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS = PREFIX+".quoteallsqlidentifiers";

	static final String PROP_SCHEMADUMP_DUMPDROPSTATEMENTS = PREFIX+".dumpdropstatements";
	static final String PROP_SCHEMADUMP_DUMPSCRIPTCOMMENTS = PREFIX+".dumpscriptcomments";
	static final String PROP_SCHEMADUMP_DUMPREMARKS = PREFIX+".dumpremarks";
	static final String PROP_SCHEMADUMP_INDEXORDERBY = PREFIX+".index.orderby";
	static final String PROP_SCHEMADUMP_PKS = PREFIX+".pks";
	static final String PROP_SCHEMADUMP_FKs = PREFIX+".fks";
	//static final String PROP_SCHEMADUMP_WRITEAPPEND = PREFIX+".writeappend";
	
	static final String PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN = PREFIX+".outputfilepattern";
	static final String PROP_SCHEMADUMP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE = PREFIX+".outputobjectwithreferencingtable";
	static final String PROP_SCHEMADUMP_FKS_ATEND =  PREFIX+".fks.atend";
	
	@Deprecated static final String PREFIX_OUTPATTERN_BYTYPE = "sqldump.outputfilepattern.bytype.";
	@Deprecated static final String PREFIX_OUTPATTERN_MAPTYPE = "sqldump.outputfilepattern.maptype.";

	static final String PREFIX_SCHEMADUMP_OUTPATTERN_BYTYPE = PREFIX+".outputfilepattern.bytype.";
	static final String PREFIX_SCHEMADUMP_OUTPATTERN_MAPTYPE = PREFIX+".outputfilepattern.maptype.";
	
	Map<DBObjectType, DBObjectType> mappingBetweenDBObjectTypes = new HashMap<DBObjectType, DBObjectType>();
	
	@Override
	public void setProperties(Properties prop) {
		//init control vars
		doSchemaDumpPKs = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_PKS, JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_PKS, doSchemaDumpPKs);
		boolean doSchemaDumpFKsAtEnd = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_FKS_ATEND, PROP_DO_SCHEMADUMP_FKS_ATEND, !dumpFKsInsideTable);
		//XXX doSchemaDumpGrants = prop.getProperty(SQLDataDump.PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		dumpWithSchemaName = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_DUMP_WITH_SCHEMA_NAME, PROP_DUMP_WITH_SCHEMA_NAME, dumpWithSchemaName);
		dumpSynonymAsTable = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_SYNONYM_AS_TABLE, PROP_DUMP_SYNONYM_AS_TABLE, dumpSynonymAsTable);
		dumpViewAsTable = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_VIEW_AS_TABLE, PROP_DUMP_VIEW_AS_TABLE, dumpViewAsTable);
		dumpMaterializedViewAsTable = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMADUMP_MATERIALIZEDVIEW_AS_TABLE, PROP_DUMP_MATERIALIZEDVIEW_AS_TABLE, dumpMaterializedViewAsTable);
		dumpDropStatements = Utils.getPropBool(prop, PROP_SCHEMADUMP_DUMPDROPSTATEMENTS, dumpDropStatements);
		dumpWithCreateOrReplace = Utils.getPropBool(prop, PROP_SCHEMADUMP_USECREATEORREPLACE, dumpWithCreateOrReplace);
		dumpScriptComments = Utils.getPropBool(prop, PROP_SCHEMADUMP_DUMPSCRIPTCOMMENTS, dumpScriptComments);
		dumpRemarks = Utils.getPropBool(prop, PROP_SCHEMADUMP_DUMPREMARKS, dumpRemarks);
		String dumpIndexOrderBy = prop.getProperty(PROP_SCHEMADUMP_INDEXORDERBY);
		dumpIndexesSortedByTableName = "tablename".equalsIgnoreCase(dumpIndexOrderBy);
		dumpFKs = Utils.getPropBool(prop, PROP_SCHEMADUMP_FKs, dumpFKs);
		
		//dumpWriteAppend = Utils.getPropBool(prop, PROP_SCHEMADUMP_WRITEAPPEND, dumpWriteAppend);
		DBObject.dumpCreateOrReplace = dumpWithCreateOrReplace;
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);

		fromDbId = DBMSResources.instance().dbid();
		toDbId = prop.getProperty(Defs.PROP_TO_DB_ID);
		dumpFKsInsideTable = !doSchemaDumpFKsAtEnd;
		
		mainOutputFilePattern = Utils.getPropWithDeprecated(prop, PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, PROP_MAIN_OUTPUT_FILE_PATTERN, mainOutputFilePattern);
		if(mainOutputFilePattern==null) {
			outputConnPropPrefix = prop.getProperty(PROP_OUTPUT_CONN_PROP_PREFIX);
		}
		
		String outputobjectswithtable = Utils.getPropWithDeprecated(prop, PROP_SCHEMADUMP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE, PROP_OUTPUT_OBJECT_WITH_REFERENCING_TABLE, null);
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
		
		for(DBObjectType dbtype: DBObjectType.values()) {
			DBObjectType typeMappedTo = null;
			
			String typeMappedToStr = Utils.getPropWithDeprecated(prop, PREFIX_SCHEMADUMP_OUTPATTERN_MAPTYPE+dbtype.name(), PREFIX_OUTPATTERN_MAPTYPE+dbtype.name(), null);
			if(typeMappedToStr==null) { continue; }
			try {
				typeMappedTo = DBObjectType.valueOf(typeMappedToStr);
				mappingBetweenDBObjectTypes.put(dbtype, typeMappedTo);
			}
			catch(IllegalArgumentException e) {
				log.warn("unknown object type on property file: '"+typeMappedToStr+"'");
			}
		}
		
		this.prop = prop;
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
		
		if(outputConnPropPrefix!=null) {
			outputConn = ConnectionUtil.initDBConnection(outputConnPropPrefix, prop, false);
		}
		
		log.info("dumping schema..."
				+(toDbId!=null?" from '"+fromDbId+"' to '"+toDbId+"'":""));
		if(fromDbId==null) {
			log.info("fromDbId is null: no conversion");
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
		
		setupScriptDumpSpecificFeatures(toDbId!=null?toDbId:fromDbId);
		
		//XXX: order of objects within table: FK, index, grants? grant, fk, index?
		
		if(schemaModel==null || schemaModel.getTables()==null) {
			log.warn("no tables grabbed! terminating dumpSchema()");
			return;
		}
		
		for(Table table: schemaModel.getTables()) {
			switch(table.getType()) {
				case TABLE: 
				case BASE_TABLE: 
					break;
				case SYNONYM: if(dumpSynonymAsTable) { break; } else { continue; } 
				case VIEW: if(dumpViewAsTable) { break; } else { continue; }
				case MATERIALIZED_VIEW: if(dumpMaterializedViewAsTable) { break; } else { continue; }
				//XXX: other table types: EXTERNAL_TABLE(?), SYSTEM_TABLE, SYSTEM_VIEW, TYPE
				default: break;
			}
			
			categorizedOut(table.getSchemaName(), table.getName(), DBObjectType.TABLE, table.getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable && dumpFKs, dumpDropStatements, dumpScriptComments, colTypeConversionProp, schemaModel.getForeignKeys())+";\n");
			String afterTableScript = table.getAfterCreateTableScript(dumpWithSchemaName, dumpRemarks);
			if(afterTableScript!=null && !afterTableScript.trim().equals("")) {
				categorizedOut(table.getSchemaName(), table.getName(), DBObjectType.TABLE, afterTableScript);
			}
			
			//table end: ';'
			//categorizedOut(table.schemaName, table.name, DBObjectType.TABLE, ";\n");

			//FK outside table, with referencing table
			if(dumpFKsWithReferencingTable && !dumpFKsInsideTable && dumpFKs) {
				for(FK fk: schemaModel.getForeignKeys()) {
					if(fk.getFkTable().equals(table.getName())) {
						String fkscript = fk.fkScriptWithAlterTable(dumpDropStatements, dumpWithSchemaName);
						categorizedOut(table.getSchemaName(), table.getName(), DBObjectType.TABLE, fkscript);
					}
				}
			}
			
			//Indexes
			if(dumpIndexesWithReferencingTable) {
				for(Index idx: schemaModel.getIndexes()) {
					//option for index output inside table
					if(idx==null) {
						log.warn("index null? table: "+table);
						continue;
					}
					if(table.getName().equals(idx.getTableName())) {
						categorizedOut(idx.getSchemaName(), idx.getTableName(), DBObjectType.TABLE, idx.getDefinition(dumpWithSchemaName)+";\n");
					}
				}
			}

			String tableName = DBObject.getFinalName(table.getSchemaName(), table.getName(), dumpWithSchemaName);
			
			//Grants
			if(dumpGrantsWithReferencingTable) {
				String grantOutput = compactGrantDump(table.getGrants(), tableName, toDbId);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.getSchemaName(), table.getName(), DBObjectType.TABLE, grantOutput);
				}
			}
			
			//Triggers
			if(dumpTriggersWithReferencingTable) {
				for(Trigger tr: schemaModel.getTriggers()) {
					//option for trigger output inside table
					if(table.getName().equals(tr.getTableName())) {
						categorizedOut(tr.getSchemaName(), tr.getTableName(), DBObjectType.TABLE, tr.getDefinition(dumpWithSchemaName)+"\n");
					}
				}
			}
		}
		
		//FKs
		if(!dumpFKsInsideTable && !dumpFKsWithReferencingTable && dumpFKs) {
			dumpFKsOutsideTable(schemaModel.getForeignKeys());
		}
		
		//Views
		for(View v: schemaModel.getViews()) {
			categorizedOut(v.getSchemaName(), v.getName(), DBObjectType.VIEW, v.getDefinition(dumpWithSchemaName)+";\n");
		}

		//Triggers
		for(Trigger t: schemaModel.getTriggers()) {
			if(!dumpTriggersWithReferencingTable || t.getTableName()==null) {
				categorizedOut(t.getSchemaName(), t.getName(), DBObjectType.TRIGGER, t.getDefinition(dumpWithSchemaName)+"\n");
			}
		}

		//ExecutableObjects
		for(ExecutableObject eo: schemaModel.getExecutables()) {
			// TODOne categorizedOut(eo.schemaName, eo.name, DBObjectType.EXECUTABLE,
			if(eo.isDumpable()) {
				categorizedOut(eo.getSchemaName(), eo.getName(), eo.getType(), 
					"-- Executable: "+eo.getType()+" "+eo.getName()+"\n"
					+eo.getDefinition(dumpWithSchemaName)+"\n");
			}
			else {
				log.debug("executable with no body (not dumped) ["+eo.getSchemaName()+"."+eo.getName()+"::"+eo.getType()+" ; package="+eo.getPackageName()+"]");
			}
		}

		//Synonyms
		for(Synonym s: schemaModel.getSynonyms()) {
			categorizedOut(s.getSchemaName(), s.getName(), DBObjectType.SYNONYM, s.getDefinition(dumpWithSchemaName)+";\n");
		}

		//Indexes
		if(!dumpIndexesWithReferencingTable) {
			List<Index> li = new ArrayList<Index>(); 
			li.addAll(schemaModel.getIndexes());
			if(dumpIndexesSortedByTableName) {
				Collections.sort(li, new Index.ByTableNameComparator());
			}
			for(Index idx: li) {
				categorizedOut(idx.getSchemaName(), idx.getName(), DBObjectType.INDEX, idx.getDefinition(dumpWithSchemaName)+";\n");
			}
		}
		
		//Grants
		if(!dumpGrantsWithReferencingTable) {
			//tables
			for(Table table: schemaModel.getTables()) {
				String tableName = DBObject.getFinalName(table.getSchemaName(), table.getName(), dumpWithSchemaName);
				String grantOutput = compactGrantDump(table.getGrants(), tableName, toDbId);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(table.getSchemaName(), table.getName(), DBObjectType.GRANT, grantOutput);
				}
			}
			//executables
			//TODO: how to dump exec grants if 'dumpGrantsWithReferencingTable' is true?
			//XXX: compactGrantDump for Executable's Grants doesn't make sense, since there is only one type of privilege for Executables (EXECUTE) (for now?)
			for(ExecutableObject eo: schemaModel.getExecutables()) {
				String eoName = DBObject.getFinalName(eo.getSchemaName(), eo.getName(), dumpWithSchemaName);
				//log.debug("exec to dump grants: "+eoName+" garr: "+eo.grants);
				String grantOutput = compactGrantDump(eo.getGrants(), eoName, toDbId);
				if(grantOutput!=null && !"".equals(grantOutput)) {
					categorizedOut(eo.getSchemaName(), eo.getName(), DBObjectType.GRANT, grantOutput);
				}
			}
		}

		//Sequences
		for(Sequence s: schemaModel.getSequences()) {
			categorizedOut(s.getSchemaName(), s.getName(), DBObjectType.SEQUENCE, s.getDefinition(dumpWithSchemaName)+";\n");
		}

		//XXX: log: 'n' objects dumped...
		log.info("...schema dumped");
		
		}
		catch(Exception e) {
			log.error("error dumping schema: "+e);
			log.info("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
		finally {
			if(outputConnPropPrefix!=null && outputConn!=null) {
				try {
					outputConn.close();
				} catch (SQLException e) {
					log.warn("error closing connection '"+outputConn+"': "+e);
					log.debug("error closing connection '"+outputConn+"'", e);
				}
			}
		}
	}
	
	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		for(FK fk: foreignKeys) {
			String fkscript = fk.fkScriptWithAlterTable(dumpDropStatements, dumpWithSchemaName);
			categorizedOut(fk.getSchemaName(), fk.getName(), DBObjectType.FK, fkscript);
		}
	}
	
	Set<String> filesOpened = new TreeSet<String>();
	
	Set<String> warnedOldPatternFiles = new TreeSet<String>();
	
	void categorizedOut(String schemaName, String objectName, DBObjectType objectType, String message) throws IOException {

		//if output is connection
		if(outputConnPropPrefix!=null && outputConn!=null) {
			try {
				outputConn.createStatement().execute(message);
			} catch (SQLException e) {
				if(failonerror) {
					throw new ProcessingException("error executing sql: "+message,e);
				}
				log.warn("error executing sql [ex="+e+"]: "+message);
			}
			return;
		}
		
		DBObjectType mappedObjectType = mappingBetweenDBObjectTypes.get(objectType);
		if(mappedObjectType!=null) { objectType = mappedObjectType; }
		
		String outFilePattern = Utils.getPropWithDeprecated(prop, PREFIX_SCHEMADUMP_OUTPATTERN_BYTYPE+objectType.name(), PREFIX_OUTPATTERN_BYTYPE+objectType.name(), null);
		if(outFilePattern==null) {
			outFilePattern = mainOutputFilePattern;
		}
		if(outFilePattern==null) {
			throw new RuntimeException("output file patterns (e.g. '"+PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN+"') not defined, aborting");
		}
		
		//if 'static' writer
		Writer w = CategorizedOut.getStaticWriter(outFilePattern);
		if(w!=null) {
			w.write(message);
			w.write("\n");
			w.flush();
			return;
		}
		
		//objectName = objectName.replaceAll("\\$", "\\\\\\$");  //indeed strange but necessary if objectName contains "$". see Matcher.replaceAll
		if(schemaName==null) { schemaName = ""; }
		schemaName = Matcher.quoteReplacement(schemaName);
		objectName = Matcher.quoteReplacement(objectName);
		
		String outFileTmp = outFilePattern;
		String outFile = outFilePattern
			.replaceAll(FILENAME_PATTERN_SCHEMA_QUOTED, schemaName)
			.replaceAll(FILENAME_PATTERN_OBJECTTYPE_QUOTED, objectType.name())
			.replaceAll(FILENAME_PATTERN_OBJECTNAME_QUOTED, objectName);
		
		if(!outFileTmp.equals(outFile) && !warnedOldPatternFiles.contains(outFileTmp)) {
			warnedOldPatternFiles.add(outFileTmp);
			log.warn("using deprecated pattern '${xxx}': "
					+FILENAME_PATTERN_SCHEMA+", "+FILENAME_PATTERN_OBJECTTYPE+" or "+FILENAME_PATTERN_OBJECTNAME
					+" [filename="+outFileTmp+"]");
		}
		
		outFile = outFile
			.replaceAll(PATTERN_SCHEMANAME_QUOTED, schemaName)
			.replaceAll(PATTERN_OBJECTTYPE_QUOTED, objectType.name())
			.replaceAll(PATTERN_OBJECTNAME_QUOTED, objectName);
		
		boolean alreadyOpened = filesOpened.contains(outFile);
		if(!alreadyOpened) { filesOpened.add(outFile); }
		
		File f = new File(outFile);
		//String dirStr = f.getParent();
		File dir = f.getParentFile();
		if(dir!=null) {
		if(!dir.exists()) {
			dir.mkdirs();
		}
		else {
			if(!dir.isDirectory()) {
				throw new IOException(dir+" already exists and is not a directory");
			}
		}
		}
		FileWriter fos = new FileWriter(f, alreadyOpened); //if already opened, append; if not, create
		//XXX: remove '\n'?
		fos.write(message+"\n");
		fos.close();
	}

	/*
	@Deprecated
	String simpleGrantDump(Collection<Grant> grants, String finalTableName, String toDbId) {
		StringBuffer sb = new StringBuffer();
		
		for(Grant grant: grants) {
			sb.append("grant "+grant.privilege
					+" on "+finalTableName
					+" to "+grant.grantee
					+(grant.withGrantOption?" WITH GRANT OPTION":"")
					+";\n\n");
		}
		if(sb.length()>2) {
			return sb.substring(0, sb.length()-1);
		}
		return "";
	}
	*/
	
	static class PrivilegeWithColumns {
		final PrivilegeType priv;
		final TreeSet<String> columns = new TreeSet<String>();
		
		public PrivilegeWithColumns(PrivilegeType priv) {
			this.priv = priv;
		}
	}
	
	static String compactGrantDump(Collection<Grant> grants, String finalTableName, String toDbId) {
		Map<String, List<PrivilegeWithColumns>> mapWithGrant = new TreeMap<String, List<PrivilegeWithColumns>>();
		Map<String, List<PrivilegeWithColumns>> mapWOGrant = new TreeMap<String, List<PrivilegeWithColumns>>();
		
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
			if(toDbId!=null && !privsToDump.contains(g.getPrivilege().toString())) { continue; }
			
			if(g.isWithGrantOption()) {
				addGrantToMap(mapWithGrant, g);
			}
			else {
				addGrantToMap(mapWOGrant, g);
			}
		}
		
		StringBuilder sb = new StringBuilder();
		
		appendGrantDump(mapWithGrant, finalTableName, " WITH GRANT OPTION", sb);
		appendGrantDump(mapWOGrant, finalTableName, null, sb);
		
		if(sb.length()>2) {
			return sb.substring(0, sb.length()-1);
		}
		return "";
	}
	
	static void addGrantToMap(Map<String, List<PrivilegeWithColumns>> grantMap, Grant g) {
		List<PrivilegeWithColumns> privs = grantMap.get(g.getGrantee());
		if(privs==null) {
			privs = new ArrayList<PrivilegeWithColumns>();
			grantMap.put(g.getGrantee(), privs);
		}
		boolean added = false;
		for(int i=0;i<privs.size();i++) {
			if(privs.get(i).priv.equals(g.getPrivilege())) {
				if(g.getColumn()!=null) {
					privs.get(i).columns.add(g.getColumn());
				}
				added = true;
			}
		}
		if(!added) {
			PrivilegeWithColumns pwc = new PrivilegeWithColumns(g.getPrivilege());
			if(g.getColumn()!=null) {
				pwc.columns.add(g.getColumn());
			}
			privs.add(pwc);
		}
	}
	
	static void appendGrantDump(Map<String, List<PrivilegeWithColumns>> grantMap, String finalTableName, String footerString, StringBuilder sb) {
		for(Entry<String, List<PrivilegeWithColumns>> entry: grantMap.entrySet()) {
			List<PrivilegeWithColumns> privs = entry.getValue();
			StringBuilder sb2 = new StringBuilder();
			boolean is1st = true;
			for(PrivilegeWithColumns pwc: privs) {
				if(!is1st) {
					sb2.append(", ");
				}
				sb2.append(pwc.priv);
				if(pwc.columns.size()>0) {
					sb2.append(" ("+Utils.join(pwc.columns, ", ")+")");
				}
				is1st = false;
			}
			sb.append("grant "+sb2.toString()
					+" on "+finalTableName
					+" to "+entry.getKey()
					+(footerString!=null?footerString:"")
					+";\n\n");
			/*for(PrivilegeType priv: privs) {
				sb.append("grant "+priv
					+" on "+tableName
					+" to "+grantee
					+" WITH GRANT OPTION"+";\n");
			}*/
		}
		
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}

	//XXX: move to DBMSResources?
	static final String DBPROP_COLUMN_USEAUTOINCREMENT = "column.useautoincrement";
	
	static void setupScriptDumpSpecificFeatures(String toDbId) {
		Properties p = DBMSResources.instance().getProperties();
		if(Utils.getStringListFromProp(p, DBPROP_COLUMN_USEAUTOINCREMENT, ",").contains(toDbId)) {
			Column.useAutoIncrement = true;
		}
	}
	
	@Override
	public String getMimeType() {
		return SQL_MIME_TYPE;
	}

}
