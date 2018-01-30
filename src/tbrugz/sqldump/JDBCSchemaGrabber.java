package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.FK.UpdateRule;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.Relation;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ModelMetaData;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

class DBObjectId extends DBIdentifiable {
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return null;
	}
}

/*
 * TODOne: accept list of schemas to grab/dump
 * TODO: accept list of tables/objects to grab/dump, types of objects to grab/dump
 * XXX: performance optimization: grab (columns, FKs, ...) in bulk
 */
public class JDBCSchemaGrabber extends AbstractFailable implements SchemaModelGrabber {
	
	static final String PREFIX = "sqldump.schemagrab";
	
	// grabber properties
	static final String PROP_SCHEMAGRAB_TABLES = PREFIX+".tables";
	static final String PROP_SCHEMAGRAB_PKS = PREFIX+".pks";
	static final String PROP_SCHEMAGRAB_FKS = PREFIX+".fks";
	static final String PROP_SCHEMAGRAB_EXPORTEDFKS = PREFIX+".exportedfks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_PKS = "sqldump.doschemadump.pks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_FKS = "sqldump.doschemadump.fks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_EXPORTEDFKS = "sqldump.doschemadump.exportedfks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	public static final String PROP_SCHEMAGRAB_GRANTS = PREFIX+".grants";
	static final String PROP_SCHEMAGRAB_ALLGRANTS = PREFIX+".allgrants"; //XXX: xperimental
	static final String PROP_SCHEMAGRAB_INDEXES = PREFIX+".indexes";
	@Deprecated static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_SCHEMAGRAB_PROCEDURESANDFUNCTIONS = PREFIX+".proceduresandfunctions";
	@Deprecated static final String PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS = "sqldump.doschemadump.ignoretableswithzerocolumns";
	static final String PROP_SCHEMAGRAB_IGNORETABLESWITHZEROCOLUMNS = PREFIX+".ignoretableswithzerocolumns";
	static final String PROP_SCHEMAGRAB_SETCONNREADONLY = PREFIX+".setconnectionreadonly";
	static final String PROP_SCHEMAGRAB_METADATA = PREFIX+".metadata";
	
	static final String PROP_SCHEMAGRAB_RECURSIVEDUMP = PREFIX+".recursivegrabbasedonfks";
	static final String PROP_SCHEMAGRAB_RECURSIVEDUMP_DEEP = PREFIX+".recursivegrabbasedonfks.deep";
	static final String PROP_SCHEMAGRAB_RECURSIVEDUMP_MAXLEVEL = PREFIX+".recursivegrabbasedonfks.maxlevel";
	static final String PROP_SCHEMAGRAB_RECURSIVEDUMP_EXPORTEDFKS = PREFIX+".recursivegrabbasedonfks.exportedfks";
	
	@Deprecated static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP = "sqldump.doschemadump.recursivedumpbasedonfks";
	@Deprecated static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP = "sqldump.doschemadump.recursivedumpbasedonfks.deep";
	@Deprecated static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL = "sqldump.doschemadump.recursivedumpbasedonfks.maxlevel";
	@Deprecated static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS = "sqldump.doschemadump.recursivedumpbasedonfks.exportedfks";

	static final String PROP_SCHEMAGRAB_TABLEFILTER = PREFIX+".tablefilter";
	static final String PROP_SCHEMAGRAB_EXCLUDETABLES = PREFIX+".tablename.excludes";
	@Deprecated
	static final String PROP_SCHEMADUMP_EXCLUDETABLES = "sqldump.schemadump.tablename.excludes";
	static final String PROP_SCHEMAGRAB_EXCLUDEOBJECTS = PREFIX+".objectname.excludes";
	static final String PROP_SCHEMAGRAB_TABLETYPES = PREFIX+".tabletypes";

	// xtra/generic grabber properties
	static final String PROP_SCHEMAINFO_DOMAINTABLES = "sqldump.schemainfo.domaintables";
	
	static final String PROP_SCHEMAGRAB_DBSPECIFIC = PREFIX+".db-specific-features";
	@Deprecated
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";

	static final Log log = LogFactory.getLog(JDBCSchemaGrabber.class);
	
	static final String[] DEFAULT_SCHEMA_NAMES = {
		"public", // postgresql, h2, hsqldb
		"APP",    // derby
		"",       // 'schema-less' databases
		"Default" // neo4j
	};
	
	//XXX: schema names to ignore by default... information_schema, pg_catalog, ...
	
	static final String DBID_ORACLE = "oracle";
	
	Connection conn;
	
	//tables OK for data dump
	//public List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties propOriginal;
	DBMSFeatures feats = null;
	String grabberId;
	
	//Properties dbmsSpecificResource = new ParametrizedProperties();
	
	boolean doSchemaGrabTables = true, //TODOne: add prop for doSchemaGrabTables
			doSchemaGrabPKs = true, 
			doSchemaGrabFKs = true, 
			doSchemaGrabExportedFKs = false, 
			doSchemaGrabTableGrants = false,
			doGrabAllSchemaGrants = false, 
			doSchemaGrabIndexes = false,
			doSchemaGrabProceduresAndFunctions = true,
			doSchemaGrabDbSpecific = false,
			doSetConnectionReadOnly = false,
			doGrabMetadata = false;

	boolean ignoretableswithzerocolumns = true,
		recursivedump = false,
		deeprecursivedump = false,
		grabExportedFKsAlso = false;
	
	List<TableType> tableTypesToGrab = null;
	
	Long maxLevel = null;
	
	public JDBCSchemaGrabber() {
		initCounters();
	}
	
	@Override
	public void setProperties(Properties prop) {
		log.info(getIdDesc()+"init JDBCSchemaGrabber...");
		
		propOriginal = prop;
		//papp = prop;
		papp.putAll(prop);
		
		//init control vars
		doSchemaGrabTables = Utils.getPropBool(papp, PROP_SCHEMAGRAB_TABLES, doSchemaGrabTables);
		
		doSchemaGrabPKs = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMAGRAB_PKS, PROP_DO_SCHEMADUMP_PKS, doSchemaGrabPKs);
		doSchemaGrabFKs = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMAGRAB_FKS, PROP_DO_SCHEMADUMP_FKS, doSchemaGrabFKs);
		doSchemaGrabExportedFKs = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMAGRAB_EXPORTEDFKS, PROP_DO_SCHEMADUMP_EXPORTEDFKS, doSchemaGrabExportedFKs);
		doSchemaGrabTableGrants = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMAGRAB_GRANTS, PROP_DO_SCHEMADUMP_GRANTS, doSchemaGrabTableGrants);
		
		doGrabAllSchemaGrants = Utils.getPropBool(papp, PROP_SCHEMAGRAB_ALLGRANTS, doGrabAllSchemaGrants);
		
		doSchemaGrabIndexes = Utils.getPropBoolWithDeprecated(prop, PROP_SCHEMAGRAB_INDEXES, PROP_DO_SCHEMADUMP_INDEXES, doSchemaGrabIndexes);
		doSchemaGrabProceduresAndFunctions = Utils.getPropBool(papp, PROP_SCHEMAGRAB_PROCEDURESANDFUNCTIONS, doSchemaGrabProceduresAndFunctions);
		doSchemaGrabDbSpecific = Utils.getPropBoolWithDeprecated(papp, PROP_SCHEMAGRAB_DBSPECIFIC, PROP_DUMP_DBSPECIFIC, doSchemaGrabDbSpecific);
		
		ignoretableswithzerocolumns = Utils.getPropBoolWithDeprecated(papp, PROP_SCHEMAGRAB_IGNORETABLESWITHZEROCOLUMNS, PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS, ignoretableswithzerocolumns);
		
		recursivedump = Utils.getPropBoolWithDeprecated(papp, PROP_SCHEMAGRAB_RECURSIVEDUMP, PROP_DO_SCHEMADUMP_RECURSIVEDUMP, recursivedump);
		deeprecursivedump = Utils.getPropBoolWithDeprecated(papp, PROP_SCHEMAGRAB_RECURSIVEDUMP_DEEP, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP, deeprecursivedump);
		grabExportedFKsAlso = Utils.getPropBoolWithDeprecated(papp, PROP_SCHEMAGRAB_RECURSIVEDUMP_EXPORTEDFKS, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS, grabExportedFKsAlso);
		maxLevel = Utils.getPropLongWithDeprecated(papp, PROP_SCHEMAGRAB_RECURSIVEDUMP_MAXLEVEL, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL, maxLevel);
		
		doSetConnectionReadOnly = Utils.getPropBool(papp, PROP_SCHEMAGRAB_SETCONNREADONLY, doSetConnectionReadOnly);
		doGrabMetadata = Utils.getPropBool(papp, PROP_SCHEMAGRAB_METADATA, doSetConnectionReadOnly);
		
		List<String> tableTypesToGrabStr = Utils.getStringListFromProp(prop, PROP_SCHEMAGRAB_TABLETYPES, ",");
		if(tableTypesToGrabStr!=null) {
			tableTypesToGrab = new ArrayList<TableType>();
			for(String s: tableTypesToGrabStr) {
				try {
					TableType tt = TableType.valueOf(s);
					tableTypesToGrab.add(tt);
				}
				catch(IllegalArgumentException e) {
					log.warn("'"+s+"' is not a valid table type for grabbing tables (property '"+PROP_SCHEMAGRAB_TABLETYPES+"')");
				}
			}
		}

		/*try {
			dbmsSpecificResource.load(JDBCSchemaGrabber.class.getClassLoader().getResourceAsStream(SQLDump.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
	
	@Override
	public Connection getConnection() {
		return conn;
	}

	public void setConnection(Connection conn) {
		this.conn = conn;
		if(doSetConnectionReadOnly) {
			try {
				conn.setReadOnly(true);
			} catch (SQLException e) {
				log.warn("error setting props [readonly=true] for db connection");
				log.debug("stack...", e);
				try { conn.rollback(); }
				catch(SQLException ee) { log.warn("error in rollback(): "+ee.getMessage()); }
			}
		}
	}
	
	@Override
	public boolean needsConnection() {
		return true;
	}
	
	Map<TableType, Integer> tablesCountByTableType = new HashMap<TableType, Integer>();
	Map<DBObjectType, Integer> execCountByType = new HashMap<DBObjectType, Integer>();
	
	List<Pattern> excludeTableFilters; //XXX: remove as object property & add as grabRelations() parameter?
	
	void initFeatures(Connection conn) throws SQLException {
		feats = DBMSResources.instance().getSpecificFeatures(conn.getMetaData());
	}
	
	@Override
	public SchemaModel grabSchema() {
		try {
		
		/*if(Utils.getPropBool(papp, SQLDump.PROP_FROM_DB_ID_AUTODETECT)) {
			String dbid = detectDbId(conn.getMetaData());
			if(dbid!=null) {
				log.info("database type identifier: "+dbid);
				papp.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
				propOriginal.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
			}
			else { log.warn("can't detect database type"); }
		}*/

		//DBMSResources.instance().updateMetaData(conn.getMetaData(), true);
		//feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		//feats.procProperties(papp);
		initFeatures(conn);
		DatabaseMetaData dbmd = feats.getMetadataDecorator(conn.getMetaData());
		log.info("feats/metadata: "+feats+" / "+dbmd);
		ConnectionUtil.showDBInfo(conn.getMetaData());
		if(log.isInfoEnabled()) {
			List<String> catalogs = SQLUtils.getCatalogNames(dbmd);
			if(catalogs!=null && catalogs.size()>0) {
				log.info(getIdDesc()+"catalogs: "+catalogs);
			}
			//log.debug("schemas: "+SQLUtils.getSchemaNames(dbmd));
			//XXX: show current catalog/schema? maybe not: https://forums.oracle.com/thread/1097687
		}
		
		SchemaModel schemaModel = new SchemaModel();
		@SuppressWarnings("deprecation")
		String schemaPattern = Utils.getPropWithDeprecated(papp, Defs.PROP_SCHEMAGRAB_SCHEMANAMES, Defs.PROP_DUMPSCHEMAPATTERN, null);
		
		if(schemaPattern==null) {
			List<String> schemas = SQLUtils.getSchemaNames(dbmd);
			log.info(getIdDesc()+"schemaPattern not defined. schemas available: "+schemas);
			schemaPattern = Utils.getEqualIgnoreCaseFromList(schemas, papp.getProperty(SQLDump.CONN_PROPS_PREFIX + ConnectionUtil.SUFFIX_USER));
			boolean equalsUsername = false;
			if(schemaPattern!=null) { equalsUsername = true; }
			
			int counter = 0;
			while(schemaPattern==null && DEFAULT_SCHEMA_NAMES.length>counter) {
				schemaPattern = Utils.getEqualIgnoreCaseFromList(schemas, DEFAULT_SCHEMA_NAMES[counter]);
				if(schemaPattern!=null) { break; }
				counter++;
			}
			
			if(schemaPattern!=null) {
				log.info(getIdDesc()+"setting suggested schema: '"+schemaPattern+"'"
						+(equalsUsername?" (same as username)":"") );
				papp.setProperty(Defs.PROP_SCHEMAGRAB_SCHEMANAMES, schemaPattern);
				if(propOriginal!=null) {
					propOriginal.setProperty(Defs.PROP_SCHEMAGRAB_SCHEMANAMES, schemaPattern);
				}
			}
		}

		if(schemaPattern==null) {
			log.error("schema name undefined & no suggestion available, aborting...");
			if(failonerror) { throw new ProcessingException("schema name undefined & no suggestion available, aborting..."); }
			return null;
		}
		
		log.info(getIdDesc()+"schema grab... schema(s): '"+schemaPattern+"' [features: "+feats.getClass().getSimpleName()+"]");

		initCounters();
		
		//register exclude table filters
		excludeTableFilters = getExcludeFilters(papp, PROP_SCHEMADUMP_EXCLUDETABLES, "table");
		if(excludeTableFilters.size()>0) {
			log.warn("using deprecated '"+PROP_SCHEMADUMP_EXCLUDETABLES+"' properties - use '"+PROP_SCHEMAGRAB_EXCLUDETABLES+"' instead");
		}
		else {
			excludeTableFilters = getExcludeFilters(papp, PROP_SCHEMAGRAB_EXCLUDETABLES, "table");
		}
		List<Pattern> excludeObjectFilters = getExcludeFilters(papp, PROP_SCHEMAGRAB_EXCLUDEOBJECTS, "dbobject");
		
		String[] schemasArr = schemaPattern.split(",");
		List<String> schemasList = new ArrayList<String>();
		for(String schemaName: schemasArr) {
			schemasList.add(schemaName.trim());
		}
		
		//schemaModel.setSqlDialect(DBMSResources.instance().dbid());
		schemaModel.setSqlDialect(feats.getId());
		log.info("feats/metadata: "+feats+" / "+dbmd+ " / "+feats.getId());

		if(doSchemaGrabTables) {
			List<String> tablePatterns = Utils.getStringListFromProp(papp, PROP_SCHEMAGRAB_TABLEFILTER, ","); 
			for(String schemaName: schemasList) {
				if(tablePatterns==null) {
					grabRelations(schemaModel, dbmd, feats, schemaName, null, false);
				}
				else {
					for(String tableName: tablePatterns) {
						grabRelations(schemaModel, dbmd, feats, schemaName, tableName, false);
					}
				}
			}
		}
		
		//TODO!!: option to grab all FKs from schema...
		
		if(doSchemaGrabTables) {
			if(recursivedump) {
				int lastTableCount = schemaModel.getTables().size();
				int level = 0;
				log.info(getIdDesc()+"grabbing tables recursively["+level+"]: #ini:"+lastTableCount
						+(maxLevel!=null?" [maxlevel="+maxLevel+"]":" [maxlevel not defined]"));
				while(true) {
					level++;
					grabTablesRecursivebasedOnFKs(dbmd, feats, schemaModel, schemaPattern, grabExportedFKsAlso);
					
					int newTableCount = schemaModel.getTables().size();
					boolean wontGrowMore = (newTableCount <= lastTableCount);
					boolean maxLevelReached = (maxLevel!=null && level>=maxLevel);
					log.info(getIdDesc()+"grabbing tables recursively["+level+"]: #last:"+lastTableCount+" #now:"+newTableCount
							+(wontGrowMore?" [won't grow more]":"")
							+(maxLevelReached?" [maxlevel reached]":"")
							);
					if(wontGrowMore || maxLevelReached) { break; }
					lastTableCount = newTableCount;
				}
			}
		}
		
		log.info(getIdDesc()+schemaModel.getTables().size()+" tables grabbed ["+tableStats()+"]");
		if(doSchemaGrabFKs || doSchemaGrabExportedFKs || recursivedump) {
			log.info(getIdDesc()+schemaModel.getForeignKeys().size()+" FKs grabbed");
		}
		if(doSchemaGrabIndexes) {
			log.info(getIdDesc()+schemaModel.getIndexes().size()+" indexes grabbed");
		}
		//XXX show #grants grabbed?
		
		if(doSchemaGrabProceduresAndFunctions) {
			int countproc = 0, countfunc = 0;
			try {
				for(String schemaName: schemasList) {
					List<ExecutableObject> eos = doGrabProcedures(dbmd, schemaName, true);
					if(eos!=null) {
						countproc += eos.size();
						filterObjects(eos, excludeObjectFilters, "procedure");
						schemaModel.getExecutables().addAll(eos);
					}
				}
			}
			catch(LinkageError e) {
				log.warn("abstract method error: "+e);
			}
			catch(RuntimeException e) {
				log.warn(getIdDesc()+"runtime exception grabbing procedures: "+e);
			}
			catch(SQLException e) {
				log.warn(getIdDesc()+"sql exception grabbing procedures: "+e);
			}
			log.info(getIdDesc()+countproc+" procedures grabbed ["+executableStats()+"]");
			
			try {
				for(String schemaName: schemasList) {
					List<ExecutableObject> eos = doGrabFunctions(dbmd, schemaName, true);
					if(eos!=null) {
						countfunc += eos.size();
						filterObjects(eos, excludeObjectFilters, "function");
						schemaModel.getExecutables().addAll(eos);
					}
				}
			}
			catch(LinkageError e) {
				log.warn("abstract method error: "+e);
			}
			catch(RuntimeException e) {
				log.warn(getIdDesc()+"runtime exception grabbing functions: "+e);
			}
			catch(SQLException e) {
				// h2 & postgresql can't grab functions
				String warn = getIdDesc()+"sql exception grabbing functions: "+e;
				if(feats.getId().equals("h2") || feats.getId().equals("pgsql")) {
					log.debug(warn);
				}
				else {
					log.warn(warn);
				}
			}
			//XXX: add ["+executableStats()+"]?
			log.info(getIdDesc()+countfunc+" functions grabbed");
		}
		
		//XXX!!! schema GRANTs ? how/where to add in schemaModel?
		if(doGrabAllSchemaGrants) {
			for(String schemaName: schemasList) {
				log.info(getIdDesc()+"getting grants from schema "+schemaName);
				//XXX filter by GRANTEE, not GRANTOR/SCHEMA ? schema != user (in some databases?)
				//XXX no grants for 'role's? (tested on oracle)
				ResultSet grantrs = dbmd.getTablePrivileges(null, schemaName, null);
				//List<Grant> grants = grabSchemaGrants(grantrs); //table.setGrants(  );
				closeResultSetAndStatement(grantrs);
			}
			//log.info("getting grants from all schemas ");
			//ResultSet grantrs = dbmd.getTablePrivileges(null, null, null);
			//QueryDumper.simplerRSDump(grantrs);
			//closeResultSetAndStatement(grantrs);
		}
		
		//XXX: getClientInfoProperties?
		
		if(doSchemaGrabDbSpecific) {
			for(String schemaName: schemasList) {
				grabDbSpecific(schemaModel, schemaName);
			}

			//add constraints to views
			if(schemaModel.getViews().size()>0) {
				for(View view: schemaModel.getViews()) {
					//PKs
					
					if(doSchemaGrabPKs) { //XXX: add doSchemaGrabViewPKs?
						view.setConstraints(grabRelationPKs(dbmd, view));
					}

					//XXX: grab columns & remarks from dbmd...
					//Columns & Remarks
					Table t = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, view.getSchemaName(), view.getName());
					if(t==null) {
						log.debug("view not found in grabbed tables' list: "+view.getSchemaName()+"."+view.getName());
						continue;
					}
					view.setSimpleColumns(t.getColumns());
					view.setRemarks(t.getRemarks());
				}
			}
			dbSpecificLogs(schemaModel);
		}
		
		if(doGrabMetadata) {
			Map<String,String> meta = ModelMetaData.getProperties(dbmd);
			schemaModel.setMetadata(meta);
			log.debug("metadata grabbed: "+meta);
		}

		filterObjects(schemaModel.getExecutables(), excludeObjectFilters, "executable");
		filterObjects(schemaModel.getTriggers(), excludeObjectFilters, "trigger");
		filterObjects(schemaModel.getViews(), excludeObjectFilters, "view");
		//XXX: filter FKs, indexes, sequences, synonyms?
		
		return schemaModel;

		}
		catch(SQLException e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
	}
	
	synchronized void initCounters() {
		tablesCountByTableType.clear();
		for(TableType tt: TableType.values()) {
			tablesCountByTableType.put(tt, 0);
		}
		execCountByType.clear();
		for(DBObjectType t: DBObjectType.values()) {
			execCountByType.put(t, 0);
		}
	}
	
	String tableStats() {
		StringBuilder sb = new StringBuilder();
		int countTT = 0;
		for(TableType tt: tablesCountByTableType.keySet()) {
			int count = tablesCountByTableType.get(tt);
			if(count>0) {
				sb.append((countTT==0?"":", ")+"#"+tt+"s="+count);
				countTT++;
			}
		}
		return sb.toString();
	}
	
	String executableStats() {
		StringBuilder sb = new StringBuilder();
		int countT = 0;
		for(DBObjectType t: execCountByType.keySet()) {
			int count = execCountByType.get(t);
			if(count>0) {
				sb.append((countT==0?"":", ")+"#"+t+"s="+count);
				countT++;
			}
		}
		return sb.toString();
	}
	
	void dbSpecificLogs(SchemaModel schemaModel) {
		if(!doSchemaGrabIndexes && schemaModel.getIndexes().size()>0) { //XXX: really?
			log.info(getIdDesc()+schemaModel.getIndexes().size()+" indexes grabbed");
		}
		if(schemaModel.getViews().size()>0) {
			log.info(getIdDesc()+schemaModel.getViews().size()+" views grabbed");
		}
		if(schemaModel.getExecutables().size()>0) {
			Map<String, Integer> countMap = new TreeMap<String, Integer>();
			List<String> l = new ArrayList<String>();
			for(ExecutableObject eo: schemaModel.getExecutables()) {
				String type = eo.getType().toString();
				if(!eo.isDumpable()) { type += "(no-body)"; }
				
				Integer count = countMap.get(type);
				if(count==null) { count = 1; }
				else { count++; }
				countMap.put(type, count);
			}
			for(Entry<String, Integer> e: countMap.entrySet()) {
				l.add("#"+e.getKey()+"="+e.getValue());
			}
			
			log.info(getIdDesc()+schemaModel.getExecutables().size()+" executables grabbed ["+Utils.join(l, ", ")+"]");
		}
		if(schemaModel.getTriggers().size()>0) {
			log.info(getIdDesc()+schemaModel.getTriggers().size()+" triggers grabbed");
		}
		if(schemaModel.getSequences().size()>0) {
			log.info(getIdDesc()+schemaModel.getSequences().size()+" sequences grabbed");
		}
		if(schemaModel.getSynonyms().size()>0) {
			log.info(getIdDesc()+schemaModel.getSynonyms().size()+" synonyms grabbed");
		}
	}
	
	//XXX shoud it be "grabTables"?
	void grabRelations(SchemaModel schemaModel, DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, String schemaPattern, String tablePattern, boolean tableOnly) throws SQLException { //, String padding
		log.debug(getIdDesc()+"grabRelations()... schema: "+schemaPattern+", tablePattern: "+tablePattern);
		List<String> domainTables = Utils.getStringListFromProp(papp, PROP_SCHEMAINFO_DOMAINTABLES, ",");
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, tablePattern, null);

		while(rs.next()) {
			TableType ttype = null;
			String tableName = rs.getString("TABLE_NAME");
			String schemaName = rs.getString("TABLE_SCHEM");
			String catalogName = rs.getString("TABLE_CAT");
			
			//for MySQL
			if(schemaName==null && catalogName!=null) { schemaName = catalogName; }
			
			ttype = TableType.getTableType(rs.getString("TABLE_TYPE"), tableName);
			if(ttype==null) { continue; }
			
			if(tableTypesToGrab!=null) {
				if(!tableTypesToGrab.contains(ttype)) { continue; }
			}
			
			//test for filter exclusion
			boolean ignoreTable = false;
			for(Pattern p: excludeTableFilters) {
				if(p.matcher(tableName).matches()) {
					log.debug("ignoring table: "+tableName);
					ignoreTable = true; break;
				}
			}
			if(ignoreTable) { continue; }
			
			tablesCountByTableType.put(ttype, tablesCountByTableType.get(ttype)+1);
			
			//defining model
			Table table = dbmsfeatures.getTableObject();
			table.setName( tableName );
			table.setSchemaName(schemaName);
			table.setType(ttype);
			table.setRemarks(rs.getString("REMARKS"));
			if(domainTables!=null && domainTables.contains(table.getName())) {
				log.debug("domain table: "+table.getName());
				table.setDomainTable(true);
			}
			dbmsfeatures.addTableSpecificFeatures(table, rs);
			dbmsfeatures.addTableSpecificFeatures(table, conn);
			
			try {
				String fullTablename = table.getQualifiedName();
				log.debug("getting columns from "+fullTablename);

				//columns
				ResultSet cols = dbmd.getColumns(null, table.getSchemaName(), tableName, null);
				int numCol = 0;
				while(cols.next()) {
					Column c = retrieveColumn(cols);
					table.getColumns().add(c);
					dbmsfeatures.addColumnSpecificFeatures(c, cols);
					numCol++;
					//String colDesc = getColumnDesc(c, columnTypeMapping, papp.getProperty(PROP_FROM_DB_ID), papp.getProperty(PROP_TO_DB_ID));
				}
				closeResultSetAndStatement(cols);
				if(numCol==0) {
					log.warn("zero columns on table '"+fullTablename+"'? [ignored="+ignoretableswithzerocolumns+"]");
					if(ignoretableswithzerocolumns) { continue; }
				}
				
				//PKs
				if(doSchemaGrabPKs) {
					table.getConstraints().addAll(grabRelationPKs(dbmd, table));
				}

				//FKs
				if(!tableOnly || deeprecursivedump) {
					schemaModel.getForeignKeys().addAll(grabRelationFKs(dbmd, dbmsfeatures, table, doSchemaGrabFKs, doSchemaGrabExportedFKs));
				}
				
				//GRANTs
				if(doSchemaGrabTableGrants) {
					List<Grant> grants = new ArrayList<Grant>();;
					{
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, table.getSchemaName(), tableName);
					grants.addAll( grabSchemaGrants(grantrs, false) );
					closeResultSetAndStatement(grantrs);
					}

					{
					log.debug("getting column grants from "+fullTablename);
					ResultSet grantColRs = dbmd.getColumnPrivileges(null, table.getSchemaName(), tableName, null);
					grants.addAll( grabSchemaGrants(grantColRs, true) );
					closeResultSetAndStatement(grantColRs);
					}
					
					table.setGrants(grants);
				}
				
				//INDEXes
				if(doSchemaGrabIndexes && TableType.TABLE.equals(table.getType()) && !tableOnly) {
					log.debug("getting indexes from "+fullTablename);
					ResultSet indexesrs = dbmd.getIndexInfo(null, table.getSchemaName(), tableName, false, false);
					grabSchemaIndexes(indexesrs, schemaModel.getIndexes());
					closeResultSetAndStatement(indexesrs);
				}
				
				//tableNamesForDataDump.add(tableName);
			}
			catch(OutOfMemoryError oome) {
				log.error("OutOfMemoryError: memory: max: "+Runtime.getRuntime().maxMemory()+"; total: "+Runtime.getRuntime().totalMemory()+"; free: "+Runtime.getRuntime().freeMemory());
				throw oome;
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+tableName+" ["+sqle+"]");
				log.info("exception in table: "+tableName+" ["+sqle.getMessage()+"]", sqle);
				//tableNamesForDataDump.remove(tableName);
			}
			
			table.validateConstraints();

			schemaModel.getTables().add(table);
		}
		closeResultSetAndStatement(rs);
		
		/*if(recursivedump && (!tableOnly || deeprecursivedump)) {
			grabTablesRecursivebasedOnFKs(dbmd, dbmsfeatures, schemaModel, schemaPattern);
		}*/

		//log.debug("tables::["+schemaModel.tables.size()+"]\n"+schemaModel.tables+"\n");
		//log.debug("FKs::["+schemaModel.foreignKeys.size()+"]\n"+schemaModel.foreignKeys+"\n");

		/*
		if(doSchemaGrabDbSpecific && !tableOnly) {
			grabDbSpecific(schemaModel, schemaPattern);
		}*/
		
		//return schemaModel;
	}

	public static List<FK> grabRelationFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, Relation relation, boolean grabFKs, boolean grabExportedFKs) throws SQLException {
		String fullTablename = (relation.getSchemaName()==null?"":relation.getSchemaName()+".")+relation.getName();
		List<FK> ret = new ArrayList<FK>();
		
		//FKs
		if(grabFKs) {
			log.debug("getting FKs from "+fullTablename);
			ResultSet fkrs = dbmd.getImportedKeys(null, relation.getSchemaName(), relation.getName());
			ret.addAll(grabSchemaFKs(fkrs, dbmsfeatures));
			closeResultSetAndStatement(fkrs);
		}

		//FKs "exported"
		if(grabExportedFKs) {
			log.debug("getting 'exported' FKs from "+fullTablename);
			ResultSet fkrs = dbmd.getExportedKeys(null, relation.getSchemaName(), relation.getName());
			ret.addAll(grabSchemaFKs(fkrs, dbmsfeatures));
			closeResultSetAndStatement(fkrs);
		}
		
		return ret;
	}

	public static List<Constraint> grabRelationPKs(DatabaseMetaData dbmd, Relation relation) throws SQLException {
		//String fullTablename = (relation.getSchemaName()==null?"":relation.getSchemaName()+".")+relation.getName();
		List<Constraint> ret = new ArrayList<Constraint>();
		
			//log.debug("getting PKs from "+fullTablename);
			ResultSet pks = dbmd.getPrimaryKeys(null, relation.getSchemaName(), relation.getName());
			Constraint pk = grabSchemaPKs(pks, relation);
			if(pk!=null) {
				ret.add(pk);
				//relation.getConstraints().add(pk);
			}
			closeResultSetAndStatement(pks);

		return ret;
	}
	
	void grabTablesRecursivebasedOnFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, SchemaModel schemaModel, String schemaPattern, boolean grabExportedFKsAlso) throws SQLException { //, String padding
		log.debug("recursivegrab: "+schemaPattern);
		Set<DBObjectId> ids = new HashSet<DBObjectId>();
		for(FK fk: schemaModel.getForeignKeys()) {
			DBObjectId dbid = new DBObjectId();
			dbid.setName(fk.getPkTable());
			dbid.setSchemaName(fk.getPkTableSchemaName());
			ids.add(dbid);
	
			//Exported FKs
			if(grabExportedFKsAlso) {
				DBObjectId dbidFk = new DBObjectId();
				dbidFk.setName(fk.getFkTable());
				dbidFk.setSchemaName(fk.getFkTableSchemaName());
				ids.add(dbidFk);
			}
			
			/*if(!schemaPattern.equals(fk.pkTableSchemaName) && !containsTableWithSchemaAndName(schemaModel.tables, fk.pkTableSchemaName, fk.pkTable)) {
				log.warn("recursivegrab-grabschema: "+fk.pkTableSchemaName+"."+fk.pkTable);
				grabSchema(dbmd, fk.pkTableSchemaName, fk.pkTable, true);				
			}*/
		}
		for(DBObjectId id: ids) {
			//if(!schemaPattern.equals(id.schemaName) && !containsTableWithSchemaAndName(schemaModel.tables, id.schemaName, id.name)) {
			if(!containsTableWithSchemaAndName(schemaModel.getTables(), id.getSchemaName(), id.getName())) {
				log.debug("recursivegrab-grabschema: "+id.getSchemaName()+"."+id.getName());
				grabRelations(schemaModel, dbmd, dbmsfeatures, id.getSchemaName(), id.getName(), true);
			}
		}
	}
	
	private static boolean containsTableWithSchemaAndName(Set<Table> tables, String schemaName, String tableName) {
		for(Table t: tables) {
			if(t.getName().equals(tableName) && t.getSchemaName().equals(schemaName)) { return true; }
		}
		return false;
	}
	
	void grabDbSpecific(SchemaModel model, String schemaPattern) throws SQLException {
		//DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		if(feats!=null) { feats.grabDBObjects(model, schemaPattern, conn); }
	}

	public List<ExecutableObject> doGrabProcedures(DatabaseMetaData dbmd, String schemaPattern, boolean grabParams) throws SQLException {
		List<ExecutableObject> eos = null;
		if(feats==null) { initFeatures(dbmd.getConnection()); }

		ResultSet rsProc = dbmd.getProcedures(null, schemaPattern, null);
		eos = grabProcedures(rsProc);
		closeResultSetAndStatement(rsProc);
		if(grabParams) {
			ResultSet rsProcCols = dbmd.getProcedureColumns(null, schemaPattern, null, null);
			grabProceduresColumns(eos, rsProcCols, feats.getId());
			closeResultSetAndStatement(rsProcCols);
		}
		return eos;
	}
	
	public List<ExecutableObject> doGrabFunctions(DatabaseMetaData dbmd, String schemaPattern, boolean grabParams) throws SQLException {
		List<ExecutableObject> eos = null;
		ResultSet rsFunc = dbmd.getFunctions(null, schemaPattern, null);
		eos = grabFunctions(rsFunc);
		closeResultSetAndStatement(rsFunc);
		if(grabParams) {
			ResultSet rsFuncCols = dbmd.getFunctionColumns(null, schemaPattern, null, null);
			grabFunctionsColumns(eos, rsFuncCols, feats.getId());
			closeResultSetAndStatement(rsFuncCols);
		}
		return eos;
	}
	
	List<ExecutableObject> grabProcedures(ResultSet rs) throws SQLException {
		List<ExecutableObject> eos = new ArrayList<ExecutableObject>();
		while(rs.next()) {
			ExecutableObject eo = new ExecutableObject();
			eo.setName(rs.getString("PROCEDURE_NAME"));
			eo.setSchemaName(rs.getString("PROCEDURE_SCHEM"));
			if( DBID_ORACLE.equals(feats.getId()) ) {
				eo.setPackageName(rs.getString("PROCEDURE_CAT")); //?? Oracle-only? Not good on H2... XXX: Test if DB has packages?
			}
			eo.setRemarks(rs.getString("REMARKS"));
			
			int type = rs.getInt("PROCEDURE_TYPE");
			switch (type) {
			case DatabaseMetaData.procedureReturnsResult:
				eo.setType(DBObjectType.FUNCTION);
				break;
			default:
				eo.setType(DBObjectType.PROCEDURE);
				break;
			}
			//XXX: SPECIFIC_NAME?
			
			eos.add(eo);
			execCountByType.put(eo.getType(), execCountByType.get(eo.getType())+1);
		}
		return eos;
	}

	public static void grabProceduresColumns(List<ExecutableObject> eos, ResultSet rs, String dbid) throws SQLException {
		while(rs.next()) {
			ExecutableParameter ep = new ExecutableParameter();
			ep.setName(rs.getString("COLUMN_NAME"));
			ep.setDataType(rs.getString("TYPE_NAME"));
			
			int type = rs.getInt("COLUMN_TYPE");
			switch (type) {
			case DatabaseMetaData.procedureColumnIn:
				ep.setInout(ExecutableParameter.INOUT.IN);
				break;
			case DatabaseMetaData.procedureColumnInOut:
				ep.setInout(ExecutableParameter.INOUT.INOUT);
				break;
			case DatabaseMetaData.procedureColumnOut:
				ep.setInout(ExecutableParameter.INOUT.OUT);
				break;
			default:
				break;
			}
			
			try {
				ep.setPosition(rs.getInt("ORDINAL_POSITION"));
			}
			catch(SQLException e) {
				try {
					ep.setPosition(rs.getInt("SEQUENCE")); //XXX: oracle-only?
				}
				catch(SQLException e2) {
					log.warn("column name for procedure parameter ordinal position not found: "+e);
					log.debug("grabProceduresColumns: columns available: "+SQLUtils.getColumnNames(rs.getMetaData()));
				}
			}
			
			String pName = rs.getString("PROCEDURE_NAME");
			String pSchem = rs.getString("PROCEDURE_SCHEM");
			ExecutableObject eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.PROCEDURE, pSchem, pName);
			if(eo==null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.FUNCTION, pSchem, pName);
				//XXX: oracle driver adds 1 to function parameter positions...
				if( DBID_ORACLE.equals(dbid) ) {
					ep.setPosition(ep.getPosition()-1);
				}
			}
			if(eo==null) {
				log.warn("procedure '"+pSchem+"."+pName+"' not found in "+eos);
				return;
			}
			
			if(ep.getPosition()==0) { 
				eo.setReturnParam(ep);
			}
			else {
				if(eo.getParams()==null) {
					eo.setParams(new ArrayList<ExecutableParameter>());
				}
				eo.getParams().add(ep);
			}
		}
	}

	List<ExecutableObject> grabFunctions(ResultSet rs) throws SQLException {
		//SQLUtils.dumpRS(rs);
		List<ExecutableObject> eos = new ArrayList<ExecutableObject>();
		while(rs.next()) {
			ExecutableObject eo = new ExecutableObject();
			eo.setName(rs.getString("FUNCTION_NAME"));
			eo.setSchemaName(rs.getString("FUNCTION_SCHEM"));
			eo.setPackageName(rs.getString("FUNCTION_CAT"));
			eo.setRemarks(rs.getString("REMARKS"));
			
			eo.setType(DBObjectType.FUNCTION);
			/*int type = rs.getInt("FUNCTION_TYPE");
			switch (type) {
			case DatabaseMetaData.functionReturnsTable:
			case DatabaseMetaData.functionNoTable:
			case DatabaseMetaData.functionResultUnknown:
			}*/
			//XXX: SPECIFIC_NAME?
			
			eos.add(eo);
		}
		return eos;
	}

	public static void grabFunctionsColumns(List<ExecutableObject> eos, ResultSet rs, String dbid) throws SQLException {
		SQLException sqlex = null;
		while(rs.next()) {
			ExecutableParameter ep = new ExecutableParameter();
			int type = Integer.MIN_VALUE;
			try {
				ep.setName(rs.getString("COLUMN_NAME"));
				type = rs.getInt("COLUMN_TYPE");
			}
			catch(SQLException e) { //db2
				ep.setName(rs.getString("PARAMETER_NAME"));
				type = rs.getInt("PARAMETER_TYPE");
				if(sqlex==null) {
					log.warn("grabFunctionsColumns: sqlex: "+e);
					log.info("grabFunctionsColumns: columns: "+SQLUtils.getColumnNames(rs.getMetaData()));
					sqlex = e;
				}
			}
			ep.setDataType(rs.getString("TYPE_NAME"));
			
			switch (type) {
			case DatabaseMetaData.functionColumnIn:
				ep.setInout(ExecutableParameter.INOUT.IN);
				break;
			case DatabaseMetaData.functionColumnInOut:
				ep.setInout(ExecutableParameter.INOUT.INOUT);
				break;
			case DatabaseMetaData.functionColumnOut:
				ep.setInout(ExecutableParameter.INOUT.OUT);
				break;
			case DatabaseMetaData.functionColumnResult: //??
			case DatabaseMetaData.functionColumnUnknown:
			default:
				break;
			}
			
			try {
				ep.setPosition(rs.getInt("ORDINAL_POSITION"));
			}
			catch(SQLException e) {
				log.warn("unknown column name for function parameter ordinal position: "+e);
			}
			
			String pName = rs.getString("FUNCTION_NAME"); //XXX: postgresql error?
			String pSchem = rs.getString("FUNCTION_SCHEM");
			ExecutableObject eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.FUNCTION, pSchem, pName);
			if(eo==null) {
				log.warn("function '"+pSchem+"."+pName+"' not found in "+eos);
				return;
			}

			if(ep.getPosition()==0) {
				eo.setReturnParam(ep);
			}
			else {
				if(eo.getParams()==null) {
					eo.setParams(new ArrayList<ExecutableParameter>());
				}
				eo.getParams().add(ep);
			}
		}
	}
	
	static boolean grabColumnIsAutoincrement = true;
	
	public static Column retrieveColumn(ResultSet cols) throws SQLException {
		Column c = new Column();
		c.setName( cols.getString("COLUMN_NAME") );
		c.setType(cols.getString("TYPE_NAME"));
		c.setNullable("YES".equals(cols.getString("IS_NULLABLE")));
		Object columnSize = cols.getObject("COLUMN_SIZE");
		if(columnSize!=null) {
			int icolumnSize = ((Number) columnSize).intValue();
			c.setColumSize(icolumnSize);
		}
		c.setOrdinalPosition(cols.getInt("ORDINAL_POSITION"));
		c.setRemarks(cols.getString("REMARKS"));
		if(grabColumnIsAutoincrement) {
			boolean autoInc = false;
			try {
				autoInc = "YES".equals( cols.getString("IS_AUTOINCREMENT") );
				if(autoInc) {
					//log.info("autoIncrement...["+c.getName()+"]: "+cols.getString("IS_AUTOINCREMENT"));
					c.setAutoIncrement(true);
				}
			}
			catch(Exception e) {
				grabColumnIsAutoincrement = false;
				log.warn("DatabaseMetaData.getColumns(): column 'IS_AUTOINCREMENT' not available");
			}
		}
		Object decimalDigits = cols.getObject("DECIMAL_DIGITS");
		if(decimalDigits!=null) {
			int iDecimalDigits = ((Number) decimalDigits).intValue();
			if(iDecimalDigits!=0) {
				c.setDecimalDigits(iDecimalDigits);
			}
		}
		return c;
	}

	Set<String> unknownPrivilegesWarned = new HashSet<String>();
	
	@Deprecated
	public List<Grant> grabSchemaGrants(ResultSet grantrs) throws SQLException {
		return grabSchemaGrants(grantrs, false);
	}
	
	public List<Grant> grabSchemaGrants(ResultSet grantrs, boolean grabColumn) throws SQLException {
		List<Grant> grantsList = new ArrayList<Grant>();
		String privilege = null;
		while(grantrs.next()) {
			try {
				Grant grant = new Grant();
				
				grant.setGrantee(grantrs.getString("GRANTEE"));
				privilege = Utils.normalizeEnumStringConstant(grantrs.getString("PRIVILEGE"));
				grant.setPrivilege(PrivilegeType.valueOf(privilege));
				grant.setTable(grantrs.getString("TABLE_NAME"));
				if(grabColumn) {
					grant.setColumn(grantrs.getString("COLUMN_NAME"));
				}
				grant.setWithGrantOption("YES".equals(grantrs.getString("IS_GRANTABLE")));
				grantsList.add(grant);
			}
			catch(IllegalArgumentException iae) {
				if(!unknownPrivilegesWarned.contains(privilege)) {
					log.warn("unknown privilege: "+privilege+" [ex: "+iae+"]");
					unknownPrivilegesWarned.add(privilege);
				}
			}
		}
		return grantsList;
	}
	
	static Constraint grabSchemaPKs(ResultSet pks, Relation relation) throws SQLException {
		Map<Integer, String> pkCols = new TreeMap<Integer, String>();
		String pkName = null;
		
		int count=0;
		while(pks.next()) {
			pkName = pks.getString("PK_NAME");
			if(pkName==null || pkName.equals("PRIMARY")) { //equals("PRIMARY"): for MySQL
				pkName = SQLUtils.newNameFromTableName(relation.getName(), SQLUtils.pkNamePattern);
			}
			pkCols.put(pks.getInt("KEY_SEQ"), pks.getString("COLUMN_NAME"));
			count++;
		}
		if(count==0) { return null; }//no PK

		Constraint cPK = new Constraint();
		cPK.setType(ConstraintType.PK);
		cPK.setName(pkName);
		cPK.getUniqueColumns().addAll( pkCols.values() );
		return cPK;
	}

	public static List<FK> grabSchemaFKs(ResultSet fkrs, DBMSFeatures dbmsfeatures) throws SQLException {
		Map<String, FK> fks = new HashMap<String, FK>();
		int count=0;
		boolean askForUkType = true;
		while(fkrs.next()) {
			//log.debug("FK!!!");
			String fkName = fkrs.getString("FK_NAME");
			String fkTableName = fkrs.getString("FKTABLE_NAME");
			String pkTableName = fkrs.getString("PKTABLE_NAME");
			if(fkName==null) {
				log.warn("nameless FK: "+fkTableName+"->"+pkTableName);
				fkName = newFKName(fkTableName, pkTableName, count);
				count++;
			}
			FK fk = fks.get(fkName);
			if(fk==null) {
				fk = dbmsfeatures.getForeignKeyObject();
				fk.setName(fkName);
				fks.put(fkName, fk);
			}
			if(fk.getPkTable()==null) {
				fk.setPkTable(pkTableName);
				fk.setFkTable(fkTableName);
				fk.setPkTableSchemaName(fkrs.getString("PKTABLE_SCHEM"));
				fk.setFkTableSchemaName(fkrs.getString("FKTABLE_SCHEM"));
				String pkTableCatalog = fkrs.getString("PKTABLE_CAT");
				String fkTableCatalog = fkrs.getString("FKTABLE_CAT");
				String updateRuleStr = fkrs.getString("UPDATE_RULE");
				int updateRule = -1;
				if(updateRuleStr!=null) {
					updateRule = fkrs.getInt("UPDATE_RULE");
				}
				int deleteRule = fkrs.getInt("DELETE_RULE");
				fk.setUpdateRule(UpdateRule.getUpdateRule(updateRule));
				fk.setDeleteRule(UpdateRule.getUpdateRule(deleteRule));
				//log.info("fk: "+fkName+" :: rules: "+updateRule+"|"+fk.updateRule+" / "+deleteRule+"|"+fk.deleteRule+"");
				
				//for MySQL
				if(fk.getPkTableSchemaName()==null && pkTableCatalog!=null) { fk.setPkTableSchemaName(pkTableCatalog); }
				if(fk.getFkTableSchemaName()==null && fkTableCatalog!=null) { fk.setFkTableSchemaName(fkTableCatalog); }
				
				if(askForUkType) {
					try {
						fk.setFkReferencesPK("P".equals(fkrs.getString("UK_CONSTRAINT_TYPE")));
					}
					catch(SQLException e) {
						askForUkType = false;
						log.debug("resultset has no 'UK_CONSTRAINT_TYPE' column [fkTable='"+fk.getFkTable()+"'; ukTable='"+fk.getPkTable()+"']");
					}
				}
				dbmsfeatures.addFKSpecificFeatures(fk, fkrs);
			}
			String fkcol = fkrs.getString("FKCOLUMN_NAME");
			String pkcol = fkrs.getString("PKCOLUMN_NAME");
			fk.getFkColumns().add(fkcol);
			fk.getPkColumns().add(pkcol);
			
			log.debug("fk: "+fkName+" - "+fk+" / fkcol:"+fkcol+" / pkcol:"+pkcol);
		}
		List<FK> ret = new ArrayList<FK>();
		for(Entry<String, FK> entry: fks.entrySet()) {
			ret.add(entry.getValue());
		}
		return ret;
	}
	
	public static void grabSchemaIndexes(ResultSet indexesrs, Collection<Index> indexes) throws SQLException {
		Index idx = null;
		
		while(indexesrs.next()) {
			String idxName = indexesrs.getString("INDEX_NAME");
			log.debug("index: "+idxName);
			if(idxName==null) {
				 //each table appears to have a no-name index, maybe "oracle-only"...
				log.debug("nameless index: "+indexesrs.getString("TABLE_NAME"));
				continue; 
			}
			if(idx==null || !idxName.equals(idx.getName())) {
				//end last object
				if(idx!=null) {
					indexes.add(idx);
				}
				//new object
				idx = new Index();
				idx.setName(idxName);
				boolean bNonUnique = indexesrs.getBoolean("NON_UNIQUE");
				idx.setUnique(!bNonUnique);
				
				idx.setSchemaName( indexesrs.getString("TABLE_SCHEM") );
				idx.setTableName( indexesrs.getString("TABLE_NAME") );
				String catName = indexesrs.getString("TABLE_CAT");

				//for MySQL
				if(idx.getSchemaName()==null && catName!=null) { idx.setSchemaName( catName ); }
				if(idx.getName().equals("PRIMARY")) {
					idx.setName( SQLUtils.newNameFromTableName(idx.getTableName(), SQLUtils.pkiNamePattern) );
				}
				
			}
			idx.getColumns().add(indexesrs.getString("COLUMN_NAME"));
		}
		if(idx!=null) {
			indexes.add(idx);
		}
	}
	
	public static void closeResultSetAndStatement(ResultSet rs) {
		try {
			if(rs!=null) {
				/*try {
					if(rs.getStatement()!=null && !rs.getStatement().isClosed()) {
						rs.getStatement().close();
					}
				}
				catch(Throwable e) { // AbstractMethodError?
					log.debug("Error on closeResultSetAndStatement: "+e, e);
				}
				
				try {
					if(!rs.isClosed()) {
						rs.close();
					}
				}
				catch(Throwable e) { // AbstractMethodError?
					log.debug("Error on closeResultSetAndStatement: "+e, e);
					rs.close();
				}*/
				if(rs.getStatement()!=null) {
					rs.getStatement().close();
 				}
				rs.close();
			}
		} catch (UnsupportedOperationException e) {
			log.warn("Error closing resultset or statement: "+e);
			//log.debug("Error closing resultset or statement: "+e.getMessage(), e);
		} catch (SQLException e) {
			log.warn("Error closing resultset or statement: "+e);
			log.debug("Error closing resultset or statement: "+e.getMessage(), e);
		}
	}
	
	static List<Pattern> getExcludeFilters(Properties prop, String propKey, String objectType) {
		List<Pattern> excludeFilters = new ArrayList<Pattern>();
		//int count = 1;
		String filter = prop.getProperty(propKey);
		if(filter==null) { return excludeFilters; }
		String[] excludes = filter.split("\\|");
		for(String ex: excludes) {
			String pattern = ex.trim();
			log.info("added "+objectType+" ignore filter: "+pattern);
			excludeFilters.add(Pattern.compile(pattern));
			//count++;
		}
		return excludeFilters;
	}
	
	static String newFKName(String fkTable, String pkTable, int count) {
		return fkTable.replaceAll(" ", "_")+"_"+count+"_FK";
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}
	
	static void filterObjects(Collection<? extends DBIdentifiable> list, List<Pattern> excludeFilters, String objectType) {
		if(excludeFilters==null) { return; }
		
		Iterator<? extends DBIdentifiable> i = list.iterator();
		
		loop_dbi:
		//for(int i=list.size()-1;i>=0;i--) {
		//for(DBIdentifiable dbi: list) {
		while(i.hasNext()) {
			//DBIdentifiable dbi = list.get(i);
			DBIdentifiable dbi = i.next();
			for(Pattern p: excludeFilters) {
				if(p.matcher(dbi.getName()).matches()) {
					//boolean removed = list.remove(dbi);
					i.remove();
					log.info("ignoring "+objectType+": "+DBObject.getFinalName(dbi.getSchemaName(), dbi.getName(), true));//+" [removed="+removed+"]");
					continue loop_dbi;
				}
			}
		}
	}
	
	
	@Override
	public void setId(String grabberId) {
		this.grabberId = grabberId;
	}
	
	String getIdDesc() {
		return grabberId!=null?"["+grabberId+"] ":"";
	}
	
}
