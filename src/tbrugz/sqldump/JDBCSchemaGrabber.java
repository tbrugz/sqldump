package tbrugz.sqldump;

import java.util.*;
import java.util.regex.Pattern;
import java.sql.*;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
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
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.ConnectionUtil;
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
	
	//sqldump.properties
	static final String PROP_SCHEMAGRAB_TABLES = "sqldump.schemagrab.tables";
	static final String PROP_SCHEMAGRAB_PKS = "sqldump.schemagrab.pks";
	static final String PROP_SCHEMAGRAB_FKS = "sqldump.schemagrab.fks";
	static final String PROP_SCHEMAGRAB_EXPORTEDFKS = "sqldump.schemagrab.exportedfks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_PKS = "sqldump.doschemadump.pks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_FKS = "sqldump.doschemadump.fks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_EXPORTEDFKS = "sqldump.doschemadump.exportedfks";
	@Deprecated
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	static final String PROP_SCHEMAGRAB_GRANTS = "sqldump.schemagrab.grants";
	static final String PROP_SCHEMAGRAB_ALLGRANTS = "sqldump.schemagrab.allgrants"; //XXX: xperimental
	static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_SCHEMAGRAB_PROCEDURESANDFUNCTIONS = "sqldump.schemagrab.proceduresandfunctions";
	static final String PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS = "sqldump.doschemadump.ignoretableswithzerocolumns";
	static final String PROP_SCHEMAGRAB_SETCONNREADONLY = "sqldump.schemagrab.setconnectionreadonly";
	
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP = "sqldump.doschemadump.recursivedumpbasedonfks";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP = "sqldump.doschemadump.recursivedumpbasedonfks.deep";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL = "sqldump.doschemadump.recursivedumpbasedonfks.maxlevel";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS = "sqldump.doschemadump.recursivedumpbasedonfks.exportedfks";

	static final String PROP_SCHEMADUMP_DOMAINTABLES = "sqldump.schemainfo.domaintables";
	static final String PROP_SCHEMADUMP_TABLEFILTER = "sqldump.schemagrab.tablefilter";
	static final String PROP_SCHEMAGRAB_EXCLUDETABLES = "sqldump.schemagrab.tablename.excludes";
	@Deprecated
	static final String PROP_SCHEMADUMP_EXCLUDETABLES = "sqldump.schemadump.tablename.excludes";
	static final String PROP_SCHEMAGRAB_EXCLUDEOBJECTS = "sqldump.schemagrab.objectname.excludes";
	static final String PROP_SCHEMAGRAB_TABLETYPES = "sqldump.schemagrab.tabletypes";
	
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";

	static Log log = LogFactory.getLog(JDBCSchemaGrabber.class);
	
	static String[] DEFAULT_SCHEMA_NAMES = {
		"public", //postgresql, h2, hsqldb
		"APP",    //derby
		"",       //'schema-less' databases
	};
	
	Connection conn;
	
	//tables OK for data dump
	//public List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties propOriginal;
	DBMSFeatures feats = null;
	
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
			doSetConnectionReadOnly = false;
	
	List<TableType> tableTypesToGrab = null;
	
	Long maxLevel = null;
	
	@Override
	public void setProperties(Properties prop) {
		log.info("init JDBCSchemaGrabber...");
		
		propOriginal = prop;
		//papp = prop;
		papp.putAll(prop);
		
		//inicializa variaveis controle
		doSchemaGrabTables = Utils.getPropBool(papp, PROP_SCHEMAGRAB_TABLES, doSchemaGrabTables);
		
		if(Utils.propertyExists(prop, PROP_DO_SCHEMADUMP_PKS)) {
			log.warn("using deprecated prop '"+PROP_DO_SCHEMADUMP_PKS+"' - use '"+PROP_SCHEMAGRAB_PKS+"' instead");
			doSchemaGrabPKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_PKS, doSchemaGrabPKs);
		}
		doSchemaGrabPKs = Utils.getPropBool(papp, PROP_SCHEMAGRAB_PKS, doSchemaGrabPKs);
		
		if(Utils.propertyExists(prop, PROP_DO_SCHEMADUMP_FKS)) {
			log.warn("using deprecated prop '"+PROP_DO_SCHEMADUMP_FKS+"' - use '"+PROP_SCHEMAGRAB_FKS+"' instead");
			doSchemaGrabFKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_FKS, doSchemaGrabFKs);
		}
		doSchemaGrabFKs = Utils.getPropBool(papp, PROP_SCHEMAGRAB_FKS, doSchemaGrabFKs);
		
		if(Utils.propertyExists(prop, PROP_DO_SCHEMADUMP_EXPORTEDFKS)) {
			log.warn("using deprecated prop '"+PROP_DO_SCHEMADUMP_EXPORTEDFKS+"' - use '"+PROP_SCHEMAGRAB_EXPORTEDFKS+"' instead");
			doSchemaGrabExportedFKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_EXPORTEDFKS, doSchemaGrabExportedFKs);
		}
		doSchemaGrabExportedFKs = Utils.getPropBool(papp, PROP_SCHEMAGRAB_EXPORTEDFKS, doSchemaGrabExportedFKs);
		
		if(Utils.propertyExists(prop, PROP_DO_SCHEMADUMP_GRANTS)) {
			log.warn("using deprecated prop '"+PROP_DO_SCHEMADUMP_GRANTS+"' - use '"+PROP_SCHEMAGRAB_GRANTS+"' instead");
			doSchemaGrabTableGrants = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_GRANTS, doSchemaGrabTableGrants);
		}
		doSchemaGrabTableGrants = Utils.getPropBool(papp, PROP_SCHEMAGRAB_GRANTS, doSchemaGrabTableGrants);
		
		doGrabAllSchemaGrants = Utils.getPropBool(papp, PROP_SCHEMAGRAB_ALLGRANTS, doGrabAllSchemaGrants);
		doSchemaGrabIndexes = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_INDEXES, doSchemaGrabIndexes);
		doSchemaGrabProceduresAndFunctions = Utils.getPropBool(papp, PROP_SCHEMAGRAB_PROCEDURESANDFUNCTIONS, doSchemaGrabProceduresAndFunctions);
		doSchemaGrabDbSpecific = Utils.getPropBool(papp, PROP_DUMP_DBSPECIFIC, doSchemaGrabDbSpecific);
		maxLevel = Utils.getPropLong(papp, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL);
		doSetConnectionReadOnly = Utils.getPropBool(papp, PROP_SCHEMAGRAB_SETCONNREADONLY, doSetConnectionReadOnly);
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
	
	Map<TableType, Integer> tablesCountByTableType;
	Map<DBObjectType, Integer> execCountByType;
	
	List<Pattern> excludeTableFilters; //XXX: remove as object property & add as grabRelations() parameter?
	
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

		DBMSResources.instance().updateMetaData(conn.getMetaData(), true);
		feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		DatabaseMetaData dbmd = feats.getMetadataDecorator(conn.getMetaData());
		log.debug("feats/metadata: "+feats+" / "+dbmd);
		ConnectionUtil.showDBInfo(conn.getMetaData());
		if(log.isInfoEnabled()) {
			List<String> catalogs = SQLUtils.getCatalogNames(dbmd);
			if(catalogs!=null && catalogs.size()>0) {
				log.info("catalogs: "+catalogs);
			}
			//log.debug("schemas: "+SQLUtils.getSchemaNames(dbmd));
			//XXX: show current catalog/schema? maybe not: https://forums.oracle.com/thread/1097687
		}
		
		SchemaModel schemaModel = new SchemaModel();
		String schemaPattern = papp.getProperty(SQLDump.PROP_DUMPSCHEMAPATTERN);
		
		if(schemaPattern==null) {
			List<String> schemas = SQLUtils.getSchemaNames(dbmd);
			log.info("schemaPattern not defined. schemas avaiable: "+schemas);
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
				log.info("setting suggested schema: '"+schemaPattern+"'"
						+(equalsUsername?" (same as username)":"") );
				papp.setProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, schemaPattern);
				propOriginal.setProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, schemaPattern);
			}
		}

		if(schemaPattern==null) {
			log.error("schema name undefined & no suggestion avaiable, aborting...");
			if(failonerror) { throw new ProcessingException("schema name undefined & no suggestion avaiable, aborting..."); }
			return null;
		}
		
		log.info("schema dump... schema(s): '"+schemaPattern+"'");

		//init stat objects
		tablesCountByTableType = new HashMap<TableType, Integer>();
		for(TableType tt: TableType.values()) {
			tablesCountByTableType.put(tt, 0);
		}
		execCountByType = new HashMap<DBObjectType, Integer>();
		for(DBObjectType t: DBObjectType.values()) {
			execCountByType.put(t, 0);
		}
		
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
		
		schemaModel.setSqlDialect(DBMSResources.instance().dbid());

		if(doSchemaGrabTables) {
			
		List<String> tablePatterns = Utils.getStringListFromProp(papp, PROP_SCHEMADUMP_TABLEFILTER, ","); 
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
		
		//TODO: option to grab all FKs from schema...
		
		if(doSchemaGrabTables) {
		
		boolean recursivedump = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_RECURSIVEDUMP, false);
		if(recursivedump) {
			boolean grabExportedFKsAlso = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS, false);
			int lastTableCount = schemaModel.getTables().size();
			int level = 0;
			log.info("grabbing tables recursively["+level+"]: #ini:"+lastTableCount
					+(maxLevel!=null?" [maxlevel="+maxLevel+"]":" [maxlevel not defined]"));
			while(true) {
				level++;
				grabTablesRecursivebasedOnFKs(dbmd, feats, schemaModel, schemaPattern, grabExportedFKsAlso);
				
				int newTableCount = schemaModel.getTables().size();
				boolean wontGrowMore = (newTableCount <= lastTableCount);
				boolean maxLevelReached = (maxLevel!=null && level>=maxLevel);
				log.info("grabbing tables recursively["+level+"]: #last:"+lastTableCount+" #now:"+newTableCount
						+(wontGrowMore?" [won't grow more]":"")
						+(maxLevelReached?" [maxlevel reached]":"")
						);
				if(wontGrowMore || maxLevelReached) { break; }
				lastTableCount = newTableCount;
			}
		}
		
		}
		
		log.info(schemaModel.getTables().size()+" tables grabbed ["+tableStats()+"]");
		log.info(schemaModel.getForeignKeys().size()+" FKs grabbed");
		if(doSchemaGrabIndexes) {
			log.info(schemaModel.getIndexes().size()+" indexes grabbed");
		}
		
		if(doSchemaGrabProceduresAndFunctions) {
			int countproc = 0, countfunc = 0;
			try {
				for(String schemaName: schemasList) {
					List<ExecutableObject> eos = doGrabProcedures(dbmd, schemaName);
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
				log.warn("runtime exception grabbing functions: "+e);
			}
			catch(SQLException e) {
				log.warn("sql exception grabbing procedures: "+e);
			}
			log.info(countproc+" procedures grabbed ["+executableStats()+"]");
			
			try {
				for(String schemaName: schemasList) {
					List<ExecutableObject> eos = doGrabFunctions(dbmd, schemaName);
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
				log.warn("runtime exception grabbing functions: "+e);
			}
			catch(SQLException e) {
				log.warn("sql exception grabbing functions: "+e);
			}
			//XXX: add ["+executableStats()+"]?
			log.info(countfunc+" functions grabbed");
		}
		
		//XXX schema GRANTs ? how/where to add in schemaModel?
		if(doGrabAllSchemaGrants) {
			for(String schemaName: schemasList) {
				log.info("getting grants from schema "+schemaName);
				//XXX filter by GRANTEE, not GRANTOR/SCHEMA ? schema != user (in some databases?)
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
					view.setConstraints(grabRelationPKs(dbmd, view));

					//Columns & Remarks
					Table t = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, view.getSchemaName(), view.getName());
					if(t==null) {
						log.warn("view not found in grabbed tables' list: "+view.getSchemaName()+"."+view.getName());
						continue;
					}
					view.setSimpleColumns(t.getColumns());
					view.setRemarks(t.getRemarks());
				}
			}
		}

		filterObjects(schemaModel.getExecutables(), excludeObjectFilters, "executable");
		filterObjects(schemaModel.getTriggers(), excludeObjectFilters, "trigger");
		filterObjects(schemaModel.getViews(), excludeObjectFilters, "view");
		//XXX: filter FKs, indexes, sequences, synonyms?
		
		return schemaModel;

		}
		catch(Exception e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
	}
	
	String tableStats() {
		StringBuffer sb = new StringBuffer();
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
		StringBuffer sb = new StringBuffer();
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
	
	//XXX shoud it be "grabTables"?
	void grabRelations(SchemaModel schemaModel, DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, String schemaPattern, String tablePattern, boolean tableOnly) throws Exception { //, String padding
		log.debug("grabRelations()... schema: "+schemaPattern+", tablePattern: "+tablePattern);
		List<String> domainTables = Utils.getStringListFromProp(papp, PROP_SCHEMADUMP_DOMAINTABLES, ",");
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, tablePattern, null);

		boolean deeprecursivedump = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP, false);
		boolean ignoretableswithzerocolumns = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS, true);
		
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
				table.getConstraints().addAll(grabRelationPKs(dbmd, table));

				//FKs
				if(!tableOnly || deeprecursivedump) {
					schemaModel.getForeignKeys().addAll(grabRelationFKs(dbmd, dbmsfeatures, table));
				}
				
				//GRANTs
				if(doSchemaGrabTableGrants) {
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, table.getSchemaName(), tableName);
					table.setGrants( grabSchemaGrants(grantrs) );
					closeResultSetAndStatement(grantrs);
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

	List<FK> grabRelationFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, Relation relation) throws SQLException, IOException {
		String fullTablename = (relation.getSchemaName()==null?"":relation.getSchemaName()+".")+relation.getName();
		List<FK> ret = new ArrayList<FK>();
		
		//FKs
		if(doSchemaGrabFKs) {
			log.debug("getting FKs from "+fullTablename);
			ResultSet fkrs = dbmd.getImportedKeys(null, relation.getSchemaName(), relation.getName());
			ret.addAll(grabSchemaFKs(fkrs, relation, dbmsfeatures));
			closeResultSetAndStatement(fkrs);
		}

		//FKs "exported"
		if(doSchemaGrabExportedFKs) {
			log.debug("getting 'exported' FKs from "+fullTablename);
			ResultSet fkrs = dbmd.getExportedKeys(null, relation.getSchemaName(), relation.getName());
			ret.addAll(grabSchemaFKs(fkrs, relation, dbmsfeatures));
			closeResultSetAndStatement(fkrs);
		}
		
		return ret;
	}

	List<Constraint> grabRelationPKs(DatabaseMetaData dbmd, Relation relation) throws SQLException, IOException {
		//String fullTablename = (relation.getSchemaName()==null?"":relation.getSchemaName()+".")+relation.getName();
		List<Constraint> ret = new ArrayList<Constraint>();
		
		if(doSchemaGrabPKs) {
			//log.debug("getting PKs from "+fullTablename);
			ResultSet pks = dbmd.getPrimaryKeys(null, relation.getSchemaName(), relation.getName());
			Constraint pk = grabSchemaPKs(pks, relation);
			if(pk!=null) {
				ret.add(pk);
				//relation.getConstraints().add(pk);
			}
			closeResultSetAndStatement(pks);
		}

		return ret;
	}
	
	void grabTablesRecursivebasedOnFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, SchemaModel schemaModel, String schemaPattern, boolean grabExportedFKsAlso) throws Exception { //, String padding
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
	
	static boolean containsTableWithSchemaAndName(Set<Table> tables, String schemaName, String tableName) {
		for(Table t: tables) {
			if(t.getName().equals(tableName) && t.getSchemaName().equals(schemaName)) return true;
		}
		return false;
	}
	
	void grabDbSpecific(SchemaModel model, String schemaPattern) throws SQLException {
		//DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		if(feats!=null) { feats.grabDBObjects(model, schemaPattern, conn); }
	}

	List<ExecutableObject> doGrabProcedures(DatabaseMetaData dbmd, String schemaPattern) throws SQLException {
		List<ExecutableObject> eos = null;
		ResultSet rsProc = dbmd.getProcedures(null, schemaPattern, null);
		eos = grabProcedures(rsProc);
		closeResultSetAndStatement(rsProc);
		ResultSet rsProcCols = dbmd.getProcedureColumns(null, schemaPattern, null, null);
		grabProceduresColumns(eos, rsProcCols);
		closeResultSetAndStatement(rsProcCols);
		return eos;
	}
	
	List<ExecutableObject> doGrabFunctions(DatabaseMetaData dbmd, String schemaPattern) throws SQLException {
		List<ExecutableObject> eos = null;
		ResultSet rsFunc = dbmd.getFunctions(null, schemaPattern, null);
		eos = grabFunctions(rsFunc);
		closeResultSetAndStatement(rsFunc);
		ResultSet rsFuncCols = dbmd.getFunctionColumns(null, schemaPattern, null, null);
		grabFunctionsColumns(eos, rsFuncCols);
		closeResultSetAndStatement(rsFuncCols);
		return eos;
	}
	
	List<ExecutableObject> grabProcedures(ResultSet rs) throws SQLException {
		List<ExecutableObject> eos = new ArrayList<ExecutableObject>();
		while(rs.next()) {
			ExecutableObject eo = new ExecutableObject();
			eo.setName(rs.getString("PROCEDURE_NAME"));
			eo.setSchemaName(rs.getString("PROCEDURE_SCHEM"));
			eo.setPackageName(rs.getString("PROCEDURE_CAT")); //?? Oracle-only?
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

	void grabProceduresColumns(List<ExecutableObject> eos, ResultSet rs) throws SQLException {
		while(rs.next()) {
			ExecutableParameter ep = new ExecutableParameter();
			ep.name = rs.getString("COLUMN_NAME");
			ep.dataType = rs.getString("TYPE_NAME");
			
			int type = rs.getInt("COLUMN_TYPE");
			switch (type) {
			case DatabaseMetaData.procedureColumnIn:
				ep.inout = ExecutableParameter.INOUT.IN;
				break;
			case DatabaseMetaData.procedureColumnInOut:
				ep.inout = ExecutableParameter.INOUT.INOUT;
				break;
			case DatabaseMetaData.procedureColumnOut:
				ep.inout = ExecutableParameter.INOUT.OUT;
				break;
			default:
				break;
			}
			
			try {
				ep.position = rs.getInt("ORDINAL_POSITION");
			}
			catch(SQLException e) {
				try {
					ep.position = rs.getInt("SEQUENCE"); //XXX: oracle-only?
				}
				catch(SQLException e2) {
					log.warn("column name for procedure parameter ordinal position not found: "+e);
					log.debug("grabProceduresColumns: columns avaiable: "+SQLUtils.getColumnNames(rs.getMetaData()));
				}
			}
			
			String pName = rs.getString("PROCEDURE_NAME");
			String pSchem = rs.getString("PROCEDURE_SCHEM");
			ExecutableObject eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.PROCEDURE, pSchem, pName);
			if(eo==null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.FUNCTION, pSchem, pName);
				//XXX: oracle driver adds 1 to function parameter positions...
				if("oracle".equals(DBMSResources.instance().dbid())) {
					ep.position--;
				}
			}
			
			if(ep.position==0) { 
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
			//eo.setPackageName(rs.getString("FUNCTION_CAT"));
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

	void grabFunctionsColumns(List<ExecutableObject> eos, ResultSet rs) throws SQLException {
		while(rs.next()) {
			ExecutableParameter ep = new ExecutableParameter();
			ep.name = rs.getString("COLUMN_NAME");
			ep.dataType = rs.getString("TYPE_NAME");
			
			int type = rs.getInt("COLUMN_TYPE");
			switch (type) {
			case DatabaseMetaData.functionColumnIn:
				ep.inout = ExecutableParameter.INOUT.IN;
				break;
			case DatabaseMetaData.functionColumnInOut:
				ep.inout = ExecutableParameter.INOUT.INOUT;
				break;
			case DatabaseMetaData.functionColumnOut:
				ep.inout = ExecutableParameter.INOUT.OUT;
				break;
			//XXX case DatabaseMetaData.functionColumnReturn: //??
			default:
				break;
			}
			
			try {
				ep.position = rs.getInt("ORDINAL_POSITION");
			}
			catch(SQLException e) {
				log.warn("unknown column name for function parameter ordinal position: "+e);
			}
			
			String pName = rs.getString("FUNCTION_NAME");
			String pSchem = rs.getString("FUNCTION_SCHEM");
			ExecutableObject eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(eos, DBObjectType.FUNCTION, pSchem, pName);
			
			if(ep.position==0) {
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
	
	static Column retrieveColumn(ResultSet cols) throws SQLException {
		Column c = new Column();
		c.setName( cols.getString("COLUMN_NAME") );
		c.type = cols.getString("TYPE_NAME");
		c.nullable = "YES".equals(cols.getString("IS_NULLABLE"));
		c.columSize = cols.getInt("COLUMN_SIZE");
		c.setRemarks(cols.getString("REMARKS"));
		boolean autoInc = false;
		if(grabColumnIsAutoincrement) {
			try {
				cols.getBoolean("IS_AUTOINCREMENT");
			}
			catch(Exception e) {
				grabColumnIsAutoincrement = false;
				log.warn("DatabaseMetaData.getColumns(): column 'IS_AUTOINCREMENT' not avaiable");
			}
		}
		if(autoInc) { c.autoIncrement = true; }
		Object decimalDigits = cols.getObject("DECIMAL_DIGITS");
		if(decimalDigits!=null) {
			int iDecimalDigits = ((Number) decimalDigits).intValue();
			if(iDecimalDigits!=0) {
				c.decimalDigits = iDecimalDigits;
			} 
		}
		return c;
	}

	Set<String> unknownPrivilegesWarned = new HashSet<String>();
	
	List<Grant> grabSchemaGrants(ResultSet grantrs) throws SQLException {
		List<Grant> grantsList = new ArrayList<Grant>();
		String privilege = null;
		while(grantrs.next()) {
			try {
				Grant grant = new Grant();
				
				grant.grantee = grantrs.getString("GRANTEE");
				privilege = Utils.normalizeEnumStringConstant(grantrs.getString("PRIVILEGE"));
				grant.privilege = PrivilegeType.valueOf(privilege);
				grant.table = grantrs.getString("TABLE_NAME");
				grant.withGrantOption = "YES".equals(grantrs.getString("IS_GRANTABLE"));
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
	
	Constraint grabSchemaPKs(ResultSet pks, Relation relation) throws SQLException {
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
		if(count==0) return null; //no PK

		Constraint cPK = new Constraint();
		cPK.type = ConstraintType.PK;
		cPK.setName(pkName);
		cPK.uniqueColumns.addAll( pkCols.values() );
		return cPK;
	}

	List<FK> grabSchemaFKs(ResultSet fkrs, Relation table, DBMSFeatures dbmsfeatures) throws SQLException, IOException {
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
				fk.updateRule = UpdateRule.getUpdateRule(updateRule);
				fk.deleteRule = UpdateRule.getUpdateRule(deleteRule);
				//log.info("fk: "+fkName+" :: rules: "+updateRule+"|"+fk.updateRule+" / "+deleteRule+"|"+fk.deleteRule+"");
				
				//for MySQL
				if(fk.getPkTableSchemaName()==null && pkTableCatalog!=null) { fk.setPkTableSchemaName(pkTableCatalog); }
				if(fk.getFkTableSchemaName()==null && fkTableCatalog!=null) { fk.setFkTableSchemaName(fkTableCatalog); }
				
				if(askForUkType) {
					try {
						fk.fkReferencesPK = "P".equals(fkrs.getString("UK_CONSTRAINT_TYPE"));
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
		for(String key: fks.keySet()) {
			ret.add(fks.get(key));
		}
		return ret;
	}
	
	void grabSchemaIndexes(ResultSet indexesrs, Set<Index> indexes) throws SQLException {
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
				idx.unique = !bNonUnique;
				
				idx.setSchemaName( indexesrs.getString("TABLE_SCHEM") );
				idx.tableName = indexesrs.getString("TABLE_NAME");
				String catName = indexesrs.getString("TABLE_CAT");

				//for MySQL
				if(idx.getSchemaName()==null && catName!=null) { idx.setSchemaName( catName ); }
				if(idx.getName().equals("PRIMARY")) {
					idx.setName( SQLUtils.newNameFromTableName(idx.tableName, SQLUtils.pkiNamePattern) );
				}
				
			}
			idx.columns.add(indexesrs.getString("COLUMN_NAME"));
		}
		if(idx!=null) {
			indexes.add(idx);
		}
	}
	
	static void closeResultSetAndStatement(ResultSet rs) {
		try {
			if(rs!=null) {
				if(rs.getStatement()!=null) {
					rs.getStatement().close();
				}
				rs.close();
			}
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
	
}
