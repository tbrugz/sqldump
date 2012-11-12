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
import tbrugz.sqldump.dbmodel.DBObjectType;
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
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.ParametrizedProperties;
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
public class JDBCSchemaGrabber implements SchemaModelGrabber {
	
	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP_PKS = "sqldump.doschemadump.pks";
	static final String PROP_DO_SCHEMADUMP_FKS = "sqldump.doschemadump.fks";
	static final String PROP_DO_SCHEMADUMP_EXPORTEDFKS = "sqldump.doschemadump.exportedfks";
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS = "sqldump.doschemadump.ignoretableswithzerocolumns";
	
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP = "sqldump.doschemadump.recursivedumpbasedonfks";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP = "sqldump.doschemadump.recursivedumpbasedonfks.deep";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL = "sqldump.doschemadump.recursivedumpbasedonfks.maxlevel";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS = "sqldump.doschemadump.recursivedumpbasedonfks.exportedfks";

	static final String PROP_SCHEMADUMP_DOMAINTABLES = "sqldump.schemainfo.domaintables";
	static final String PROP_SCHEMADUMP_TABLEFILTER = "sqldump.schemagrab.tablefilter";
	static final String PROP_SCHEMADUMP_EXCLUDETABLES = "sqldump.schemadump.tablename.excludes";
	
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";

	static Log log = LogFactory.getLog(JDBCSchemaGrabber.class);
	
	static String[] DEFAULT_SCHEMA_NAMES = {
		"public", //postgresql, h2, hsqldb
		"APP",    //derby
	};
	
	Connection conn;
	
	//tables OK for data dump
	//public List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties propOriginal;
	DBMSFeatures feats = null;
	
	//Properties dbmsSpecificResource = new ParametrizedProperties();
	
	boolean doSchemaGrabPKs = true, 
			doSchemaGrabFKs = true, 
			doSchemaGrabExportedFKs = false, 
			doSchemaGrabGrants = false, 
			doSchemaGrabIndexes = false,
			doSchemaGrabDbSpecific = false;
	
	Long maxLevel = null;
	
	@Override
	public void procProperties(Properties prop) {
		log.info("init JDBCSchemaGrabber...");
		
		propOriginal = prop;
		//papp = prop;
		papp.putAll(prop);
		
		//inicializa variaveis controle
		doSchemaGrabPKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_PKS, doSchemaGrabPKs);
		doSchemaGrabFKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_FKS, doSchemaGrabFKs);
		doSchemaGrabExportedFKs = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_EXPORTEDFKS, doSchemaGrabExportedFKs);
		doSchemaGrabGrants = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_GRANTS, doSchemaGrabGrants);
		doSchemaGrabIndexes = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_INDEXES, doSchemaGrabIndexes);
		doSchemaGrabDbSpecific = Utils.getPropBool(papp, PROP_DUMP_DBSPECIFIC, doSchemaGrabDbSpecific);
		maxLevel = Utils.getPropLong(papp, PROP_DO_SCHEMADUMP_RECURSIVEDUMP_MAXLEVEL);

		/*try {
			dbmsSpecificResource.load(JDBCSchemaGrabber.class.getClassLoader().getResourceAsStream(SQLDump.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}

	public void setConnection(Connection conn) {
		this.conn = conn;
		try {
			conn.setReadOnly(true);
		} catch (SQLException e) {
			log.warn("error setting props [readonly=true] for db connection");
			log.debug("stack...", e);
			try { conn.rollback(); }
			catch(SQLException ee) { log.warn("error in rollback(): "+ee.getMessage()); }
		}
	}
	
	@Override
	public boolean needsConnection() {
		return true;
	}
	
	void end() throws Exception {
		log.info("...done");
		conn.close();
	}

	Map<TableType, Integer> tablesCountByTableType;
	List<Pattern> excludeTableFilters;
	
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

		feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		DatabaseMetaData dbmd = feats.getMetadataDecorator(conn.getMetaData());
		SQLUtils.ConnectionUtil.showDBInfo(conn.getMetaData());
		
		SchemaModel schemaModel = new SchemaModel();
		String schemaPattern = papp.getProperty(SQLDump.PROP_DUMPSCHEMAPATTERN);
		
		if(schemaPattern==null) {
			List<String> schemas = SQLUtils.getSchemaNames(conn.getMetaData());
			log.info("schemaPattern not defined. schemas avaiable: "+schemas);
			schemaPattern = Utils.getEqualIgnoreCaseFromList(schemas, papp.getProperty(SQLDump.CONN_PROPS_PREFIX + SQLUtils.ConnectionUtil.SUFFIX_USER));
			boolean equalsUsername = false;
			if(schemaPattern!=null) { equalsUsername = true; }
			
			int counter = 0;
			while(schemaPattern==null && DEFAULT_SCHEMA_NAMES.length>counter) {
				schemaPattern = Utils.getEqualIgnoreCaseFromList(schemas, DEFAULT_SCHEMA_NAMES[counter]);
				if(schemaPattern!=null) { break; }
				counter++;
			}
			
			if(schemaPattern!=null) {
				log.info("setting suggested schema: "+schemaPattern+(equalsUsername?" (same as username)":""));
				papp.setProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, schemaPattern);
				propOriginal.setProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, schemaPattern);
			}
		}

		if(schemaPattern==null) {
			log.warn("schema name undefined & no suggestion avaiable, aborting...");
			return null;
		}
		
		log.info("schema dump... schema(s): "+schemaPattern);
		tablesCountByTableType = new HashMap<TableType, Integer>();
		for(TableType tt: TableType.values()) {
			tablesCountByTableType.put(tt, 0);
		}
		
		//register excklude table filters
		excludeTableFilters = getExcludeTableFilters(papp);
		
		String[] schemasArr = schemaPattern.split(",");
		List<String> schemasList = new ArrayList<String>();
		for(String schemaName: schemasArr) {
			schemasList.add(schemaName.trim());
		}
		
		schemaModel.setSqlDialect(DBMSResources.instance().dbid());

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
		
		log.info(schemaModel.getTables().size()+" tables grabbed ["+tableStats()+"]");
		log.info(schemaModel.getForeignKeys().size()+" FKs grabbed");
		if(doSchemaGrabIndexes) {
			log.info(schemaModel.getIndexes().size()+" indexes grabbed");
		}
			
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
						log.warn("table not found: "+view.getSchemaName()+"."+view.getName());
						continue;
					}
					view.setSimpleColumns(t.getColumns());
					view.setRemarks(t.getRemarks());
				}
			}
		}

		return schemaModel;

		}
		catch(Exception e) {
			log.warn("error grabbing schema: "+e);
			log.info("error grabbing schema", e);
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
				String fullTablename = (schemaPattern==null?"":table.getSchemaName()+".")+tableName;
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
				if(doSchemaGrabGrants) {
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, table.getSchemaName(), tableName);
					table.setGrants( grabSchemaGrants(grantrs, tableName) );
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
				log.warn("OutOfMemoryError: memory: max: "+Runtime.getRuntime().maxMemory()+"; total: "+Runtime.getRuntime().totalMemory()+"; free: "+Runtime.getRuntime().freeMemory());
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
			dbid.setName(fk.pkTable);
			dbid.setSchemaName(fk.pkTableSchemaName);
			ids.add(dbid);
	
			//Exported FKs
			if(grabExportedFKsAlso) {
				DBObjectId dbidFk = new DBObjectId();
				dbidFk.setName(fk.fkTable);
				dbidFk.setSchemaName(fk.fkTableSchemaName);
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
			catch(SQLException e) {
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

	List<Grant> grabSchemaGrants(ResultSet grantrs, String tableName) throws SQLException {
		List<Grant> grantsList = new ArrayList<Grant>();
		while(grantrs.next()) {
			try {
				Grant grant = new Grant();
				
				grant.grantee = grantrs.getString("GRANTEE");
				grant.privilege = PrivilegeType.valueOf(Utils.normalizeEnumStringConstant(grantrs.getString("PRIVILEGE")));
				grant.table = grantrs.getString("TABLE_NAME");
				grant.withGrantOption = "YES".equals(grantrs.getString("IS_GRANTABLE"));
				grantsList.add(grant);
			}
			catch(IllegalArgumentException iae) {
				log.warn(iae);
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
				pkName = newNameFromTableName(relation.getName(), pkNamePattern);
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
			if(fkName==null) {
				log.warn("nameless FK: "+fkrs.getString("FKTABLE_NAME")+"->"+fkrs.getString("PKTABLE_NAME"));
				fkName = "FK_"+count;
				count++;
			}
			FK fk = fks.get(fkName);
			if(fk==null) {
				fk = dbmsfeatures.getForeignKeyObject();
				fk.setName(fkName);
				fks.put(fkName, fk);
			}
			if(fk.pkTable==null) {
				fk.pkTable = fkrs.getString("PKTABLE_NAME");
				fk.fkTable = fkrs.getString("FKTABLE_NAME");
				fk.pkTableSchemaName = fkrs.getString("PKTABLE_SCHEM");
				fk.fkTableSchemaName = fkrs.getString("FKTABLE_SCHEM");
				String pkTableCatalog = fkrs.getString("PKTABLE_CAT");
				String fkTableCatalog = fkrs.getString("FKTABLE_CAT");
				String updateRule = fkrs.getString("UPDATE_RULE");
				String deleteRule = fkrs.getString("DELETE_RULE");
				fk.updateRule = UpdateRule.getUpdateRule(updateRule);
				fk.deleteRule = UpdateRule.getUpdateRule(deleteRule);
				//log.debug("fk: "+fkName+" :: rules: "+updateRule+"|"+fk.updateRule+" / "+deleteRule+"|"+fk.deleteRule+"");
				
				//for MySQL
				if(fk.pkTableSchemaName==null && pkTableCatalog!=null) { fk.pkTableSchemaName = pkTableCatalog; }
				if(fk.fkTableSchemaName==null && fkTableCatalog!=null) { fk.fkTableSchemaName = fkTableCatalog; }
				
				if(askForUkType) {
					try {
						fk.fkReferencesPK = "P".equals(fkrs.getString("UK_CONSTRAINT_TYPE"));
					}
					catch(SQLException e) {
						askForUkType = false;
						log.debug("resultset has no 'UK_CONSTRAINT_TYPE' column [fkTable='"+fk.fkTable+"'; ukTable='"+fk.pkTable+"']");
					}
				}
				dbmsfeatures.addFKSpecificFeatures(fk, fkrs);
			}
			fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
			fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
			log.debug("fk: "+fkName+" - "+fk);
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
					idx.setName( newNameFromTableName(idx.tableName, pkiNamePattern) );
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
	
	static List<Pattern> getExcludeTableFilters(Properties prop) {
		List<Pattern> excludeFilters = new ArrayList<Pattern>();
		//int count = 1;
		String filter = prop.getProperty(PROP_SCHEMADUMP_EXCLUDETABLES);
		if(filter==null) { return excludeFilters; }
		String[] excludes = filter.split("\\|");
		for(String ex: excludes) {
			String pattern = ex.trim();
			log.info("added ignore filter: "+pattern);
			excludeFilters.add(Pattern.compile(pattern));
			//count++;
		}
		return excludeFilters;
	}
	
	//XXX: props for setting pk(i)NamePatterns?
	String pkNamePattern = "${tablename}_pk";
	String pkiNamePattern = "${tablename}_pki";
	
	String newNameFromTableName(String tableName, String pattern) {
		return pattern.replaceAll("\\$\\{tablename\\}", tableName);
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
	
}
