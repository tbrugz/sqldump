package tbrugz.sqldump;

import java.util.*;
import java.util.regex.Pattern;
import java.sql.*;
import java.io.*;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.DBObject.DBObjectId;

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
	static final String PROP_DO_SCHEMADUMP_FKS_ATEND = "sqldump.doschemadump.fks.atend";
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS = "sqldump.doschemadump.ignoretableswithzerocolumns";
	
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP = "sqldump.doschemadump.recursivedumpbasedonfks";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP = "sqldump.doschemadump.recursivedumpbasedonfks.deep";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS = "sqldump.doschemadump.recursivedumpbasedonfks.exportedfks";
	
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";

	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	
	static Logger log = Logger.getLogger(JDBCSchemaGrabber.class);
	
	Connection conn;
	
	//tables OK for data dump
	//public List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties propOriginal;
	Properties columnTypeMapping = new ParametrizedProperties();
	
	boolean doSchemaGrabPKs = false, doSchemaGrabFKs = false, doSchemaGrabExportedFKs = false, doSchemaGrabGrants = false, doSchemaGrabIndexes = false;
	boolean doSchemaGrabDbSpecific = false;
	
	@Override
	public void procProperties(Properties prop) {
		log.info("init JDBCSchemaGrabber...");
		propOriginal = prop;
		//papp = prop;
		papp.putAll(prop);
		
		//inicializa variaveis controle
		doSchemaGrabPKs = papp.getProperty(PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		doSchemaGrabFKs = papp.getProperty(PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		doSchemaGrabExportedFKs = papp.getProperty(PROP_DO_SCHEMADUMP_EXPORTEDFKS, "").equals("true");
		doSchemaGrabGrants = papp.getProperty(PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		doSchemaGrabIndexes = papp.getProperty(PROP_DO_SCHEMADUMP_INDEXES, "").equals("true");
		doSchemaGrabDbSpecific = papp.getProperty(PROP_DUMP_DBSPECIFIC, "").equals("true");

		try {
			columnTypeMapping.load(JDBCSchemaGrabber.class.getClassLoader().getResourceAsStream(SQLDump.COLUMN_TYPE_MAPPING_RESOURCE));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setConnection(Connection conn) {
		this.conn = conn;
		try {
			conn.setReadOnly(true);
		} catch (SQLException e) {
			log.warn("error setting props [readonly=true] for db connection");
			e.printStackTrace();
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
	public SchemaModel grabSchema() throws Exception {
		if(Utils.getPropBool(papp, SQLDump.PROP_FROM_DB_ID_AUTODETECT)) {
			String dbid = detectDbId(conn.getMetaData());
			if(dbid!=null) {
				log.info("database type identifier: "+dbid);
				papp.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
				propOriginal.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
			}
			else { log.warn("can't detect database type"); }
		}

		DBMSFeatures feats = grabDbSpecificFeaturesClass();
		DatabaseMetaData dbmd = feats.getMetadataDecorator(conn.getMetaData());
		showDBInfo(conn.getMetaData());
		
		SchemaModel schemaModel = new SchemaModel();
		String schemaPattern = papp.getProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, null);

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

		for(String schemaName: schemasList) {
			grabSchema(schemaModel, dbmd, feats, schemaName, null, false);
		}
		
		boolean recursivedump = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP));
		if(recursivedump) {
			boolean grabExportedFKsAlso = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP_EXPORTEDFKS));
			int lastTableCount = schemaModel.tables.size();
			log.info("grabbing tables recursively: #ini:"+lastTableCount);
			while(true) {
				grabTablesRecursivebasedOnFKs(dbmd, feats, schemaModel, schemaPattern, grabExportedFKsAlso);
				int newTableCount = schemaModel.tables.size();
				if(newTableCount <= lastTableCount) { break; }
				log.info("grabbing tables recursively: #last:"+lastTableCount+" #now:"+newTableCount);
				lastTableCount = newTableCount;
			}
		}
		
		log.info(schemaModel.tables.size()+" tables grabbed ["+tableStats()+"]");
		log.info(schemaModel.foreignKeys.size()+" FKs grabbed");
		if(doSchemaGrabIndexes) {
			log.info(schemaModel.indexes.size()+" indexes grabbed");
		}
			
		if(doSchemaGrabDbSpecific) {
			for(String schemaName: schemasList) {
				grabDbSpecific(schemaModel, schemaName);
			}
		}

		return schemaModel;
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
	
	static void showDBInfo(DatabaseMetaData dbmd) {
		try {
			log.info("database info: "+dbmd.getDatabaseProductName()+"; "+dbmd.getDatabaseProductVersion()+" ["+dbmd.getDatabaseMajorVersion()+"."+dbmd.getDatabaseMinorVersion()+"]");
			log.info("jdbc driver info: "+dbmd.getDriverName()+"; "+dbmd.getDriverVersion()+" ["+dbmd.getDriverMajorVersion()+"."+dbmd.getDriverMinorVersion()+"]");
		} catch (Exception e) {
			log.warn("error grabbing database/jdbc driver info: "+e);
			//e.printStackTrace();
		}
	}
	
	//private static String PADDING = "  ";
	
	void grabSchema(SchemaModel schemaModel, DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, String schemaPattern, String tablePattern, boolean tableOnly) throws Exception { //, String padding
		log.debug("grabSchema()... schema: "+schemaPattern+", tablePattern: "+tablePattern);
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, tablePattern, null);

		//boolean recursivedump = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP));
		boolean deeprecursivedump = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP));
		boolean ignoretableswithzerocolumns = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP_IGNORETABLESWITHZEROCOLUMNS);
		
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
			table.name = tableName;
			table.schemaName = schemaName;
			table.setType(ttype);
			table.setRemarks(rs.getString("REMARKS"));
			dbmsfeatures.addTableSpecificFeatures(table, rs);
			
			try {
				String fullTablename = (schemaPattern==null?"":table.schemaName+".")+tableName;
				log.debug("getting columns from "+fullTablename);

				//columns
				ResultSet cols = dbmd.getColumns(null, table.schemaName, tableName, null);
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
					log.debug("getting PKs from "+fullTablename);
					ResultSet pks = dbmd.getPrimaryKeys(null, table.schemaName, tableName);
					grabSchemaPKs(pks, table);
					closeResultSetAndStatement(pks);
				}

				//FKs
				if(doSchemaGrabFKs && (!tableOnly || deeprecursivedump)) {
					log.debug("getting FKs from "+fullTablename);
					ResultSet fkrs = dbmd.getImportedKeys(null, table.schemaName, tableName);
					grabSchemaFKs(fkrs, table, schemaModel.foreignKeys);
					closeResultSetAndStatement(fkrs);
				}

				//FKs "exported"
				if(doSchemaGrabExportedFKs && (!tableOnly || deeprecursivedump)) {
					log.debug("getting 'exported' FKs from "+fullTablename);
					ResultSet fkrs = dbmd.getExportedKeys(null, table.schemaName, tableName);
					grabSchemaFKs(fkrs, table, schemaModel.foreignKeys);
					closeResultSetAndStatement(fkrs);
				}
				
				//GRANTs
				if(doSchemaGrabGrants) {
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, table.schemaName, tableName);
					table.setGrants( grabSchemaGrants(grantrs, tableName) );
					closeResultSetAndStatement(grantrs);
				}
				
				//INDEXes
				if(doSchemaGrabIndexes && TableType.TABLE.equals(table.getType()) && !tableOnly) {
					log.debug("getting indexes from "+fullTablename);
					ResultSet indexesrs = dbmd.getIndexInfo(null, table.schemaName, tableName, false, false);
					grabSchemaIndexes(indexesrs, schemaModel.indexes);
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
				log.debug("exception in table: "+tableName+" ["+sqle.getMessage()+"]", sqle);
				//tableNamesForDataDump.remove(tableName);
			}
			
			table.validateConstraints();

			schemaModel.tables.add(table);
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
	
	void grabTablesRecursivebasedOnFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, SchemaModel schemaModel, String schemaPattern, boolean grabExportedFKsAlso) throws Exception { //, String padding
		log.debug("recursivegrab: "+schemaPattern);
		Set<DBObjectId> ids = new HashSet<DBObjectId>();
		for(FK fk: schemaModel.foreignKeys) {
			DBObjectId dbid = new DBObjectId();
			dbid.name = fk.pkTable;
			dbid.schemaName = fk.pkTableSchemaName;
			ids.add(dbid);
	
			//Exported FKs
			if(grabExportedFKsAlso) {
				DBObjectId dbidFk = new DBObjectId();
				dbidFk.name = fk.fkTable;
				dbidFk.schemaName = fk.fkTableSchemaName;
				ids.add(dbidFk);
			}
			
			/*if(!schemaPattern.equals(fk.pkTableSchemaName) && !containsTableWithSchemaAndName(schemaModel.tables, fk.pkTableSchemaName, fk.pkTable)) {
				log.warn("recursivegrab-grabschema: "+fk.pkTableSchemaName+"."+fk.pkTable);
				grabSchema(dbmd, fk.pkTableSchemaName, fk.pkTable, true);				
			}*/
		}
		for(DBObjectId id: ids) {
			//if(!schemaPattern.equals(id.schemaName) && !containsTableWithSchemaAndName(schemaModel.tables, id.schemaName, id.name)) {
			if(!containsTableWithSchemaAndName(schemaModel.tables, id.schemaName, id.name)) {
				log.debug("recursivegrab-grabschema: "+id.schemaName+"."+id.name);
				grabSchema(schemaModel, dbmd, dbmsfeatures, id.schemaName, id.name, true);				
			}
		}
	}
	
	static boolean containsTableWithSchemaAndName(Set<Table> tables, String schemaName, String tableName) {
		for(Table t: tables) {
			if(t.name.equals(tableName) && t.schemaName.equals(schemaName)) return true;
		}
		return false;
	}
	
	void grabDbSpecific(SchemaModel model, String schemaPattern) throws SQLException {
		DBMSFeatures feats = grabDbSpecificFeaturesClass();
		if(feats!=null) feats.grabDBObjects(model, schemaPattern, conn);
	}

	DBMSFeatures grabDbSpecificFeaturesClass() {
		String dbSpecificFeaturesClass = columnTypeMapping.getProperty("dbms."+papp.getProperty(SQLDump.PROP_FROM_DB_ID)+".specificgrabclass");
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				DBMSFeatures of = (DBMSFeatures) c.newInstance();
				of.procProperties(papp);
				return of;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return new DefaultDBMSFeatures();
	}
	
	static Column retrieveColumn(ResultSet cols) throws SQLException {
		Column c = new Column();
		c.setName( cols.getString("COLUMN_NAME") );
		c.type = cols.getString("TYPE_NAME");
		c.nullable = "YES".equals(cols.getString("IS_NULLABLE"));
		c.columSize = cols.getInt("COLUMN_SIZE");
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
	
	void grabSchemaPKs(ResultSet pks, Table table) throws SQLException {
		Map<Integer, String> pkCols = new TreeMap<Integer, String>();
		String pkName = null;
		
		int count=0;
		while(pks.next()) {
			pkName = pks.getString("PK_NAME");
			if(pkName==null || pkName.equals("PRIMARY")) { //equals("PRIMARY"): for MySQL
				pkName = "PK_"+table.name;
			}
			pkCols.put(pks.getInt("KEY_SEQ"), pks.getString("COLUMN_NAME"));
			count++;
		}
		if(count==0) return; //no PK

		Constraint cPK = new Constraint();
		cPK.type = ConstraintType.PK;
		cPK.setName(pkName);
		cPK.uniqueColumns.addAll( pkCols.values() );
		table.getConstraints().add(cPK);
	}

	void grabSchemaFKs(ResultSet fkrs, Table table, Set<FK> foreignKeys) throws SQLException, IOException {
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
			log.debug("fk: "+fkName+" - "+fk);
			if(fk==null) {
				fk = new FK();
				fk.setName(fkName);
				fks.put(fkName, fk);
			}
			if(fk.pkTable==null) {
				fk.pkTable = fkrs.getString("PKTABLE_NAME");
				fk.fkTable = fkrs.getString("FKTABLE_NAME");
				fk.pkTableSchemaName = fkrs.getString("PKTABLE_SCHEM");
				fk.fkTableSchemaName = fkrs.getString("FKTABLE_SCHEM");
				if(askForUkType) {
					try {
						fk.fkReferencesPK = "P".equals(fkrs.getString("UK_CONSTRAINT_TYPE"));
					}
					catch(SQLException e) {
						askForUkType = false;
						log.debug("resultset has no 'UK_CONSTRAINT_TYPE' column [fkTable='"+fk.fkTable+"'; ukTable='"+fk.pkTable+"']");
					}
				}
			}
			fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
			fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
		}
		for(String key: fks.keySet()) {
			foreignKeys.add(fks.get(key));
		}
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
			if(idx==null || !idxName.equals(idx.name)) {
				//end last object
				if(idx!=null) {
					indexes.add(idx);
				}
				//new object
				idx = new Index();
				idx.name = idxName;
				idx.unique = indexesrs.getInt("NON_UNIQUE")==0;
				idx.schemaName = indexesrs.getString("TABLE_SCHEM");
				idx.tableName = indexesrs.getString("TABLE_NAME");
			}
			idx.columns.add(indexesrs.getString("COLUMN_NAME"));
		}
		if(idx!=null) {
			indexes.add(idx);
		}
	}
	
	String detectDbId(DatabaseMetaData dbmd) {
		try {
			String dbProdName = dbmd.getDatabaseProductName();
			String dbIdsProp = columnTypeMapping.getProperty("dbids");
			String[] dbIds = dbIdsProp.split(",");
			for(String dbid: dbIds) {
				String regex = columnTypeMapping.getProperty("dbid."+dbid.trim()+".detectregex");
				if(regex==null) { continue; }
				if(dbProdName.matches(regex)) { return dbid.trim(); }
			}
		} catch (SQLException e) {
			log.warn("Error detecting database type: "+e);
			log.debug("Error detecting database type",e);
		}
		return null;
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
		int count = 1;
		while(true) {
			String filterProp = "sqldump.schemadump.tablename.exclude."+count;
			String regex = prop.getProperty(filterProp);
			if(regex==null) { break; }
			log.info("added ignore filter: "+regex);
			excludeFilters.add(Pattern.compile(regex));
			count++;
		}
		return excludeFilters;
	}
}
