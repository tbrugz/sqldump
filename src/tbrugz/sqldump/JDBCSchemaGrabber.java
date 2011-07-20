package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.dbmodel.DBObject.DBObjectId;

/*
 * TODO: accept list of schemas, tables/objects to grab/dump, types of objects to grab/dump
 */
public class JDBCSchemaGrabber implements SchemaModelGrabber {
	
	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP_PKS = "sqldump.doschemadump.pks";
	static final String PROP_DO_SCHEMADUMP_FKS = "sqldump.doschemadump.fks";
	static final String PROP_DO_SCHEMADUMP_FKS_ATEND = "sqldump.doschemadump.fks.atend";
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP = "sqldump.doschemadump.recursivedumpbasedonfks";
	static final String PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP = "sqldump.doschemadump.recursivedumpbasedonfks.deep";
	
	static final String PROP_DUMP_SYNONYM_AS_TABLE = "sqldump.dumpsynonymastable";
	static final String PROP_DUMP_VIEW_AS_TABLE = "sqldump.dumpviewastable";
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";

	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	
	static Logger log = Logger.getLogger(JDBCSchemaGrabber.class);
	
	Connection conn;
	
	//tables OK for data dump
	public List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties columnTypeMapping = new ParametrizedProperties();
	
	boolean doSchemaGrabPKs = false, doSchemaGrabFKs = false, doSchemaGrabGrants = false, doSchemaGrabIndexes = false;
	boolean doSchemaGrabDbSpecific = false;
	
	//for conformance with SchemaModelGrabber
	@Override
	public void procProperties(Properties prop) {
		log.info("init JDBCSchemaGrabber...");
		papp.putAll(prop);
		
		//inicializa variaveis controle
		doSchemaGrabPKs = papp.getProperty(PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		doSchemaGrabFKs = papp.getProperty(PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		//doSchemaDumpFKsAtEnd = papp.getProperty(PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		doSchemaGrabGrants = papp.getProperty(PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		//dumpWithSchemaName = papp.getProperty(PROP_DUMP_WITH_SCHEMA_NAME, "").equals("true");
		//dumpSynonymAsTable = papp.getProperty(PROP_DUMP_SYNONYM_AS_TABLE, "").equals("true");
		//dumpViewAsTable = papp.getProperty(PROP_DUMP_VIEW_AS_TABLE, "").equals("true");
		doSchemaGrabIndexes = papp.getProperty(PROP_DO_SCHEMADUMP_INDEXES, "").equals("true");
		doSchemaGrabDbSpecific = papp.getProperty(PROP_DUMP_DBSPECIFIC, "").equals("true");

		try {
			columnTypeMapping.load(JDBCSchemaGrabber.class.getClassLoader().getResourceAsStream(SQLDump.COLUMN_TYPE_MAPPING_RESOURCE));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	void end() throws Exception {
		log.info("...done");
		conn.close();
	}

	@Override
	public SchemaModel grabSchema() throws Exception {
		DBMSFeatures feats = grabDbSpecificFeaturesClass();
		DatabaseMetaData dbmd = feats.getMetadataDecorator(conn.getMetaData());
		
		showDBInfo(dbmd);

		SchemaModel schemaModel = new SchemaModel();
		String schemaPattern = papp.getProperty(SQLDump.PROP_DUMPSCHEMAPATTERN, null);

		log.info("schema dump... schemapattern: "+schemaPattern);
		grabSchema(schemaModel, dbmd, feats, schemaPattern, null, false);
		
		log.info(schemaModel.tables.size()+" tables grabbed");
		log.info(schemaModel.foreignKeys.size()+" FKs grabbed");
		if(doSchemaGrabIndexes) {
			log.info(schemaModel.indexes.size()+" indexes grabbed");
		}

		if(doSchemaGrabDbSpecific) {
			grabDbSpecific(schemaModel, schemaPattern);
		}
		
		return schemaModel;
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
		log.debug("schema dump... schemapattern: "+schemaPattern+", tablePattern: "+tablePattern);
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, tablePattern, null);

		boolean recursivedump = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP));
		boolean deeprecursivedump = "true".equals(papp.getProperty(JDBCSchemaGrabber.PROP_DO_SCHEMADUMP_RECURSIVEDUMP_DEEP));
		
		while(rs.next()) {
			TableType ttype = null;
			String tableName = rs.getString("TABLE_NAME");
			String schemaName = rs.getString("TABLE_SCHEM");
			
			ttype = TableType.getTableType(rs.getString("TABLE_TYPE"), tableName);
			if(ttype==null) { continue; }
			
			//defining model
			Table table = dbmsfeatures.getTableObject();
			table.name = tableName;
			table.schemaName = schemaName;
			table.setType(ttype);
			dbmsfeatures.addTableSpecificFeatures(table, rs);
			
			try {
				String fullTablename = (schemaPattern==null?"":table.schemaName+".")+tableName;
				log.debug("getting columns from "+fullTablename);

				//columns
				ResultSet cols = dbmd.getColumns(null, table.schemaName, tableName, null);
				while(cols.next()) {
					Column c = retrieveColumn(cols);
					table.getColumns().add(c);
					//String colDesc = getColumnDesc(c, columnTypeMapping, papp.getProperty(PROP_FROM_DB_ID), papp.getProperty(PROP_TO_DB_ID));
				}
				cols.close();
				
				//PKs
				if(doSchemaGrabPKs) {
					log.debug("getting PKs from "+fullTablename);
					ResultSet pks = dbmd.getPrimaryKeys(null, table.schemaName, tableName);
					grabSchemaPKs(pks, table);
					pks.close();
				}

				//FKs
				if(doSchemaGrabFKs && (!tableOnly || deeprecursivedump)) {
					log.debug("getting FKs from "+fullTablename);
					ResultSet fkrs = dbmd.getImportedKeys(null, table.schemaName, tableName);
					grabSchemaFKs(fkrs, table, schemaModel.foreignKeys);
					fkrs.close();
				}
				
				//GRANTs
				if(doSchemaGrabGrants) {
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, table.schemaName, tableName);
					table.setGrants( grabSchemaGrants(grantrs, tableName) );
					grantrs.close();
				}
				
				//INDEXes
				if(doSchemaGrabIndexes && TableType.TABLE.equals(table.getType()) && !tableOnly) {
					log.debug("getting indexes from "+fullTablename);
					ResultSet indexesrs = dbmd.getIndexInfo(null, table.schemaName, tableName, false, false);
					grabSchemaIndexes(indexesrs, schemaModel.indexes);
					indexesrs.close();
				}
			}
			catch(OutOfMemoryError oome) {
				log.warn("OutOfMemoryError: memory: max: "+Runtime.getRuntime().maxMemory()+"; total: "+Runtime.getRuntime().totalMemory()+"; free: "+Runtime.getRuntime().freeMemory());
				throw oome;
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+tableName+" ["+sqle+"]");
				//sqle.printStackTrace();
				tableNamesForDataDump.remove(tableName);
			}
			
			table.validateConstraints();

			tableNamesForDataDump.add(tableName);
			schemaModel.tables.add(table);
		}
		rs.close();

		if(recursivedump && (!tableOnly || deeprecursivedump)) {
			grabTablesRecursivebasedOnFKs(dbmd, dbmsfeatures, schemaModel, schemaPattern);
		}
		
		//log.debug("tables::["+schemaModel.tables.size()+"]\n"+schemaModel.tables+"\n");
		//log.debug("FKs::["+schemaModel.foreignKeys.size()+"]\n"+schemaModel.foreignKeys+"\n");

		/*
		if(doSchemaGrabDbSpecific && !tableOnly) {
			grabDbSpecific(schemaModel, schemaPattern);
		}*/
		
		//return schemaModel;
	}
	
	void grabTablesRecursivebasedOnFKs(DatabaseMetaData dbmd, DBMSFeatures dbmsfeatures, SchemaModel schemaModel, String schemaPattern) throws Exception { //, String padding
		log.debug("recursivegrab: "+schemaPattern);
		Set<DBObjectId> ids = new HashSet<DBObjectId>();
		for(FK fk: schemaModel.foreignKeys) {
			DBObjectId dbid = new DBObjectId();
			dbid.name = fk.pkTable;
			dbid.schemaName = fk.pkTableSchemaName;
			ids.add(dbid);
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
		c.name = cols.getString("COLUMN_NAME");
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
		//PKs
		Map<String, Set<String>> tablePK = new HashMap<String, Set<String>>();
		//ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, tableName);
		int count=0;
		while(pks.next()) {
			String pkName = pks.getString("PK_NAME");
			if(pkName==null) {
				pkName = "PK_"+count;
				count++;
			}
			table.setPkConstraintName(pkName);
			Set<String> pk = tablePK.get(pkName);
			if(pk==null) {
				pk = new HashSet<String>(); //XXXxx: TreeSet? no need...
				tablePK.put(pkName, pk);
			}
			pk.add(pks.getString("COLUMN_NAME"));
		}
		for(String key: tablePK.keySet()) {
			for(String colName: tablePK.get(key)) {
				if(table.getColumn(colName)==null) {
					log.warn("column belongs to PK but not to table? "+table+"."+colName+" ["+key+"]");
				}
				else {
					table.getColumn(colName).pk = true;
				}
			}
		}
		
	}

	void grabSchemaFKs(ResultSet fkrs, Table table, Set<FK> foreignKeys) throws SQLException, IOException {
		Map<String, FK> fks = new HashMap<String, FK>();
		int count=0;
		while(fkrs.next()) {
			//log.debug("FK!!!");
			String fkName = fkrs.getString("FK_NAME");
			if(fkName==null) {
				log.warn("nameless FK: "+fkrs.getString("FKTABLE_NAME")+"->"+fkrs.getString("PKTABLE_NAME"));
				fkName = "FK_"+count;
				count++;
			}
			log.debug("fk: "+fkName);
			FK fk = fks.get(fkName);
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
	
}
